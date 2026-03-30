//go:build k8s_integration

package k8s_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	clientset  *kubernetes.Clientset
	namespace  string
	kubeconfig string
	pgPort     int
	portFwdCmd *exec.Cmd
	testEnv    k8sTestEnvironment
)

const (
	duckgresServiceTarget = "svc/duckgres"
	duckgresServicePort   = 5432
)

func TestMain(m *testing.M) {
	var err error
	testEnv, err = loadK8sTestEnvironment(os.Getenv)
	if err != nil {
		log.Fatalf("Failed to load K8s test environment: %v", err)
	}
	namespace = testEnv.Namespace
	skipSetup := envOr("DUCKGRES_K8S_TEST_SKIP_SETUP", "") == "true"
	if !skipSetup {
		if namespace != "duckgres" {
			log.Fatalf("Managed k8s integration setup requires namespace duckgres, got %q", namespace)
		}
		if err := setupMultiTenant(); err != nil {
			log.Fatalf("Failed to set up multi-tenant environment: %v", err)
		}
	}

	// Build kubeconfig clientset
	kubeconfig = envOr("DUCKGRES_K8S_TEST_KUBECONFIG", filepath.Join(os.Getenv("HOME"), ".kube", "config"))
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("Failed to load kubeconfig: %v", err)
	}
	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create k8s client: %v", err)
	}

	if _, err := waitForSingleReadyPod(namespace, "app=duckgres-control-plane", 90*time.Second); err != nil {
		log.Fatalf("Control-plane pod not ready: %v", err)
	}

	if err := restartPortForward(); err != nil {
		log.Fatalf("Failed to start port-forward: %v", err)
	}
	if err := waitForDBReady(90 * time.Second); err != nil {
		log.Fatalf("Database not ready: %v", err)
	}

	code := m.Run()

	// Cleanup port-forward
	closePortForward()

	// Cleanup K8s resources (unless skip_setup, meaning external management)
	if !skipSetup {
		_ = runCmd("kubectl", "delete", "namespace", namespace, "--ignore-not-found", "--wait=true")
		if testEnv.CleanupRecipe != "" {
			_ = runProjectCmd("just", testEnv.CleanupRecipe)
		}
	}

	os.Exit(code)
}

// --- Test Cases ---

func TestK8sBasicQuery(t *testing.T) {
	var result int
	if err := retryScanIntWithReconnect("SELECT 1", 30*time.Second, &result); err != nil {
		t.Fatalf("SELECT 1 failed: %v", err)
	}
	if result != 1 {
		t.Fatalf("expected 1, got %d", result)
	}
}

func TestK8sWorkerPodCreation(t *testing.T) {
	// Run a query first to ensure at least one worker is spawned
	var result int
	if err := retryScanIntWithReconnect("SELECT 42", 30*time.Second, &result); err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Check for worker pods
	pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		t.Fatalf("failed to list worker pods: %v", err)
	}
	if len(pods.Items) == 0 {
		t.Fatal("expected at least one worker pod, found none")
	}

	for _, pod := range pods.Items {
		// Verify labels
		if pod.Labels["app"] != "duckgres-worker" {
			t.Errorf("worker pod %s missing app=duckgres-worker label", pod.Name)
		}
		cpLabel := pod.Labels["duckgres/control-plane"]
		if cpLabel == "" {
			t.Errorf("worker pod %s missing duckgres/control-plane label", pod.Name)
		}
		if pod.Labels["duckgres/worker-id"] == "" {
			t.Errorf("worker pod %s missing duckgres/worker-id label", pod.Name)
		}
	}
}

func TestK8sSharedWarmWorkerActivation(t *testing.T) {
	if _, err := latestWorkerPodBeforeQuery(60 * time.Second); err != nil {
		t.Fatalf("expected prewarmed worker before first query: %v", err)
	}

	var attached int
	if err := retryScanIntWithReconnect("SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'ducklake'", 90*time.Second, &attached); err != nil {
		t.Fatalf("shared warm worker activation did not attach ducklake: %v", err)
	}
	if attached != 1 {
		t.Fatalf("expected one attached ducklake catalog after activation, got %d", attached)
	}
}

func TestK8sWorkerCrashRecovery(t *testing.T) {
	// Run a query to ensure a worker exists
	if err := retryQueryWithReconnect("SELECT 1", 30*time.Second); err != nil {
		t.Fatalf("initial query failed: %v", err)
	}

	// List current workers
	pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		t.Fatalf("failed to list worker pods: %v", err)
	}
	if len(pods.Items) == 0 {
		t.Fatal("no worker pods to delete")
	}

	// Delete a worker pod
	workerName := pods.Items[0].Name
	t.Logf("Deleting worker pod %s to test crash recovery", workerName)
	err = clientset.CoreV1().Pods(namespace).Delete(context.Background(), workerName, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete worker pod: %v", err)
	}

	// Wait for the pod to actually disappear
	waitForPodGone(t, namespace, workerName, 60*time.Second)

	if err := retryQueryWithReconnect("SELECT 1", 60*time.Second); err != nil {
		t.Fatalf("query failed after worker crash recovery: %v", err)
	}
}

func TestK8sMultipleConcurrentConnections(t *testing.T) {
	const n = 5
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			db := openDB(t)
			defer db.Close()

			var result int
			if err := db.QueryRow(fmt.Sprintf("SELECT %d", id)).Scan(&result); err != nil {
				errs <- fmt.Errorf("connection %d: query failed: %w", id, err)
				return
			}
			if result != id {
				errs <- fmt.Errorf("connection %d: expected %d, got %d", id, id, result)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func TestK8sWorkerSecurityContext(t *testing.T) {
	// Ensure a worker exists
	if err := retryQueryWithReconnect("SELECT 1", 30*time.Second); err != nil {
		t.Fatalf("query failed: %v", err)
	}

	pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		t.Fatalf("failed to list worker pods: %v", err)
	}
	if len(pods.Items) == 0 {
		t.Fatal("no worker pods found")
	}

	pod := pods.Items[0]

	// Verify pod-level security context
	if pod.Spec.SecurityContext == nil {
		t.Fatal("pod security context is nil")
	}
	if pod.Spec.SecurityContext.RunAsNonRoot == nil || !*pod.Spec.SecurityContext.RunAsNonRoot {
		t.Error("expected runAsNonRoot=true")
	}
	if pod.Spec.SecurityContext.RunAsUser == nil || *pod.Spec.SecurityContext.RunAsUser != 1000 {
		t.Errorf("expected runAsUser=1000, got %v", pod.Spec.SecurityContext.RunAsUser)
	}

	// Verify container-level security context
	if len(pod.Spec.Containers) == 0 {
		t.Fatal("no containers in worker pod")
	}
	csc := pod.Spec.Containers[0].SecurityContext
	if csc == nil {
		t.Fatal("container security context is nil")
	}
	if csc.AllowPrivilegeEscalation == nil || *csc.AllowPrivilegeEscalation {
		t.Error("expected allowPrivilegeEscalation=false")
	}
}

func TestK8sCPDeletionGarbageCollects(t *testing.T) {
	// Ensure a worker exists
	if err := retryQueryWithReconnect("SELECT 1", 30*time.Second); err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// List worker pods
	workerPods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		t.Fatalf("failed to list worker pods: %v", err)
	}
	if len(workerPods.Items) == 0 {
		t.Skip("no worker pods found — cannot test GC")
	}

	ownedWorkers := workerPodsByControlPlaneLabel(workerPods.Items)
	if len(ownedWorkers) == 0 {
		t.Skip("no worker pods with duckgres/control-plane label found")
	}

	// Delete a CP pod that currently owns at least one worker.
	var cpName string
	var workerNames []string
	for ownerName, owned := range ownedWorkers {
		if len(owned) > 0 {
			cpName = ownerName
			workerNames = append([]string(nil), owned...)
			break
		}
	}
	if cpName == "" {
		t.Skip("no control-plane-owned worker pods found")
	}

	cpPods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=duckgres-control-plane",
	})
	if err != nil || len(cpPods.Items) == 0 {
		t.Fatalf("failed to find CP pod: %v", err)
	}
	foundCP := false
	for _, pod := range cpPods.Items {
		if pod.Name == cpName {
			foundCP = true
			break
		}
	}
	if !foundCP {
		t.Skipf("control-plane pod %s no longer exists", cpName)
	}
	gracePeriodSeconds := int64(0)
	t.Logf("Force deleting CP pod %s to test crash-style garbage collection", cpName)
	err = clientset.CoreV1().Pods(namespace).Delete(context.Background(), cpName, metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriodSeconds,
	})
	if err != nil {
		t.Fatalf("failed to delete CP pod: %v", err)
	}

	// Wait for the deleted control plane's worker pods to be retired.
	allGone := false
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		remaining := 0
		for _, name := range workerNames {
			_, err := clientset.CoreV1().Pods(namespace).Get(context.Background(), name, metav1.GetOptions{})
			switch {
			case err == nil:
				remaining++
			case isPodGoneError(err):
				continue
			default:
				t.Logf("transient error checking worker pod %s deletion: %v", name, err)
				remaining++
			}
		}
		if remaining == 0 {
			allGone = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !allGone {
		t.Error("worker pods were not garbage-collected after CP deletion within 90s")
	}

	// Wait for the deployment to recreate the CP
	if err := waitForDeployment(namespace, "duckgres-control-plane", 120*time.Second); err != nil {
		t.Fatalf("CP deployment did not recover: %v", err)
	}

	// Restart port-forward since the old CP pod is gone
	if err := restartPortForward(); err != nil {
		t.Fatalf("failed to restart port-forward: %v", err)
	}

	// Verify the system works again
	if err := retryQueryWithReconnect("SELECT 1", 60*time.Second); err != nil {
		t.Fatalf("query failed after CP recreation: %v", err)
	}
}

// --- Helpers ---

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runProjectCmd(name string, args ...string) error {
	projectRoot := findProjectRoot()
	cmd := exec.Command(name, args...)
	cmd.Dir = projectRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func setupMultiTenant() error {
	log.Println("Setting up multi-tenant duckgres test environment...")
	_ = runCmd("kubectl", "delete", "namespace", namespace, "--ignore-not-found", "--wait=true")
	if testEnv.CleanupRecipe != "" {
		_ = runProjectCmd("just", testEnv.CleanupRecipe)
	}
	return runProjectCmd("just", testEnv.SetupRecipe)
}

func waitForDeployment(ns, name string, timeout time.Duration) error {
	return runCmd("kubectl", "-n", ns, "wait", "deployment/"+name,
		"--for=condition=available",
		fmt.Sprintf("--timeout=%ds", int(timeout.Seconds())))
}

func startPortForward(ns, target string, remotePort int) (int, *exec.Cmd, error) {
	// Find a free local port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, nil, fmt.Errorf("find free port: %w", err)
	}
	localPort := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	cmd := exec.Command("kubectl", "-n", ns, "port-forward", target,
		fmt.Sprintf("%d:%d", localPort, remotePort))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, nil, fmt.Errorf("start port-forward: %w", err)
	}

	return localPort, cmd, nil
}

func closePortForward() {
	if portFwdCmd == nil || portFwdCmd.Process == nil {
		portFwdCmd = nil
		return
	}

	_ = portFwdCmd.Process.Kill()
	_ = portFwdCmd.Wait()
	portFwdCmd = nil
}

func restartPortForward() error {
	closePortForward()

	localPort, cmd, err := startPortForward(namespace, duckgresServiceTarget, duckgresServicePort)
	if err != nil {
		return err
	}

	pgPort = localPort
	portFwdCmd = cmd

	if err := waitForPort(pgPort, 30*time.Second); err != nil {
		closePortForward()
		return err
	}

	return nil
}

func waitForPort(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 1*time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("port %d not reachable after %s", port, timeout)
}

func waitForPodGone(t *testing.T, ns, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := clientset.CoreV1().Pods(ns).Get(context.Background(), name, metav1.GetOptions{})
		switch {
		case err == nil:
			time.Sleep(2 * time.Second)
		case isPodGoneError(err):
			return
		default:
			t.Logf("transient error checking pod %s deletion: %v", name, err)
			time.Sleep(2 * time.Second)
		}
	}
	t.Logf("Warning: pod %s still exists after %s", name, timeout)
}

func waitForSingleReadyPod(ns, labelSelector string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pods, err := clientset.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{
			LabelSelector: labelSelector,
		})
		if err != nil {
			return "", err
		}

		if name, ok := findReadyPodName(pods.Items); ok {
			return name, nil
		}

		time.Sleep(2 * time.Second)
	}

	return "", fmt.Errorf("expected at least one ready pod for %q within %s", labelSelector, timeout)
}

func latestWorkerPod(t *testing.T) corev1.Pod {
	t.Helper()

	pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		t.Fatalf("failed to list worker pods: %v", err)
	}
	if len(pods.Items) == 0 {
		t.Fatal("expected at least one worker pod, found none")
	}

	latest := pods.Items[0]
	for _, pod := range pods.Items[1:] {
		if pod.CreationTimestamp.After(latest.CreationTimestamp.Time) {
			latest = pod
		}
	}
	return latest
}

func latestWorkerPodBeforeQuery(timeout time.Duration) (corev1.Pod, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
			LabelSelector: "app=duckgres-worker",
		})
		if err != nil {
			return corev1.Pod{}, err
		}
		if len(pods.Items) > 0 {
			latest := pods.Items[0]
			for _, pod := range pods.Items[1:] {
				if pod.CreationTimestamp.After(latest.CreationTimestamp.Time) {
					latest = pod
				}
			}
			return latest, nil
		}
		time.Sleep(2 * time.Second)
	}
	return corev1.Pod{}, fmt.Errorf("no worker pods appeared within %s", timeout)
}

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := openDBConn()
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}

	return db
}

func openDBConn() (*sql.DB, error) {
	// kubectl port-forward passes raw TCP bytes, so the client still needs
	// SSL. lib/pq sslmode=require skips server cert verification by default,
	// which works with self-signed certs.
	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=postgres dbname=duckgres sslmode=require", pgPort)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(30 * time.Second)
	return db, nil
}

func waitForDBReady(timeout time.Duration) error {
	return retryQueryWithReconnect("SELECT 1", timeout)
}

func retryQuery(db *sql.DB, query string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		var result int
		lastErr = db.QueryRow(query).Scan(&result)
		if lastErr == nil {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("query %q failed after %s: %w", query, timeout, lastErr)
}

func retryScanInt(db *sql.DB, query string, timeout time.Duration, dest *int) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = db.QueryRow(query).Scan(dest)
		if lastErr == nil {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("query %q failed after %s: %w", query, timeout, lastErr)
}

func retryQueryWithReconnect(query string, timeout time.Duration) error {
	return retryDBOperationWithReconnect(timeout, fmt.Sprintf("query %q", query), func(db *sql.DB) error {
		var result int
		return db.QueryRow(query).Scan(&result)
	})
}

func retryScanIntWithReconnect(query string, timeout time.Duration, dest *int) error {
	return retryDBOperationWithReconnect(timeout, fmt.Sprintf("query %q", query), func(db *sql.DB) error {
		return db.QueryRow(query).Scan(dest)
	})
}

// Use a fresh DB connection on each attempt so transient port-forward failures
// can be recovered by restarting the forwarder between retries.
func retryDBOperationWithReconnect(timeout time.Duration, description string, op func(*sql.DB) error) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		db, err := openDBConn()
		if err == nil {
			err = op(db)
			_ = db.Close()
		}
		if err == nil {
			return nil
		}

		lastErr = err
		if isTransientDBError(err) {
			if restartErr := restartPortForward(); restartErr != nil {
				lastErr = fmt.Errorf("%w; restart port-forward: %v", err, restartErr)
			}
		}

		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("%s failed after %s: %w", description, timeout, lastErr)
}

func findProjectRoot() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			log.Fatal("Could not find project root (go.mod)")
		}
		dir = parent
	}
}
