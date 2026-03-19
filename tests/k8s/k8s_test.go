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
	pgPort     int
	portFwdCmd *exec.Cmd
)

func TestMain(m *testing.M) {
	namespace = envOr("DUCKGRES_K8S_TEST_NAMESPACE", "duckgres")
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
	kubeconfig := envOr("DUCKGRES_K8S_TEST_KUBECONFIG", filepath.Join(os.Getenv("HOME"), ".kube", "config"))
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("Failed to load kubeconfig: %v", err)
	}
	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create k8s client: %v", err)
	}

	// Start port-forward
	controlPlanePod, err := waitForSingleReadyPod(namespace, "app=duckgres-control-plane", 90*time.Second)
	if err != nil {
		log.Fatalf("Control-plane pod not ready: %v", err)
	}
	pgPort, portFwdCmd, err = startPortForward(namespace, "pod/"+controlPlanePod, 5432)
	if err != nil {
		log.Fatalf("Failed to start port-forward: %v", err)
	}

	// Wait for port-forward to be ready
	if err := waitForPort(pgPort, 30*time.Second); err != nil {
		log.Fatalf("Port-forward not ready: %v", err)
	}
	if err := waitForDBReady(90 * time.Second); err != nil {
		log.Fatalf("Database not ready: %v", err)
	}

	code := m.Run()

	// Cleanup port-forward
	if portFwdCmd != nil && portFwdCmd.Process != nil {
		_ = portFwdCmd.Process.Kill()
		_ = portFwdCmd.Wait()
	}

	// Cleanup K8s resources (unless skip_setup, meaning external management)
	if !skipSetup {
		_ = runCmd("kubectl", "delete", "namespace", namespace, "--ignore-not-found")
		_ = runCmd("just", "multitenant-config-store-down")
	}

	os.Exit(code)
}

// --- Test Cases ---

func TestK8sBasicQuery(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var result int
	if err := db.QueryRow("SELECT 1").Scan(&result); err != nil {
		t.Fatalf("SELECT 1 failed: %v", err)
	}
	if result != 1 {
		t.Fatalf("expected 1, got %d", result)
	}
}

func TestK8sWorkerPodCreation(t *testing.T) {
	// Run a query first to ensure at least one worker is spawned
	db := openDB(t)
	defer db.Close()

	var result int
	if err := db.QueryRow("SELECT 42").Scan(&result); err != nil {
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

		// Verify owner references
		if len(pod.OwnerReferences) == 0 {
			t.Errorf("worker pod %s has no owner references", pod.Name)
		}
	}
}

func TestK8sWorkerCrashRecovery(t *testing.T) {
	// Run a query to ensure a worker exists
	db := openDB(t)
	defer db.Close()

	var result int
	if err := db.QueryRow("SELECT 1").Scan(&result); err != nil {
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

	// The old connection may be broken. Open a new one and retry queries.
	db.Close()

	// Wait a bit for the CP to detect the crash and be ready for new connections
	time.Sleep(5 * time.Second)

	// Open a new connection and verify queries work (CP should spawn a replacement)
	db2 := openDB(t)
	defer db2.Close()

	err = retryQuery(db2, "SELECT 1", 30*time.Second)
	if err != nil {
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
	db := openDB(t)
	defer db.Close()

	var result int
	if err := db.QueryRow("SELECT 1").Scan(&result); err != nil {
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

func TestK8sWorkerUsesRuntimeConfigSecret(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var tempDir string
	if err := db.QueryRow("SELECT value FROM duckdb_settings() WHERE name = 'temp_directory'").Scan(&tempDir); err != nil {
		t.Fatalf("failed to read temp_directory setting: %v", err)
	}
	if tempDir != "/data/runtime-secret/tmp" {
		t.Fatalf("expected runtime config temp_directory=/data/runtime-secret/tmp, got %q", tempDir)
	}

	pod := latestWorkerPod(t)
	container := pod.Spec.Containers[0]
	if !hasWorkerConfigArg(container.Args, "/etc/duckgres/runtime/duckgres.yaml") {
		t.Fatalf("expected worker args to include --config /etc/duckgres/runtime/duckgres.yaml, got %#v", container.Args)
	}

	volume := findWorkerVolume(t, pod, "duckgres-config-secret")
	if volume.Secret == nil {
		t.Fatalf("expected duckgres-config-secret volume to be secret-backed, got %#v", volume)
	}
	if volume.Secret.SecretName != "duckgres-local-runtime" {
		t.Fatalf("expected runtime config secret duckgres-local-runtime, got %q", volume.Secret.SecretName)
	}
	if len(volume.Secret.Items) != 1 || volume.Secret.Items[0].Key != "duckgres.yaml" || volume.Secret.Items[0].Path != "duckgres.yaml" {
		t.Fatalf("expected runtime config secret item duckgres.yaml, got %#v", volume.Secret.Items)
	}

	mount := findWorkerVolumeMount(t, container.VolumeMounts, "duckgres-config-secret")
	if mount.MountPath != "/etc/duckgres/runtime" {
		t.Fatalf("expected runtime config mount path /etc/duckgres/runtime, got %q", mount.MountPath)
	}
	if !mount.ReadOnly {
		t.Fatalf("expected runtime config mount to be read-only")
	}
}

func TestK8sCPDeletionGarbageCollects(t *testing.T) {
	// Ensure a worker exists
	db := openDB(t)
	defer db.Close()

	var result int
	if err := db.QueryRow("SELECT 1").Scan(&result); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	db.Close()

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

	// Verify worker pods have owner references pointing to the CP pod
	for _, wp := range workerPods.Items {
		hasOwner := false
		for _, ref := range wp.OwnerReferences {
			if ref.Kind == "Pod" {
				hasOwner = true
				t.Logf("Worker %s owned by %s (UID %s)", wp.Name, ref.Name, ref.UID)
			}
		}
		if !hasOwner {
			t.Errorf("worker pod %s has no Pod owner reference — GC will not work", wp.Name)
		}
	}

	// Delete the CP pod (the deployment will recreate it)
	cpPods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=duckgres-control-plane",
	})
	if err != nil || len(cpPods.Items) == 0 {
		t.Fatalf("failed to find CP pod: %v", err)
	}
	cpName := cpPods.Items[0].Name
	t.Logf("Deleting CP pod %s to test garbage collection", cpName)
	err = clientset.CoreV1().Pods(namespace).Delete(context.Background(), cpName, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete CP pod: %v", err)
	}

	// Wait for old worker pods to be garbage collected
	workerNames := make([]string, len(workerPods.Items))
	for i, wp := range workerPods.Items {
		workerNames[i] = wp.Name
	}

	allGone := false
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		remaining := 0
		for _, name := range workerNames {
			_, err := clientset.CoreV1().Pods(namespace).Get(context.Background(), name, metav1.GetOptions{})
			if err == nil {
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
	if portFwdCmd != nil && portFwdCmd.Process != nil {
		_ = portFwdCmd.Process.Kill()
		_ = portFwdCmd.Wait()
	}
	var pfErr error
	controlPlanePod, err := waitForSingleReadyPod(namespace, "app=duckgres-control-plane", 90*time.Second)
	if err != nil {
		t.Fatalf("control-plane pod not ready after restart: %v", err)
	}
	pgPort, portFwdCmd, pfErr = startPortForward(namespace, "pod/"+controlPlanePod, 5432)
	if pfErr != nil {
		t.Fatalf("failed to restart port-forward: %v", pfErr)
	}
	if err := waitForPort(pgPort, 30*time.Second); err != nil {
		t.Fatalf("port-forward not ready after restart: %v", err)
	}

	// Verify the system works again
	db2 := openDB(t)
	defer db2.Close()
	err = retryQuery(db2, "SELECT 1", 60*time.Second)
	if err != nil {
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

func setupMultiTenant() error {
	projectRoot := findProjectRoot()
	log.Println("Setting up multi-tenant duckgres test environment...")
	_ = runCmd("kubectl", "delete", "namespace", namespace, "--ignore-not-found", "--wait=true")
	_ = runCmd("just", "multitenant-config-store-down")

	cmd := exec.Command("just", "run-multitenant-local")
	cmd.Dir = projectRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
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
		if err != nil {
			return // pod is gone
		}
		time.Sleep(2 * time.Second)
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

		if len(pods.Items) != 1 {
			time.Sleep(2 * time.Second)
			continue
		}

		for _, pod := range pods.Items {
			if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
				continue
			}
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return pod.Name, nil
				}
			}
		}

		time.Sleep(2 * time.Second)
	}

	return "", fmt.Errorf("expected one ready pod for %q within %s", labelSelector, timeout)
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

func hasWorkerConfigArg(args []string, configPath string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "--config" && args[i+1] == configPath {
			return true
		}
	}
	return false
}

func findWorkerVolume(t *testing.T, pod corev1.Pod, name string) corev1.VolumeSource {
	t.Helper()

	for _, volume := range pod.Spec.Volumes {
		if volume.Name == name {
			return volume.VolumeSource
		}
	}
	t.Fatalf("expected worker volume %q, got %#v", name, pod.Spec.Volumes)
	return corev1.VolumeSource{}
}

func findWorkerVolumeMount(t *testing.T, mounts []corev1.VolumeMount, name string) corev1.VolumeMount {
	t.Helper()

	for _, mount := range mounts {
		if mount.Name == name {
			return mount
		}
	}
	t.Fatalf("expected worker volume mount %q, got %#v", name, mounts)
	return corev1.VolumeMount{}
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
	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=postgres sslmode=require", pgPort)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(30 * time.Second)
	return db, nil
}

func waitForDBReady(timeout time.Duration) error {
	db, err := openDBConn()
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	return retryQuery(db, "SELECT 1", timeout)
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
