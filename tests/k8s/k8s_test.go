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
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
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
		if err := buildImage(); err != nil {
			log.Fatalf("Failed to build image: %v", err)
		}
		if err := startConfigStore(); err != nil {
			log.Fatalf("Failed to start config store: %v", err)
		}
		if err := applyManifests(); err != nil {
			log.Fatalf("Failed to apply manifests: %v", err)
		}
		if err := waitForDeployment(namespace, "duckgres-control-plane", 180*time.Second); err != nil {
			log.Fatalf("Deployment not ready: %v", err)
		}
		if err := seedConfigStore(); err != nil {
			log.Fatalf("Failed to seed config store: %v", err)
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
	pgPort, portFwdCmd, err = startPortForward(namespace, "svc/duckgres", 5432)
	if err != nil {
		log.Fatalf("Failed to start port-forward: %v", err)
	}

	// Wait for port-forward to be ready
	if err := waitForPort(pgPort, 30*time.Second); err != nil {
		log.Fatalf("Port-forward not ready: %v", err)
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
		_ = stopConfigStore()
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
	pgPort, portFwdCmd, pfErr = startPortForward(namespace, "svc/duckgres", 5432)
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

func buildImage() error {
	projectRoot := findProjectRoot()
	log.Println("Building duckgres Docker image with -tags kubernetes...")
	cmd := exec.Command("docker", "build",
		"--build-arg", "BUILD_TAGS=kubernetes",
		"-t", "duckgres:test",
		".")
	cmd.Dir = projectRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func applyManifests() error {
	projectRoot := findProjectRoot()
	k8sDir := filepath.Join(projectRoot, "k8s")

	// Apply in order: namespace first, then everything else
	orderedFiles := []string{
		"namespace.yaml",
		"rbac.yaml",
		"configmap.yaml",
		"secret.yaml",
		"networkpolicy.yaml",
	}

	for _, f := range orderedFiles {
		path := filepath.Join(k8sDir, f)
		if err := runCmd("kubectl", "apply", "-f", path); err != nil {
			return fmt.Errorf("apply %s: %w", f, err)
		}
	}

	// Apply deployment with image override
	deployPath := filepath.Join(k8sDir, "control-plane-deployment.yaml")
	deployContent, err := os.ReadFile(deployPath)
	if err != nil {
		return fmt.Errorf("read deployment: %w", err)
	}

	// Replace image references to use test image
	patched := strings.ReplaceAll(string(deployContent), "duckgres:latest", "duckgres:test")

	// Write to temp file and apply
	tmp, err := os.CreateTemp("", "deploy-*.yaml")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.WriteString(patched); err != nil {
		return fmt.Errorf("write temp: %w", err)
	}
	tmp.Close()

	return runCmd("kubectl", "apply", "-f", tmp.Name())
}

func startConfigStore() error {
	projectRoot := findProjectRoot()
	composePath := filepath.Join(projectRoot, "k8s", "local-config-store.compose.yaml")
	if err := runCmd("docker", "compose", "-f", composePath, "up", "-d"); err != nil {
		return err
	}
	return waitForPort(5434, 60*time.Second)
}

func stopConfigStore() error {
	projectRoot := findProjectRoot()
	composePath := filepath.Join(projectRoot, "k8s", "local-config-store.compose.yaml")
	return runCmd("docker", "compose", "-f", composePath, "down", "-v")
}

func seedConfigStore() error {
	teamSQL := `
INSERT INTO duckgres_teams (name, max_workers, memory_budget, idle_timeout_s, created_at, updated_at)
VALUES ('local', 0, '', 0, NOW(), NOW())
ON CONFLICT (name) DO UPDATE
SET updated_at = NOW();
`
	if err := runCmd("docker", "exec", "-i", "duckgres-config-store",
		"psql", "-U", "duckgres", "-d", "duckgres_config", "-v", "ON_ERROR_STOP=1", "-c", teamSQL); err != nil {
		return fmt.Errorf("seed team: %w", err)
	}

	userSQL := `
INSERT INTO duckgres_team_users (username, password, team_name, created_at, updated_at)
VALUES ('postgres', '$2a$10$TQyt73Vw91Q1d7YcE86EVuhms/0u4qBydMDyVvZYlqDwc3/VtQAbm', 'local', NOW(), NOW())
ON CONFLICT (username) DO UPDATE
SET password = EXCLUDED.password,
    team_name = EXCLUDED.team_name,
    updated_at = NOW();
`
	if err := runCmd("docker", "exec", "-i", "duckgres-config-store",
		"psql", "-U", "duckgres", "-d", "duckgres_config", "-v", "ON_ERROR_STOP=1", "-c", userSQL); err != nil {
		return fmt.Errorf("seed user: %w", err)
	}

	time.Sleep(3 * time.Second)
	return nil
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

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	// kubectl port-forward passes raw TCP bytes, so the client still needs
	// SSL. lib/pq sslmode=require skips server cert verification by default,
	// which works with self-signed certs.
	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=postgres sslmode=require", pgPort)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}

	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(30 * time.Second)

	return db
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
