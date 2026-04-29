//go:build k8s_integration

package k8s_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	clientset  *kubernetes.Clientset
	namespace  string
	kubeconfig string
	portForward *portForwardState
	testEnv    k8sTestEnvironment
)

const (
	duckgresServiceTarget = "svc/duckgres"
	duckgresServicePort   = 5432
	initialDBReadyTimeout = 4 * time.Minute
	dbAttemptTimeout      = 30 * time.Second
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

	// SAFETY: require an explicit kubeconfig path. Falling back to
	// ~/.kube/config is how this suite once ran against the live mw-dev
	// cluster and wiped the duckgres namespace via setupMultiTenant's
	// `kubectl delete namespace duckgres`. Always fail loudly instead.
	kubeconfig = os.Getenv("DUCKGRES_K8S_TEST_KUBECONFIG")
	if kubeconfig == "" {
		log.Fatalf("DUCKGRES_K8S_TEST_KUBECONFIG is required. Run via `just test-k8s-integration` (which sets it to a kind kubeconfig) or set it explicitly. Refusing to fall back to the user's default kubeconfig because this suite is destructive.")
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("Failed to load kubeconfig: %v", err)
	}
	// SAFETY: confirm the resolved kubeconfig actually points at a local
	// kind cluster before running anything destructive.
	if err := requireLocalKindCluster(kubeconfig, config); err != nil {
		log.Fatalf("REFUSING to run k8s integration tests: %v", err)
	}
	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create k8s client: %v", err)
	}
	portForward = newPortForwardState(
		func() (int, *exec.Cmd, error) {
			return startPortForward(namespace, duckgresServiceTarget, duckgresServicePort)
		},
		waitForPort,
		func(cmd *exec.Cmd) {
			if cmd == nil || cmd.Process == nil {
				return
			}
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		},
	)

	if _, err := waitForSingleReadyPod(namespace, "app=duckgres-control-plane", 90*time.Second); err != nil {
		log.Fatalf("Control-plane pod not ready: %v", err)
	}

	if err := restartPortForward(); err != nil {
		log.Fatalf("Failed to start port-forward: %v", err)
	}
	if err := waitForDBReady(initialDBReadyTimeout); err != nil {
		log.Fatalf("Database not ready: %v", err)
	}
	if err := seedTenantIsolationFixtures(); err != nil {
		log.Fatalf("Failed to seed tenant isolation fixtures: %v", err)
	}
	if err := waitForTenantDBReady("analytics", "postgres", initialDBReadyTimeout); err != nil {
		log.Fatalf("Analytics tenant login not ready: %v", err)
	}
	if err := waitForTenantDBReady("billing", "postgres", initialDBReadyTimeout); err != nil {
		log.Fatalf("Billing tenant login not ready: %v", err)
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
	const timeout = 75 * time.Second
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			query := fmt.Sprintf("SELECT %d", id)
			if err := retryDBOperationWithReconnect(timeout, fmt.Sprintf("concurrent query %q", query), func(ctx context.Context, db *sql.DB) error {
				var result int
				if err := db.QueryRowContext(ctx, query).Scan(&result); err != nil {
					return err
				}
				if result != id {
					return fmt.Errorf("expected %d, got %d", id, result)
				}
				return nil
			}); err != nil {
				errs <- fmt.Errorf("connection %d: query failed: %w", id, err)
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
		return
	}
	if csc.AllowPrivilegeEscalation == nil || *csc.AllowPrivilegeEscalation {
		t.Error("expected allowPrivilegeEscalation=false")
	}
}

// TestK8sVersionMismatchedWorkerIsReaped verifies the leader-driven
// rolling-replacement behavior introduced when the startup orphan sweep was
// removed: when a shared warm worker pod's duckgres/control-plane label
// identifies a different Deployment ReplicaSet than the running CP's, the
// janitor leader retires it via an atomic idle->retired CAS and deletes the
// pod. Together with reconcileWarmCapacity in the same tick the slot is
// refilled with a current-version worker, so deployment rollouts replace
// shared workers gradually instead of in a destructive cross-CP sweep.
//
// We simulate the version mismatch by mutating an existing warm worker's
// label to a fake Deployment hash. The reaper has no way to distinguish a
// genuine prior-rollout pod from this fake one, which is precisely what we
// want to assert.
func TestK8sVersionMismatchedWorkerIsReaped(t *testing.T) {
	// Make sure at least one shared warm worker is up.
	if err := retryQueryWithReconnect("SELECT 1", 30*time.Second); err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Brief idle window so the worker settles back into idle state in the
	// configstore — RetireIdleWorker is a state-conditional CAS that no-ops
	// on busy/reserved/hot rows.
	time.Sleep(3 * time.Second)

	workerPods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		t.Fatalf("list worker pods: %v", err)
	}
	var target *corev1.Pod
	for i := range workerPods.Items {
		p := &workerPods.Items[i]
		// Shared warm workers (no duckgres/org label) only — the version
		// reaper currently runs against the shared pool.
		if p.Labels["duckgres/org"] != "" {
			continue
		}
		if p.Labels["duckgres/control-plane"] == "" || p.Labels["duckgres/worker-id"] == "" {
			continue
		}
		if p.DeletionTimestamp != nil {
			continue
		}
		if p.Status.Phase != corev1.PodRunning {
			continue
		}
		target = p
		break
	}
	if target == nil {
		t.Skip("no eligible shared warm worker pod found")
	}

	originalCPLabel := target.Labels["duckgres/control-plane"]
	// Pick a pod-template-hash segment that's clearly different from the
	// real one so trimK8sPodHashSuffix yields a distinct version prefix.
	fakeCPLabel := "duckgres-control-plane-deadbeef00-fake1"
	patch := []byte(fmt.Sprintf(`{"metadata":{"labels":{"duckgres/control-plane":%q}}}`, fakeCPLabel))
	if _, err := clientset.CoreV1().Pods(namespace).Patch(
		context.Background(),
		target.Name,
		k8stypes.StrategicMergePatchType,
		patch,
		metav1.PatchOptions{},
	); err != nil {
		t.Fatalf("patch pod %s control-plane label: %v", target.Name, err)
	}
	t.Logf("Mutated pod %s control-plane label %q -> %q; expecting leader version reaper to retire it",
		target.Name, originalCPLabel, fakeCPLabel)

	// Janitor leader runs every 5s; allow generous slack for leader lease
	// acquisition, configstore CAS, pod delete + grace.
	waitForPodGone(t, namespace, target.Name, 90*time.Second)
	if _, err := clientset.CoreV1().Pods(namespace).Get(context.Background(), target.Name, metav1.GetOptions{}); !isPodGoneError(err) {
		t.Fatalf("pod %s with mismatched-version label was not reaped within 90s (err=%v)", target.Name, err)
	}

	// System should still serve traffic — replenishment happens in the same
	// janitor tick that retired the mismatched worker, so there should be no
	// observable capacity dip.
	if err := retryQueryWithReconnect("SELECT 1", 30*time.Second); err != nil {
		t.Fatalf("query after version reaper retired worker failed: %v", err)
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
	cmd.Env = commandEnv()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runProjectCmd(name string, args ...string) error {
	projectRoot := findProjectRoot()
	cmd := exec.Command(name, args...)
	cmd.Dir = projectRoot
	cmd.Env = commandEnv()
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
	cmd.Env = commandEnv()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, nil, fmt.Errorf("start port-forward: %w", err)
	}

	return localPort, cmd, nil
}

func closePortForward() {
	if portForward == nil {
		return
	}
	portForward.closeCurrent()
}

func restartPortForward() error {
	if portForward == nil {
		return fmt.Errorf("port-forward state is not initialized")
	}
	return portForward.restart(30 * time.Second)
}

func restartPortForwardIfStale(stalePort int) error {
	if portForward == nil {
		return fmt.Errorf("port-forward state is not initialized")
	}
	return portForward.restartIfStale(stalePort, 30*time.Second)
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

// requireLocalKindCluster errors out unless the resolved kubeconfig points
// at a loopback API server AND the current-context name mentions "kind".
// Two independent signals so a misconfigured local proxy can't masquerade
// as kind, and so a kind kubeconfig that's been edited to reach a remote
// cluster (somehow) is also rejected.
//
// Why it matters: setupMultiTenant() begins with
//
//	kubectl delete namespace duckgres --ignore-not-found --wait=true
//
// against the resolved kubeconfig. If that resolves to a real cluster, the
// command silently succeeds and wipes production data. The loopback check
// is the reliable fingerprint — managed clusters (EKS, GKE, AKS) always
// expose a cloud DNS hostname; kind always exposes 127.0.0.1.
func requireLocalKindCluster(kubeconfigPath string, cfg *rest.Config) error {
	u, err := url.Parse(cfg.Host)
	if err != nil {
		return fmt.Errorf("parse API server URL %q: %w", cfg.Host, err)
	}
	host := u.Hostname()
	switch host {
	case "127.0.0.1", "localhost", "::1":
		// loopback — kind cluster, proceed.
	default:
		return fmt.Errorf(
			"API server at %q is not loopback. The k8s integration suite is destructive (it deletes the duckgres namespace at startup) and must only run against a local kind cluster. Run via `just test-k8s-integration` or point DUCKGRES_K8S_TEST_KUBECONFIG at a kind kubeconfig",
			cfg.Host)
	}

	raw, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("load kubeconfig %q: %w", kubeconfigPath, err)
	}
	if !strings.Contains(strings.ToLower(raw.CurrentContext), "kind") {
		return fmt.Errorf(
			"current-context %q in %s does not look like a kind cluster (expected name containing \"kind\")",
			raw.CurrentContext, kubeconfigPath)
	}
	return nil
}

func commandEnv() []string {
	env := os.Environ()
	cfg := kubeconfig
	if cfg == "" {
		cfg = envOr("DUCKGRES_K8S_TEST_KUBECONFIG", "")
		if cfg == "" {
			cfg = envOr("DUCKGRES_KIND_KUBECONFIG", "")
		}
	}
	if cfg == "" {
		return env
	}

	filtered := env[:0]
	for _, entry := range env {
		if strings.HasPrefix(entry, "KUBECONFIG=") {
			continue
		}
		filtered = append(filtered, entry)
	}
	return append(filtered, "KUBECONFIG="+cfg)
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

	var latest corev1.Pod
	found := false
	for _, pod := range pods.Items {
		if !isReadyPod(pod) {
			continue
		}
		if !found || pod.CreationTimestamp.After(latest.CreationTimestamp.Time) {
			latest = pod
			found = true
		}
	}
	if found {
		return latest
	}

	t.Fatal("expected at least one ready worker pod, found none")
	return corev1.Pod{}
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
		var latest corev1.Pod
		found := false
		for _, pod := range pods.Items {
			if !isReadyPod(pod) {
				continue
			}
			if !found || pod.CreationTimestamp.After(latest.CreationTimestamp.Time) {
				latest = pod
				found = true
			}
		}
		if found {
			return latest, nil
		}
		time.Sleep(2 * time.Second)
	}
	return corev1.Pod{}, fmt.Errorf("no ready worker pods appeared within %s", timeout)
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
	return openDBConnAs("postgres", "postgres")
}

func openDBConnAs(username, password string) (*sql.DB, error) {
	if portForward == nil {
		return nil, fmt.Errorf("port-forward state is not initialized")
	}
	databaseName := username
	if username == "postgres" {
		databaseName = "duckgres"
	}
	pgPort := portForward.currentPort()
	if pgPort == 0 {
		return nil, fmt.Errorf("port-forward port is not initialized")
	}

	// kubectl port-forward passes raw TCP bytes, so the client still needs
	// SSL. lib/pq sslmode=require skips server cert verification by default,
	// which works with self-signed certs.
	connStr := fmt.Sprintf(
		"host=127.0.0.1 port=%d user=%s password=%s dbname=%s sslmode=require connect_timeout=30",
		pgPort,
		username,
		password,
		databaseName,
	)

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

func waitForTenantDBReady(username, password string, timeout time.Duration) error {
	return retryQueryWithReconnectAs(username, password, "SELECT 1", timeout)
}

func retryQuery(db *sql.DB, query string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = scanIntQueryWithTimeout(db, query, nil)
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
		lastErr = scanIntQueryWithTimeout(db, query, dest)
		if lastErr == nil {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("query %q failed after %s: %w", query, timeout, lastErr)
}

func retryQueryWithReconnect(query string, timeout time.Duration) error {
	return retryDBOperationWithReconnectAs("postgres", "postgres", timeout, fmt.Sprintf("query %q", query), func(ctx context.Context, db *sql.DB) error {
		var result int
		return db.QueryRowContext(ctx, query).Scan(&result)
	})
}

func retryScanIntWithReconnect(query string, timeout time.Duration, dest *int) error {
	return retryDBOperationWithReconnectAs("postgres", "postgres", timeout, fmt.Sprintf("query %q", query), func(ctx context.Context, db *sql.DB) error {
		return db.QueryRowContext(ctx, query).Scan(dest)
	})
}

func retryQueryWithReconnectAs(username, password, query string, timeout time.Duration) error {
	return retryDBOperationWithReconnectAs(username, password, timeout, fmt.Sprintf("query %q", query), func(ctx context.Context, db *sql.DB) error {
		var result int
		return db.QueryRowContext(ctx, query).Scan(&result)
	})
}

// Use a fresh DB connection on each attempt so transient port-forward failures
// can be recovered by restarting the forwarder between retries.
func retryDBOperationWithReconnect(timeout time.Duration, description string, op func(context.Context, *sql.DB) error) error {
	return retryDBOperationWithReconnectAs("postgres", "postgres", timeout, description, op)
}

func retryDBOperationWithReconnectAs(username, password string, timeout time.Duration, description string, op func(context.Context, *sql.DB) error) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		stalePort := 0
		if portForward != nil {
			stalePort = portForward.currentPort()
		}
		db, err := openDBConnAs(username, password)
		if err == nil {
			attemptCtx, cancel := context.WithTimeout(context.Background(), dbAttemptTimeout)
			err = op(attemptCtx, db)
			cancel()
			_ = db.Close()
		}
		if err == nil {
			return nil
		}

		lastErr = err
		if isTransientDBError(err) {
			if restartErr := restartPortForwardIfStale(stalePort); restartErr != nil {
				lastErr = fmt.Errorf("%w; restart port-forward: %v", err, restartErr)
			}
		}

		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("%s failed after %s: %w", description, timeout, lastErr)
}

func scanIntQueryWithTimeout(db *sql.DB, query string, dest *int) error {
	ctx, cancel := context.WithTimeout(context.Background(), dbAttemptTimeout)
	defer cancel()

	var result int
	target := &result
	if dest != nil {
		target = dest
	}
	return db.QueryRowContext(ctx, query).Scan(target)
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
