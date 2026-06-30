//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/admin"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// clusterPeerFetcher implements admin.PeerFetcher. Live session/query state is
// in-memory per CP, so a single replica only knows the sessions it owns; behind
// the load-balancer that makes the dashboard's live numbers flicker as polls
// land on different pods. This fetcher GETs the other replicas' local views and
// returns their raw bodies for the admin handler to concatenate (a session is
// owned by exactly one CP, so the union is disjoint — no dedup needed).
type clusterPeerFetcher struct {
	clientset      kubernetes.Interface
	namespace      string
	selfPod        string // own pod name (POD_NAME / pool cpID)
	deployPrefix   string // pod-name prefix shared by all CP replicas
	internalSecret string
	port           int
	client         *http.Client
}

func newClusterPeerFetcher(clientset kubernetes.Interface, namespace, selfPod, internalSecret string, port int) *clusterPeerFetcher {
	return &clusterPeerFetcher{
		clientset:      clientset,
		namespace:      namespace,
		selfPod:        selfPod,
		deployPrefix:   cpDeploymentPrefix(selfPod),
		internalSecret: internalSecret,
		port:           port,
		client:         &http.Client{Timeout: 2 * time.Second},
	}
}

var _ admin.PeerFetcher = (*clusterPeerFetcher)(nil)

// cpDeploymentPrefix strips the trailing "-<replicaset>-<podsuffix>" from a CP
// pod name, leaving the prefix every replica shares across rollouts (e.g.
// "duckgres-control-plane"). Worker/cache/placeholder pods have different
// prefixes, so a HasPrefix match against it is control-plane-only.
func cpDeploymentPrefix(podName string) string {
	parts := strings.Split(podName, "-")
	if len(parts) <= 2 {
		return podName
	}
	return strings.Join(parts[:len(parts)-2], "-")
}

// FetchPeers returns each peer CP's body for path+"?scope=local" and the total
// number of peers attempted. Peers are fetched concurrently with a short
// per-peer timeout; a slow/down peer is simply omitted (graceful degradation).
func (f *clusterPeerFetcher) FetchPeers(ctx context.Context, path string) ([][]byte, int) {
	if f == nil || f.clientset == nil || f.selfPod == "" {
		return nil, 0
	}
	ips := f.discoverPeerIPs(ctx)
	if len(ips) == 0 {
		return nil, 0
	}
	bodies := make([][]byte, 0, len(ips))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ip := range ips {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			if b, ok := f.fetchOne(ctx, ip, path); ok {
				mu.Lock()
				bodies = append(bodies, b)
				mu.Unlock()
			}
		}(ip)
	}
	wg.Wait()
	return bodies, len(ips)
}

func (f *clusterPeerFetcher) discoverPeerIPs(ctx context.Context) []string {
	lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	pods, err := f.clientset.CoreV1().Pods(f.namespace).List(lctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=duckgres",
	})
	if err != nil {
		return nil
	}
	var ips []string
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Name == f.selfPod {
			continue
		}
		if !strings.HasPrefix(p.Name, f.deployPrefix+"-") {
			continue
		}
		if p.Status.Phase != corev1.PodRunning || p.Status.PodIP == "" {
			continue
		}
		ips = append(ips, p.Status.PodIP)
	}
	return ips
}

func (f *clusterPeerFetcher) fetchOne(ctx context.Context, ip, path string) ([]byte, bool) {
	rctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	url := fmt.Sprintf("http://%s:%d%s?scope=local", ip, f.port, path)
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false
	}
	// Authenticate to the peer's admin API as the internal secret (= admin).
	req.Header.Set("X-Duckgres-Internal-Secret", f.internalSecret)
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, false
	}
	return b, true
}
