//go:build kubernetes

package controlplane

func init() {
	RegisterK8sPoolFactory(func(cfg K8sWorkerPoolConfig) (WorkerPool, error) {
		pool, err := NewK8sWorkerPool(cfg)
		if err != nil {
			return nil, err
		}
		return pool, nil
	})
}
