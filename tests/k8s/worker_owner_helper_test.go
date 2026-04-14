package k8s_test

import corev1 "k8s.io/api/core/v1"

func workerPodsByControlPlaneLabel(pods []corev1.Pod) map[string][]string {
	owned := make(map[string][]string)
	for _, pod := range pods {
		cpName := pod.Labels["duckgres/control-plane"]
		if cpName == "" {
			continue
		}
		owned[cpName] = append(owned[cpName], pod.Name)
	}
	return owned
}
