package k8s_test

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func isTransientDBError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	for _, fragment := range []string{
		"eof",
		"connection reset",
		"broken pipe",
		"bad connection",
		"connection refused",
		"lost connection to pod",
		"i/o timeout",
		"no route to host",
	} {
		if strings.Contains(msg, fragment) {
			return true
		}
	}

	return false
}

func findReadyPodName(pods []corev1.Pod) (string, bool) {
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				return pod.Name, true
			}
		}
	}

	return "", false
}

func isPodGoneError(err error) bool {
	return apierrors.IsNotFound(err)
}
