package k8s_test

import (
	"fmt"
	"strings"
	"time"
)

func formatWorkerRetirementTimeoutDiagnostic(podName string, timeout time.Duration, runtimeRecord, workerPods, controlPlaneLogs string) string {
	sections := []string{
		fmt.Sprintf("worker pod %s did not reach retired state within %s", podName, timeout),
		"runtime record:",
		strings.TrimSpace(emptyIfBlank(runtimeRecord, "<none>")),
		"worker pods:",
		strings.TrimSpace(emptyIfBlank(workerPods, "<none>")),
		"control-plane logs:",
		strings.TrimSpace(emptyIfBlank(controlPlaneLogs, "<none>")),
	}
	return strings.Join(sections, "\n")
}

func emptyIfBlank(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
