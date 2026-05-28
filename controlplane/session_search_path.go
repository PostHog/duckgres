package controlplane

import "strings"

type sessionSearchPathSource string

const (
	sessionSearchPathSourceClient            sessionSearchPathSource = "client"
	sessionSearchPathSourceConfiguredDefault sessionSearchPathSource = "configured_default"
)

func effectiveSessionSearchPath(clientSearchPath, configuredDefaultSearchPath string) (string, sessionSearchPathSource) {
	switch {
	case clientSearchPath != "":
		return ensureMemoryMainInSearchPath(clientSearchPath), sessionSearchPathSourceClient
	case configuredDefaultSearchPath != "":
		return ensureMemoryMainInSearchPath(configuredDefaultSearchPath), sessionSearchPathSourceConfiguredDefault
	default:
		return "", ""
	}
}

func ensureMemoryMainInSearchPath(searchPath string) string {
	if strings.Contains(strings.ToLower(searchPath), "memory.main") {
		return searchPath
	}
	return searchPath + ",memory.main"
}
