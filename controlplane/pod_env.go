//go:build kubernetes

package controlplane

import (
	"log/slog"
	"sync"

	corev1 "k8s.io/api/core/v1"
)

// posthogLogEnvAllowlist is copied onto worker and reshard pods from the CP
// container's named env (never envFrom, never os.Getenv → value:).
// ADDITIONAL_POSTHOG_API_KEYS and POSTHOG_ANALYTICS_API_KEY stay CP-only.
var posthogLogEnvAllowlist = []string{
	"POSTHOG_API_KEY",
	"POSTHOG_HOST",
	"DUCKGRES_POSTHOG_LOG_LEVEL",
	"DUCKGRES_POSTHOG_LOG_INFO_SAMPLE",
	"DUCKGRES_POSTHOG_LOG_QUERY_TEXT",
	"DUCKGRES_IDENTIFIER",
}

var refusePostHogAPIKeyValueOnce sync.Once

// copyAllowlistedEnv DeepCopies named entries from src. Missing names are
// omitted. POSTHOG_API_KEY is copied only as a secretKeyRef — a literal
// value: is refused so the token never appears in a child pod spec.
func copyAllowlistedEnv(src []corev1.EnvVar, allowlist []string) []corev1.EnvVar {
	byName := make(map[string]corev1.EnvVar, len(src))
	for i := range src {
		byName[src[i].Name] = src[i]
	}
	var out []corev1.EnvVar
	for _, name := range allowlist {
		e, ok := byName[name]
		if !ok {
			continue
		}
		if name == "POSTHOG_API_KEY" && !envIsSecretKeyRef(e) {
			if e.Value != "" {
				refusePostHogAPIKeyValueOnce.Do(func() {
					slog.Warn("refusing to materialize POSTHOG_API_KEY as a pod spec value")
				})
			}
			continue
		}
		out = append(out, *e.DeepCopy())
	}
	return out
}

func envIsSecretKeyRef(e corev1.EnvVar) bool {
	return e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil && e.ValueFrom.SecretKeyRef.Name != ""
}

func filterPostHogLogEnv(src []corev1.EnvVar) []corev1.EnvVar {
	return copyAllowlistedEnv(src, posthogLogEnvAllowlist)
}

func envHasName(env []corev1.EnvVar, name string) bool {
	for i := range env {
		if env[i].Name == name {
			return true
		}
	}
	return false
}

func namedPostHogAPIKey(env []corev1.EnvVar) (present, secretRef bool) {
	for i := range env {
		if env[i].Name == "POSTHOG_API_KEY" {
			return true, envIsSecretKeyRef(env[i])
		}
	}
	return false, false
}

// downwardAPIIdentityEnv stamps pod/node/namespace onto child pods next to
// the existing POD_NAME / NODE_NAME Downward API fields.
func downwardAPIIdentityEnv() []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
		{
			Name: "NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			},
		},
	}
}
