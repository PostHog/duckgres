//go:build !kubernetes

package controlplane

// extractOrgFromSNI is a no-op stub in non-kubernetes builds. The
// multi-tenant control-plane code paths that consume this method are gated
// at runtime on cp.configStore != nil, which is only true in the
// kubernetes-tagged build, so this stub exists solely to satisfy the
// compiler in single-tenant / local-dev builds.
func (cp *ControlPlane) extractOrgFromSNI(_ string) (string, bool) {
	return "", false
}
