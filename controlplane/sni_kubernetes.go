//go:build kubernetes

package controlplane

import "strings"

// extractOrgFromSNI parses the org identifier from a TLS ServerName.
// Returns the prefix as orgName when sni == "<orgName>" + one of the
// configured ManagedHostnameSuffixes, where orgName is a single DNS label
// (no dots). Returns ("", false) when sni is empty, doesn't match any
// suffix, or has a multi-label prefix — caller should fall back to legacy
// routing (in passthrough mode) or reject the connection (in enforce mode).
//
// Single-label restriction: the wildcard cert *.dw.{env}.postwh.com only
// covers single-label prefixes, so a multi-label SNI ("evil.acme.dw...")
// would have already failed TLS — but we keep the check as defense in depth.
func (cp *ControlPlane) extractOrgFromSNI(sni string) (string, bool) {
	for _, suffix := range cp.cfg.ManagedHostnameSuffixes {
		if !strings.HasSuffix(sni, suffix) {
			continue
		}
		prefix := strings.TrimSuffix(sni, suffix)
		if prefix == "" || strings.ContainsRune(prefix, '.') {
			continue
		}
		return prefix, true
	}
	return "", false
}
