//go:build kubernetes

package controlplane

import "net"

func newMetadataProxySessionRegistry(maxPerOrg int) *metadataProxySessionRegistry {
	if maxPerOrg <= 0 {
		maxPerOrg = 20
	}
	return &metadataProxySessionRegistry{
		maxPerOrg: maxPerOrg,
		byOrg:     make(map[string]map[net.Conn]string),
	}
}
