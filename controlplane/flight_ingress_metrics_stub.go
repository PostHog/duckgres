//go:build !kubernetes

package controlplane

func observeOrgSessionsActive(string, int) {}

func observeOrgPgSessionAccepted(string, bool) {}

func observeSNIRoutingResolution(string, bool) {}

func observeOrgSessionQueueDepth(string, int) {}
