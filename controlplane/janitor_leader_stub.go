//go:build !kubernetes

package controlplane

import "context"

type JanitorLeaderManager struct{}

func NewJanitorLeaderManager(namespace, identity string, janitor *ControlPlaneJanitor) (*JanitorLeaderManager, error) {
	return nil, nil
}

func (m *JanitorLeaderManager) Start(ctx context.Context) error {
	return nil
}

func (m *JanitorLeaderManager) Stop() {}
