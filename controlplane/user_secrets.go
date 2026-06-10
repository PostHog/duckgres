package controlplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server/usersecrets"
)

// maxUserSecretsPerUser caps how many persistent secrets one (org, user) can
// store. Generous for real use, small enough that a misbehaving client can't
// bloat the config store.
const maxUserSecretsPerUser = 32

// CPUserSecretManager implements server.UserSecretManager on top of the
// config store: it seals user CREATE PERSISTENT SECRET statements with
// AES-GCM and persists them per (org, user, name) for replay at session
// creation. With no encryption key configured, writes are disabled (Ready
// errors, so the client gets a clear message) but deletes still work, so
// stale rows remain removable.
type CPUserSecretManager struct {
	store  *configstore.ConfigStore
	cipher *usersecrets.Cipher // nil when DUCKGRES_USER_SECRET_KEY is unset
}

// NewCPUserSecretManager builds the manager. encodedKey is the value of
// DUCKGRES_USER_SECRET_KEY; empty disables persistence (not an error), a
// malformed key is a startup error (fail fast on operator mistakes rather
// than surfacing them to customers one statement at a time).
func NewCPUserSecretManager(store *configstore.ConfigStore, encodedKey string) (*CPUserSecretManager, error) {
	m := &CPUserSecretManager{store: store}
	if encodedKey != "" {
		cipher, err := usersecrets.NewCipher(encodedKey)
		if err != nil {
			return nil, fmt.Errorf("user secret key: %w", err)
		}
		m.cipher = cipher
	}
	return m, nil
}

// Ready implements server.UserSecretManager.
func (m *CPUserSecretManager) Ready() error {
	if m.cipher == nil {
		return errors.New("the operator has not configured a secret encryption key (" + usersecrets.EnvKeyName + ")")
	}
	return nil
}

// PutSecret implements server.UserSecretManager.
func (m *CPUserSecretManager) PutSecret(_ context.Context, orgID, username, secretName, statement string) error {
	if err := m.Ready(); err != nil {
		return err
	}
	sealed, err := m.cipher.Seal(orgID, username, secretName, statement)
	if err != nil {
		return fmt.Errorf("seal secret: %w", err)
	}
	if err := m.store.UpsertOrgUserSecret(orgID, username, secretName, sealed, maxUserSecretsPerUser); err != nil {
		if errors.Is(err, configstore.ErrTooManyUserSecrets) {
			return err
		}
		return fmt.Errorf("store secret: %w", err)
	}
	return nil
}

// DeleteSecret implements server.UserSecretManager. Works without a cipher so
// rows written under a lost key can still be removed.
func (m *CPUserSecretManager) DeleteSecret(_ context.Context, orgID, username, secretName string) (bool, error) {
	return m.store.DeleteOrgUserSecret(orgID, username, secretName)
}

// LoadStatements returns the user's decrypted secret statements for replay at
// session creation. Rows that fail to decrypt (e.g. written under a rotated
// key) are skipped with a loud log instead of failing the whole session.
func (m *CPUserSecretManager) LoadStatements(_ context.Context, orgID, username string) ([]string, error) {
	if m.cipher == nil {
		return nil, nil
	}
	rows, err := m.store.ListOrgUserSecrets(orgID, username)
	if err != nil {
		return nil, fmt.Errorf("list user secrets: %w", err)
	}
	statements := make([]string, 0, len(rows))
	for _, row := range rows {
		stmt, err := m.cipher.Open(row.OrgID, row.Username, row.SecretName, row.Ciphertext)
		if err != nil {
			slog.Error("User persistent secret cannot be decrypted; skipping replay (was the encryption key rotated?).",
				"org", row.OrgID, "user", row.Username, "secret", row.SecretName, "error", err)
			continue
		}
		statements = append(statements, stmt)
	}
	return statements, nil
}

// SessionSecretLoader binds an org onto LoadStatements in the shape
// SessionManager.SetUserSecretLoader expects.
func (m *CPUserSecretManager) SessionSecretLoader(orgID string) func(ctx context.Context, username string) ([]string, error) {
	return func(ctx context.Context, username string) ([]string, error) {
		return m.LoadStatements(ctx, orgID, username)
	}
}
