package configstore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/posthog/duckgres/controlplane/trinopool"
	"gorm.io/gorm"
)

// RecordTrinoPoolNodeReplacement preserves the first infrastructure replacement request.
// Retries and subsequent drift observations cannot rewrite the original node identity.
func (cs *ConfigStore) RecordTrinoPoolNodeReplacement(ctx context.Context, lease TrinoPoolLease, instanceID string, evidence trinopool.NodeReplacementEvidence) error {
	if err := evidence.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		var instance TrinoPoolInstance
		if err := tx.Where("instance_id = ? AND pool_id = ?", instanceID, lease.PoolID).First(&instance).Error; err != nil {
			return fmt.Errorf("read instance for node replacement: %w", err)
		}
		if instance.NodeReplacementEvidence != nil || trinopool.Phase(instance.Phase).Terminal() {
			return nil
		}
		return tx.Model(&TrinoPoolInstance{}).Where("instance_id = ? AND pool_id = ? AND node_replacement_evidence IS NULL", instanceID, lease.PoolID).
			Update("node_replacement_evidence", string(encoded)).Error
	})
}
