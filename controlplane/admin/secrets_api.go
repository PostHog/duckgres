//go:build kubernetes

package admin

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// secretStore is the configstore surface for user persistent secrets (the
// concrete *configstore.ConfigStore satisfies it).
type secretStore interface {
	ListOrgUserSecrets(orgID, username string) ([]configstore.OrgUserSecret, error)
	DeleteOrgUserSecret(orgID, username, secretName string) (bool, error)
}

// registerUserSecretsAPI wires list/delete for a user's stored persistent
// secrets. The ciphertext is never returned (json:"-" on the model); only names
// + timestamps are surfaced. DELETE removes the stored statement so future
// sessions stop replaying it (and the next session-create wipe drops it from
// the worker) — it does not reach into a live session.
func registerUserSecretsAPI(r *gin.RouterGroup, store secretStore) {
	r.GET("/orgs/:id/users/:username/secrets", func(c *gin.Context) {
		secrets, err := store.ListOrgUserSecrets(c.Param("id"), c.Param("username"))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"secrets": secrets})
	})

	r.DELETE("/orgs/:id/users/:username/secrets/:name", func(c *gin.Context) {
		deleted, err := store.DeleteOrgUserSecret(c.Param("id"), c.Param("username"), c.Param("name"))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !deleted {
			c.JSON(http.StatusNotFound, gin.H{"error": "secret not found"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"deleted": c.Param("name")})
	})
}
