package provisioning

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

type TrinoServiceCredentialValidator interface {
	ValidateTrinoServiceCredential(context.Context, string, string) (*configstore.TrinoServiceCredentialIdentity, error)
}

// RegisterTrinoServiceCredentialAuth mounts only the credential-check surface.
// Its dedicated token must not be accepted by any provisioning/admin route.
func RegisterTrinoServiceCredentialAuth(engine *gin.Engine, store TrinoServiceCredentialValidator, secret string) {
	if secret == "" {
		return
	}
	var expected [][sha256.Size]byte
	for _, token := range strings.Fields(secret) {
		expected = append(expected, sha256.Sum256([]byte(token)))
	}
	engine.POST("/auth/trino/service-credentials", func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		authorization := c.GetHeader("Authorization")
		provided := sha256.Sum256([]byte(strings.TrimPrefix(authorization, "Bearer ")))
		match := 0
		for _, digest := range expected {
			match |= subtle.ConstantTimeCompare(digest[:], provided[:])
		}
		if !strings.HasPrefix(authorization, "Bearer ") || match != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
		var input struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		decoder := json.NewDecoder(c.Request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()
		identity, err := store.ValidateTrinoServiceCredential(ctx, input.Username, input.Password)
		if errors.Is(err, configstore.ErrTrinoServiceCredentialDenied) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
			return
		}
		if err != nil {
			reason := "backend_error"
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				reason = "timeout"
			}
			slog.Warn("Trino service credential validation unavailable", "reason", reason)
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "credential validation unavailable"})
			return
		}
		c.JSON(http.StatusOK, identity)
	})
}
