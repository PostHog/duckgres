//go:build kubernetes

package admin

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// admissionOfferProtocolStore is intentionally narrow: activating the durable
// offer handshake is a one-way compatibility-floor change, not ordinary model
// CRUD.
type admissionOfferProtocolStore interface {
	ActivateOrgConnectionAdmissionOffers() error
}

type admissionOfferProtocolHandler struct {
	store admissionOfferProtocolStore
}

func registerAdmissionOfferProtocolAPI(r *gin.RouterGroup, store admissionOfferProtocolStore) {
	h := &admissionOfferProtocolHandler{store: store}
	r.POST("/admission/offers/activate", RequireAdmin(), h.activate)
}

func (h *admissionOfferProtocolHandler) activate(c *gin.Context) {
	if err := h.store.ActivateOrgConnectionAdmissionOffers(); err != nil {
		if errors.Is(err, configstore.ErrAdmissionOfferProtocolActivationBlocked) {
			setAuditDetail(c, "durable admission offer protocol activation blocked")
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		setAuditDetail(c, "durable admission offer protocol activation failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	setAuditDetail(c, "durable admission offer protocol enabled")
	c.JSON(http.StatusOK, gin.H{"offers_enabled": true})
}
