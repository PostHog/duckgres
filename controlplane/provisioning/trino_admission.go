package provisioning

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

var ErrTrinoCellSelectionRequired = errors.New("select an initial Trino cell before enabling Trino")
var ErrTrinoCellNotConfigured = errors.New("the assigned Trino cell is not configured")

func (h *handler) admitTrino(c *gin.Context, orgID string) bool {
	if h.trinoAdmission == nil {
		return true
	}
	if err := h.trinoAdmission(orgID); err != nil {
		if errors.Is(err, ErrTrinoCellSelectionRequired) || errors.Is(err, ErrTrinoCellNotConfigured) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot verify Trino cell assignment"})
		}
		return false
	}
	return true
}
