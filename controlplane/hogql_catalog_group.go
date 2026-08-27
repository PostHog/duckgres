//go:build kubernetes

package controlplane

import (
	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/hogqlcatalog"
)

func registerHogQLCatalogGroup(
	engine *gin.Engine,
	internalTokens admin.TokenSet,
	reader hogqlcatalog.Reader,
	publisher hogqlcatalog.Publisher,
) {
	api := engine.Group("/v1/hogql", admin.AnyTokenAuthMiddleware(internalTokens))
	hogqlcatalog.RegisterAPI(api, reader, publisher)
}
