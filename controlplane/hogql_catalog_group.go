//go:build kubernetes

package controlplane

import (
	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/hogqlcatalog"
)

func registerHogQLCatalogGroup(
	engine *gin.Engine,
	readOnlyTokens, adminTokens admin.TokenSet,
	reader hogqlcatalog.Reader,
	publisher hogqlcatalog.Publisher,
) {
	readAPI := engine.Group("/v1/hogql", admin.AnyTokenAuthMiddleware(readOnlyTokens, adminTokens))
	publishAPI := engine.Group("/v1/hogql", admin.AnyTokenAuthMiddleware(adminTokens))
	hogqlcatalog.RegisterAPI(readAPI, publishAPI, reader, publisher)
}
