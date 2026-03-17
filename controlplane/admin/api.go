//go:build kubernetes

package admin

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// WorkerStatus represents a worker's current status for the API.
type WorkerStatus struct {
	ID             int    `json:"id"`
	Team           string `json:"team"`
	ActiveSessions int    `json:"active_sessions"`
	Status         string `json:"status"`
}

// SessionStatus represents an active session for the API.
type SessionStatus struct {
	PID      int32  `json:"pid"`
	WorkerID int    `json:"worker_id"`
	Team     string `json:"team"`
}

// ClusterStatus aggregates cluster state for the dashboard.
type ClusterStatus struct {
	TotalTeams    int          `json:"total_teams"`
	TotalWorkers  int          `json:"total_workers"`
	TotalSessions int          `json:"total_sessions"`
	Teams         []TeamStatus `json:"teams"`
}

// TeamStatus is a per-team summary.
type TeamStatus struct {
	Name           string `json:"name"`
	Workers        int    `json:"workers"`
	ActiveSessions int    `json:"active_sessions"`
	MaxWorkers     int    `json:"max_workers"`
	MinWorkers     int    `json:"min_workers"`
	MemoryBudget   string `json:"memory_budget"`
}

// TeamStackInfo provides info about a team's live state.
// Implemented by the controlplane.TeamRouter via adapter.
type TeamStackInfo interface {
	// AllTeamStats returns per-team worker and session counts.
	AllTeamStats() []TeamStatus
	// AllWorkerStatuses returns all workers across teams.
	AllWorkerStatuses() []WorkerStatus
	// AllSessionStatuses returns all active sessions across teams.
	AllSessionStatuses() []SessionStatus
}

// RegisterAPI registers all admin REST endpoints on the given router group.
func RegisterAPI(r *gin.RouterGroup, store *configstore.ConfigStore, info TeamStackInfo) {
	registerAPIWithStore(r, newGormAPIStore(store), info)
}

func registerAPIWithStore(r *gin.RouterGroup, store apiStore, info TeamStackInfo) {
	h := &apiHandler{store: store, info: info}

	// Teams CRUD
	r.GET("/teams", h.listTeams)
	r.POST("/teams", h.createTeam)
	r.GET("/teams/:name", h.getTeam)
	r.PUT("/teams/:name", h.updateTeam)
	r.DELETE("/teams/:name", h.deleteTeam)
	r.GET("/teams/:name/warehouse", h.getManagedWarehouse)
	r.PUT("/teams/:name/warehouse", h.putManagedWarehouse)

	// Users CRUD
	r.GET("/users", h.listUsers)
	r.POST("/users", h.createUser)
	r.GET("/users/:username", h.getUser)
	r.PUT("/users/:username", h.updateUser)
	r.DELETE("/users/:username", h.deleteUser)

	// Workers (read-only)
	r.GET("/workers", h.listWorkers)

	// Sessions (read-only)
	r.GET("/sessions", h.listSessions)

	// Config singletons
	r.GET("/config/global", h.getGlobalConfig)
	r.PUT("/config/global", h.updateGlobalConfig)
	r.GET("/config/ducklake", h.getDuckLakeConfig)
	r.PUT("/config/ducklake", h.updateDuckLakeConfig)
	r.GET("/config/ratelimit", h.getRateLimitConfig)
	r.PUT("/config/ratelimit", h.updateRateLimitConfig)
	r.GET("/config/querylog", h.getQueryLogConfig)
	r.PUT("/config/querylog", h.updateQueryLogConfig)

	// Overview
	r.GET("/status", h.getClusterStatus)
}

type apiStore interface {
	ListTeams() ([]configstore.Team, error)
	CreateTeam(team *configstore.Team) error
	GetTeam(name string) (*configstore.Team, error)
	UpdateTeam(name string, updates configstore.Team) (*configstore.Team, bool, error)
	DeleteTeam(name string) (bool, error)

	ListUsers() ([]configstore.TeamUser, error)
	CreateUser(user *configstore.TeamUser) error
	GetUser(username string) (*configstore.TeamUser, error)
	UpdateUser(username, passwordHash, teamName string) (*configstore.TeamUser, bool, error)
	DeleteUser(username string) (bool, error)

	GetManagedWarehouse(teamName string) (*configstore.ManagedWarehouse, error)
	UpsertManagedWarehouse(teamName string, warehouse *configstore.ManagedWarehouse) (*configstore.ManagedWarehouse, bool, error)

	GetGlobalConfig() (configstore.GlobalConfig, error)
	SaveGlobalConfig(cfg *configstore.GlobalConfig) error
	GetDuckLakeConfig() (configstore.DuckLakeConfig, error)
	SaveDuckLakeConfig(cfg *configstore.DuckLakeConfig) error
	GetRateLimitConfig() (configstore.RateLimitConfig, error)
	SaveRateLimitConfig(cfg *configstore.RateLimitConfig) error
	GetQueryLogConfig() (configstore.QueryLogConfig, error)
	SaveQueryLogConfig(cfg *configstore.QueryLogConfig) error
}

type gormAPIStore struct {
	store *configstore.ConfigStore
}

func newGormAPIStore(store *configstore.ConfigStore) apiStore {
	return &gormAPIStore{store: store}
}

func (s *gormAPIStore) db() *gorm.DB {
	return s.store.DB()
}

func (s *gormAPIStore) ListTeams() ([]configstore.Team, error) {
	var teams []configstore.Team
	if err := s.db().Preload("Users").Preload("Warehouse").Find(&teams).Error; err != nil {
		return nil, err
	}
	return teams, nil
}

func (s *gormAPIStore) CreateTeam(team *configstore.Team) error {
	team.Warehouse = nil
	return s.db().Omit("Warehouse").Create(team).Error
}

func (s *gormAPIStore) GetTeam(name string) (*configstore.Team, error) {
	var team configstore.Team
	if err := s.db().Preload("Users").Preload("Warehouse").First(&team, "name = ?", name).Error; err != nil {
		return nil, err
	}
	return &team, nil
}

func (s *gormAPIStore) UpdateTeam(name string, updates configstore.Team) (*configstore.Team, bool, error) {
	result := s.db().Model(&configstore.Team{}).Where("name = ?", name).Updates(map[string]interface{}{
		"max_workers":    updates.MaxWorkers,
		"min_workers":    updates.MinWorkers,
		"memory_budget":  updates.MemoryBudget,
		"idle_timeout_s": updates.IdleTimeoutS,
	})
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, false, nil
	}
	team, err := s.GetTeam(name)
	if err != nil {
		return nil, true, err
	}
	return team, true, nil
}

func (s *gormAPIStore) DeleteTeam(name string) (bool, error) {
	returnRows := int64(0)
	err := s.db().Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("team_name = ?", name).Delete(&configstore.TeamUser{}).Error; err != nil {
			return err
		}
		result := tx.Where("name = ?", name).Delete(&configstore.Team{})
		if result.Error != nil {
			return result.Error
		}
		returnRows = result.RowsAffected
		return nil
	})
	if err != nil {
		return false, err
	}
	return returnRows > 0, nil
}

func (s *gormAPIStore) ListUsers() ([]configstore.TeamUser, error) {
	var users []configstore.TeamUser
	if err := s.db().Find(&users).Error; err != nil {
		return nil, err
	}
	return users, nil
}

func (s *gormAPIStore) CreateUser(user *configstore.TeamUser) error {
	return s.db().Create(user).Error
}

func (s *gormAPIStore) GetUser(username string) (*configstore.TeamUser, error) {
	var user configstore.TeamUser
	if err := s.db().First(&user, "username = ?", username).Error; err != nil {
		return nil, err
	}
	return &user, nil
}

func (s *gormAPIStore) UpdateUser(username, passwordHash, teamName string) (*configstore.TeamUser, bool, error) {
	updates := map[string]interface{}{}
	if passwordHash != "" {
		updates["password"] = passwordHash
	}
	if teamName != "" {
		updates["team_name"] = teamName
	}
	result := s.db().Model(&configstore.TeamUser{}).Where("username = ?", username).Updates(updates)
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, false, nil
	}
	user, err := s.GetUser(username)
	if err != nil {
		return nil, true, err
	}
	return user, true, nil
}

func (s *gormAPIStore) DeleteUser(username string) (bool, error) {
	result := s.db().Where("username = ?", username).Delete(&configstore.TeamUser{})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (s *gormAPIStore) GetManagedWarehouse(teamName string) (*configstore.ManagedWarehouse, error) {
	var warehouse configstore.ManagedWarehouse
	if err := s.db().First(&warehouse, "team_name = ?", teamName).Error; err != nil {
		return nil, err
	}
	return &warehouse, nil
}

func (s *gormAPIStore) UpsertManagedWarehouse(teamName string, warehouse *configstore.ManagedWarehouse) (*configstore.ManagedWarehouse, bool, error) {
	var count int64
	if err := s.db().Model(&configstore.Team{}).Where("name = ?", teamName).Count(&count).Error; err != nil {
		return nil, false, err
	}
	if count == 0 {
		return nil, false, nil
	}

	warehouse.TeamName = teamName
	if err := s.db().Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "team_name"}},
		DoUpdates: clause.AssignmentColumns(managedWarehouseUpsertColumns()),
	}).Create(warehouse).Error; err != nil {
		return nil, true, err
	}
	stored, err := s.GetManagedWarehouse(teamName)
	if err != nil {
		return nil, true, err
	}
	return stored, true, nil
}

func managedWarehouseUpsertColumns() []string {
	return []string{
		"aurora_region",
		"aurora_endpoint",
		"aurora_port",
		"aurora_database_name",
		"aurora_username",
		"s3_provider",
		"s3_region",
		"s3_bucket",
		"s3_path_prefix",
		"s3_endpoint",
		"s3_use_ssl",
		"s3_url_style",
		"worker_identity_namespace",
		"worker_identity_service_account_name",
		"worker_identity_iam_role_arn",
		"aurora_credentials_namespace",
		"aurora_credentials_name",
		"aurora_credentials_key",
		"s3_credentials_namespace",
		"s3_credentials_name",
		"s3_credentials_key",
		"runtime_config_namespace",
		"runtime_config_name",
		"runtime_config_key",
		"state",
		"status_message",
		"aurora_state",
		"aurora_status_message",
		"s3_state",
		"s3_status_message",
		"identity_state",
		"identity_status_message",
		"secrets_state",
		"secrets_status_message",
		"ready_at",
		"failed_at",
		"updated_at",
	}
}

func (s *gormAPIStore) GetGlobalConfig() (configstore.GlobalConfig, error) {
	var cfg configstore.GlobalConfig
	if err := s.db().First(&cfg, 1).Error; err != nil {
		return configstore.GlobalConfig{}, err
	}
	return cfg, nil
}

func (s *gormAPIStore) SaveGlobalConfig(cfg *configstore.GlobalConfig) error {
	return s.db().Save(cfg).Error
}

func (s *gormAPIStore) GetDuckLakeConfig() (configstore.DuckLakeConfig, error) {
	var cfg configstore.DuckLakeConfig
	if err := s.db().First(&cfg, 1).Error; err != nil {
		return configstore.DuckLakeConfig{}, err
	}
	return cfg, nil
}

func (s *gormAPIStore) SaveDuckLakeConfig(cfg *configstore.DuckLakeConfig) error {
	return s.db().Save(cfg).Error
}

func (s *gormAPIStore) GetRateLimitConfig() (configstore.RateLimitConfig, error) {
	var cfg configstore.RateLimitConfig
	if err := s.db().First(&cfg, 1).Error; err != nil {
		return configstore.RateLimitConfig{}, err
	}
	return cfg, nil
}

func (s *gormAPIStore) SaveRateLimitConfig(cfg *configstore.RateLimitConfig) error {
	return s.db().Save(cfg).Error
}

func (s *gormAPIStore) GetQueryLogConfig() (configstore.QueryLogConfig, error) {
	var cfg configstore.QueryLogConfig
	if err := s.db().First(&cfg, 1).Error; err != nil {
		return configstore.QueryLogConfig{}, err
	}
	return cfg, nil
}

func (s *gormAPIStore) SaveQueryLogConfig(cfg *configstore.QueryLogConfig) error {
	return s.db().Save(cfg).Error
}

type apiHandler struct {
	store apiStore
	info  TeamStackInfo
}

// --- Teams ---

func (h *apiHandler) listTeams(c *gin.Context) {
	teams, err := h.store.ListTeams()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, teams)
}

func (h *apiHandler) createTeam(c *gin.Context) {
	var team configstore.Team
	if err := c.ShouldBindJSON(&team); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if team.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	if err := h.store.CreateTeam(&team); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, team)
}

func (h *apiHandler) getTeam(c *gin.Context) {
	name := c.Param("name")
	team, err := h.store.GetTeam(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "team not found"})
		return
	}
	c.JSON(http.StatusOK, team)
}

func (h *apiHandler) updateTeam(c *gin.Context) {
	name := c.Param("name")
	var updates configstore.Team
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	team, ok, err := h.store.UpdateTeam(name, updates)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "team not found"})
		return
	}
	c.JSON(http.StatusOK, team)
}

func (h *apiHandler) deleteTeam(c *gin.Context) {
	name := c.Param("name")
	ok, err := h.store.DeleteTeam(name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "team not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": name})
}

func (h *apiHandler) getManagedWarehouse(c *gin.Context) {
	warehouse, err := h.store.GetManagedWarehouse(c.Param("name"))
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "managed warehouse not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, warehouse)
}

func (h *apiHandler) putManagedWarehouse(c *gin.Context) {
	teamName := c.Param("name")
	var warehouse configstore.ManagedWarehouse
	if err := c.ShouldBindJSON(&warehouse); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	stored, ok, err := h.store.UpsertManagedWarehouse(teamName, &warehouse)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "team not found"})
		return
	}
	c.JSON(http.StatusOK, stored)
}

// --- Users ---

func (h *apiHandler) listUsers(c *gin.Context) {
	users, err := h.store.ListUsers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, users)
}

func (h *apiHandler) createUser(c *gin.Context) {
	// Use a raw struct because TeamUser.Password has json:"-"
	var raw struct {
		Username string `json:"username"`
		Password string `json:"password"`
		TeamName string `json:"team_name"`
	}
	if err := c.ShouldBindJSON(&raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if raw.Username == "" || raw.TeamName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "username and team_name are required"})
		return
	}
	if raw.Password == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "password is required"})
		return
	}
	hash, err := configstore.HashPassword(raw.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
		return
	}
	user := configstore.TeamUser{
		Username: raw.Username,
		Password: hash,
		TeamName: raw.TeamName,
	}
	if err := h.store.CreateUser(&user); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, user)
}

func (h *apiHandler) getUser(c *gin.Context) {
	username := c.Param("username")
	user, err := h.store.GetUser(username)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	c.JSON(http.StatusOK, user)
}

func (h *apiHandler) updateUser(c *gin.Context) {
	username := c.Param("username")
	var raw struct {
		Password string `json:"password"`
		TeamName string `json:"team_name"`
	}
	if err := c.ShouldBindJSON(&raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	passwordHash := ""
	if raw.Password != "" {
		hash, err := configstore.HashPassword(raw.Password)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
			return
		}
		passwordHash = hash
	}
	user, ok, err := h.store.UpdateUser(username, passwordHash, raw.TeamName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	c.JSON(http.StatusOK, user)
}

func (h *apiHandler) deleteUser(c *gin.Context) {
	username := c.Param("username")
	ok, err := h.store.DeleteUser(username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": username})
}

// --- Workers ---

func (h *apiHandler) listWorkers(c *gin.Context) {
	if h.info == nil {
		c.JSON(http.StatusOK, []WorkerStatus{})
		return
	}
	c.JSON(http.StatusOK, h.info.AllWorkerStatuses())
}

// --- Sessions ---

func (h *apiHandler) listSessions(c *gin.Context) {
	if h.info == nil {
		c.JSON(http.StatusOK, []SessionStatus{})
		return
	}
	c.JSON(http.StatusOK, h.info.AllSessionStatuses())
}

// --- Config Singletons ---

func (h *apiHandler) getGlobalConfig(c *gin.Context) {
	cfg, err := h.store.GetGlobalConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateGlobalConfig(c *gin.Context) {
	var cfg configstore.GlobalConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	if err := h.store.SaveGlobalConfig(&cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) getDuckLakeConfig(c *gin.Context) {
	cfg, err := h.store.GetDuckLakeConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateDuckLakeConfig(c *gin.Context) {
	var cfg configstore.DuckLakeConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	if err := h.store.SaveDuckLakeConfig(&cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) getRateLimitConfig(c *gin.Context) {
	cfg, err := h.store.GetRateLimitConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateRateLimitConfig(c *gin.Context) {
	var cfg configstore.RateLimitConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	if err := h.store.SaveRateLimitConfig(&cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) getQueryLogConfig(c *gin.Context) {
	cfg, err := h.store.GetQueryLogConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateQueryLogConfig(c *gin.Context) {
	var cfg configstore.QueryLogConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	if err := h.store.SaveQueryLogConfig(&cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

// --- Status ---

func (h *apiHandler) getClusterStatus(c *gin.Context) {
	if h.info == nil {
		c.JSON(http.StatusOK, ClusterStatus{})
		return
	}

	teamStats := h.info.AllTeamStats()
	totalWorkers := 0
	totalSessions := 0
	for _, ts := range teamStats {
		totalWorkers += ts.Workers
		totalSessions += ts.ActiveSessions
	}

	c.JSON(http.StatusOK, ClusterStatus{
		TotalTeams:    len(teamStats),
		TotalWorkers:  totalWorkers,
		TotalSessions: totalSessions,
		Teams:         teamStats,
	})
}
