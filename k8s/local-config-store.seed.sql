INSERT INTO duckgres_teams (name, max_workers, min_workers, memory_budget, idle_timeout_s, created_at, updated_at)
VALUES ('local', 0, 0, '', 0, NOW(), NOW())
ON CONFLICT (name) DO UPDATE
SET updated_at = NOW();

INSERT INTO duckgres_team_users (username, password, team_name, created_at, updated_at)
VALUES ('postgres', '$2a$10$TQyt73Vw91Q1d7YcE86EVuhms/0u4qBydMDyVvZYlqDwc3/VtQAbm', 'local', NOW(), NOW())
ON CONFLICT (username) DO UPDATE
SET password = EXCLUDED.password,
    team_name = EXCLUDED.team_name,
    updated_at = NOW();
