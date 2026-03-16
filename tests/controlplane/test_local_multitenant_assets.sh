#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

recipe_list="$(just --list --list-submodules)"

grep -q "run-multitenant-local" <<<"$recipe_list"
grep -q "multitenant-config-store-up" <<<"$recipe_list"
grep -q "multitenant-seed-local" <<<"$recipe_list"
grep -q "multitenant-port-forward-pg" <<<"$recipe_list"
grep -q "multitenant-port-forward-admin" <<<"$recipe_list"
grep -q "test-local-multitenant-assets" <<<"$recipe_list"

test -f k8s/local-config-store.compose.yaml
test -f k8s/control-plane-multitenant-local.yaml
test -f k8s/local-config-store.seed.sql

grep -q -- "--config-store" k8s/control-plane-multitenant-local.yaml
grep -q -- "--config-poll-interval" k8s/control-plane-multitenant-local.yaml
grep -q -- "host.docker.internal:5434" k8s/control-plane-multitenant-local.yaml
grep -q -- "duckgres:test" k8s/control-plane-multitenant-local.yaml
grep -q -- "POSTGRES_DB: duckgres_config" k8s/local-config-store.compose.yaml
grep -q -- "INSERT INTO duckgres_teams" k8s/local-config-store.seed.sql
grep -q -- "INSERT INTO duckgres_team_users" k8s/local-config-store.seed.sql
grep -q -- "'postgres', '\$2a\$10\$TQyt73Vw91Q1d7YcE86EVuhms/0u4qBydMDyVvZYlqDwc3/VtQAbm'" k8s/local-config-store.seed.sql
