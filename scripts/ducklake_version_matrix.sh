#!/usr/bin/env bash
# ducklake_version_matrix.sh — Run DuckLake concurrency benchmarks across
# multiple DuckDB/DuckLake versions and compare results.
#
# Each version is tested in an isolated git worktree so the main tree is
# never modified. Results are JSON files written to a results directory.
#
# Usage:
#   ./scripts/ducklake_version_matrix.sh                   # run matrix
#   ./scripts/ducklake_version_matrix.sh --current-only    # benchmark current version only
#   DUCKLAKE_VERSIONS="v2.10501.0" ./scripts/ducklake_version_matrix.sh  # custom versions
#
# Requires: Docker running (for DuckLake infra), go, git, jq (for comparison)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS_DIR="${DUCKLAKE_RESULTS_DIR:-$REPO_ROOT/bench-results}"
COMPOSE_FILE="$REPO_ROOT/tests/integration/docker-compose.yml"
TEST_TIMEOUT="${DUCKLAKE_TEST_TIMEOUT:-300s}"

# DuckDB Go driver versions to test.
# Format: "module_version" — maps to duckdb-go/v2 + duckdb-go-bindings.
#   v2.10500.0 → DuckDB 1.5.0 (DuckLake 0.3)
#   v2.10501.0 → DuckDB 1.5.1 (DuckLake 0.4)
DEFAULT_VERSIONS="v2.10501.0 v2.10500.0"
VERSIONS="${DUCKLAKE_VERSIONS:-$DEFAULT_VERSIONS}"

CURRENT_ONLY=false
if [[ "${1:-}" == "--current-only" ]]; then
    CURRENT_ONLY=true
fi

mkdir -p "$RESULTS_DIR"

# --- Helpers ---

log() { echo "=== $* ===" >&2; }

ensure_infra() {
    log "Ensuring DuckLake infrastructure is running"
    if ! nc -z 127.0.0.1 35433 2>/dev/null || ! nc -z 127.0.0.1 39000 2>/dev/null; then
        docker-compose -f "$COMPOSE_FILE" up -d ducklake-metadata minio minio-init
        echo "Waiting for infrastructure..." >&2
        for i in $(seq 1 30); do
            if nc -z 127.0.0.1 35433 2>/dev/null && nc -z 127.0.0.1 39000 2>/dev/null; then
                echo "Infrastructure ready." >&2
                return 0
            fi
            sleep 1
        done
        echo "ERROR: Infrastructure not ready after 30s" >&2
        return 1
    fi
    echo "Infrastructure already running." >&2
}

# Derive the bindings version from the driver version.
# v2.10501.0 → v0.10501.0 (strip leading digit, prefix v0.)
bindings_version() {
    echo "$1" | sed 's/^v2\./v0./'
}

run_benchmark() {
    local work_dir="$1"
    local label="$2"
    local out_file="$RESULTS_DIR/${label}.json"

    log "Running benchmark: $label"
    (
        cd "$work_dir"
        DUCKGRES_BENCH_OUT="$out_file" \
            go test -v -count=1 \
            -run TestDuckLakeConcurrentTransactions \
            -timeout "$TEST_TIMEOUT" \
            ./tests/integration/ 2>&1 \
        | grep -E '(--- PASS|--- FAIL|FAIL|^ok|ducklake_concurrency_test.go.*:|DuckDB|DuckLake)' \
        || true
    )

    if [[ -f "$out_file" ]]; then
        echo "Results: $out_file" >&2
    else
        echo "WARNING: No results file produced for $label" >&2
    fi
}

run_in_worktree() {
    local version="$1"
    local bver
    bver="$(bindings_version "$version")"
    local branch="bench-ducklake-${version}"
    local worktree_dir="$REPO_ROOT/.worktrees/bench-${version}"

    log "Setting up worktree for duckdb-go $version"

    # Clean up any stale worktree
    if [[ -d "$worktree_dir" ]]; then
        git -C "$REPO_ROOT" worktree remove --force "$worktree_dir" 2>/dev/null || rm -rf "$worktree_dir"
    fi
    git -C "$REPO_ROOT" branch -D "$branch" 2>/dev/null || true

    # Create worktree from current HEAD
    git -C "$REPO_ROOT" worktree add -b "$branch" "$worktree_dir" HEAD

    # Swap the duckdb-go version
    (
        cd "$worktree_dir"
        go get "github.com/duckdb/duckdb-go/v2@${version}"
        go get "github.com/duckdb/duckdb-go-bindings@${bver}"
        # Platform libs follow bindings version
        go get "github.com/duckdb/duckdb-go-bindings/lib/darwin-arm64@${bver}" 2>/dev/null || true
        go get "github.com/duckdb/duckdb-go-bindings/lib/darwin-amd64@${bver}" 2>/dev/null || true
        go get "github.com/duckdb/duckdb-go-bindings/lib/linux-amd64@${bver}" 2>/dev/null || true
        go get "github.com/duckdb/duckdb-go-bindings/lib/linux-arm64@${bver}" 2>/dev/null || true
        go mod tidy
    )

    run_benchmark "$worktree_dir" "ducklake-${version}"

    # Cleanup worktree
    git -C "$REPO_ROOT" worktree remove --force "$worktree_dir" 2>/dev/null || true
    git -C "$REPO_ROOT" branch -D "$branch" 2>/dev/null || true
}

compare_results() {
    log "Comparison"

    local files=("$RESULTS_DIR"/ducklake-*.json)
    if [[ ${#files[@]} -lt 2 ]]; then
        echo "Need at least 2 result files to compare." >&2
        return 0
    fi

    if ! command -v jq &>/dev/null; then
        echo "Install jq for formatted comparison. Raw files in: $RESULTS_DIR" >&2
        return 0
    fi

    # Print header
    printf "%-45s" "Test"
    for f in "${files[@]}"; do
        local ver
        ver=$(jq -r '.ducklake_version // "unknown"' "$f")
        local ddb
        ddb=$(jq -r '.duckdb_version // "unknown"' "$f")
        printf "  %-22s" "DL $ver (DB $ddb)"
    done
    echo ""
    printf '%0.s-' $(seq 1 $((45 + ${#files[@]} * 24))); echo ""

    # Get all test names from first file
    local tests
    tests=$(jq -r '.metrics[].test' "${files[0]}")

    while IFS= read -r test; do
        printf "%-45s" "$test"
        for f in "${files[@]}"; do
            local rate
            rate=$(jq -r --arg t "$test" '.metrics[] | select(.test == $t) | .conflict_rate_pct // 0 | . * 10 | round / 10' "$f" 2>/dev/null || echo "n/a")
            local succ
            succ=$(jq -r --arg t "$test" '.metrics[] | select(.test == $t) | .successes // 0' "$f" 2>/dev/null || echo "n/a")
            local conf
            conf=$(jq -r --arg t "$test" '.metrics[] | select(.test == $t) | .conflicts // 0' "$f" 2>/dev/null || echo "n/a")
            printf "  %5s ok %4s err %4s%%" "$succ" "$conf" "$rate"
        done
        echo ""
    done <<< "$tests"

    echo ""
    echo "Full results in: $RESULTS_DIR/"
}

# --- Main ---

ensure_infra

if $CURRENT_ONLY; then
    # Get the current duckdb-go version from go.mod
    current_ver=$(grep 'duckdb-go/v2' "$REPO_ROOT/go.mod" | awk '{print $2}')
    run_benchmark "$REPO_ROOT" "ducklake-${current_ver}"
    echo ""
    echo "Results: $RESULTS_DIR/ducklake-${current_ver}.json"
    exit 0
fi

# First, benchmark the current version in-tree (no worktree needed)
current_ver=$(grep 'duckdb-go/v2' "$REPO_ROOT/go.mod" | awk '{print $2}')
log "Current version: duckdb-go $current_ver"
run_benchmark "$REPO_ROOT" "ducklake-${current_ver}"

# Then benchmark other versions in worktrees
for version in $VERSIONS; do
    if [[ "$version" == "$current_ver" ]]; then
        echo "Skipping $version (already tested as current)" >&2
        continue
    fi
    run_in_worktree "$version"
done

# Compare
echo ""
compare_results
