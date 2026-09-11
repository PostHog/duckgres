#!/usr/bin/env bash
# Exercise the shipped client and the integration stack's S3/admin contract.
# Setup: docker compose -f tests/integration/docker-compose.yml up -d --wait minio
# Then: docker compose -f tests/integration/docker-compose.yml run --rm --no-deps minio-init
# Run against initialized integration infrastructure; SILO_TEST_CONTAINER selects
# an isolated stack instead of the default duckgres-test-minio container.
set -euo pipefail

container=${SILO_TEST_CONTAINER:-duckgres-test-minio}
root_user=${SILO_TEST_ROOT_USER:-minioadmin}
root_password=${SILO_TEST_ROOT_PASSWORD:-minioadmin}
reader_user=${SILO_TEST_READER_USER:-trino-reader}
reader_password=${SILO_TEST_READER_PASSWORD:-trino-reader}
fixture="silo-contract-$(date +%s)-$$"
config="/tmp/$fixture"
object="ducklake/data/$fixture.txt"
outside_object="ducklake/$fixture.txt"
password="$fixture-secret"
payload="Silo storage contract $fixture"

# This intentionally fails on an image without the maintained bundled client.
docker exec "$container" mcli --version
docker exec "$container" mkdir -p "$config"
mcli() {
    docker exec -i "$container" mcli --config-dir "$config" "$@"
}
cleanup() {
    mcli rm "root/$object" "root/$outside_object" >/dev/null 2>&1 || true
    mcli admin user remove root "$fixture" >/dev/null 2>&1 || true
    docker exec "$container" rm -rf "$config" >/dev/null 2>&1 || true
}
trap cleanup EXIT

expect_denied() {
    local output
    if output=$("$@" 2>&1); then
        printf 'FAIL: unauthorized operation succeeded: %s\n' "$*" >&2
        exit 1
    fi
    # A transport failure or missing executable is not evidence of enforcement.
    if ! printf '%s\n' "$output" | grep -Eiq 'Access Denied|AccessDenied|Insufficient permissions|InvalidAccessKeyId|access key.*(does not exist|disabled)|account.*disabled'; then
        printf 'FAIL: expected an authorization error, got: %s\n' "$output" >&2
        exit 1
    fi
}

mcli alias set root http://127.0.0.1:9000 "$root_user" "$root_password"
mcli ready root
# Require the initialized bucket; never make a missing initialization pass.
mcli stat root/ducklake
mcli anonymous get root/ducklake | grep -Eq '`private`|private'
mcli admin user add root "$fixture" "$password"
mcli admin policy attach root readwrite --user "$fixture"
mcli alias set writer http://127.0.0.1:9000 "$fixture" "$password"
printf '%s' "$payload" | mcli pipe "writer/$object"
test "$(mcli cat "writer/$object")" = "$payload"

mcli admin user disable root "$fixture"
expect_denied mcli cat "writer/$object"
mcli admin user enable root "$fixture"
test "$(mcli cat "writer/$object")" = "$payload"
mcli admin user remove root "$fixture"
expect_denied mcli cat "writer/$object"

# Exercise the exact reader configured for Trino, including the data/ boundary.
mcli alias set reader http://127.0.0.1:9000 "$reader_user" "$reader_password"
test "$(mcli cat "reader/$object")" = "$payload"
mcli ls "reader/ducklake/data/" >/dev/null
expect_denied mcli cp "root/$object" "reader/$object"
printf '%s' "$payload" | mcli pipe "root/$outside_object"
expect_denied mcli cat "reader/$outside_object"
test "$(mcli cat "root/$object")" = "$payload"
printf 'PASS: Silo bucket privacy, read/write, credential revocation, and Trino reader policy\n'
