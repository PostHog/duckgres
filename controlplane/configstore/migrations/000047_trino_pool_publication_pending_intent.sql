-- +goose Up
-- Which request a tenant's open occurrence stands for, while its outcome is
-- unknown.
--
-- A publication or revocation whose response was lost is not finished: it may
-- still be executing at the Gateway and commit whenever it gets there. If the
-- driver moved on to the next desired intent under a new step identity, the two
-- would be unordered - same controller, same epoch, different steps - so the
-- older one could commit last and leave the tenant bound to a set nobody wants,
-- or revoked after being re-enabled, while duckgres had checkpointed the newer
-- intent and would never issue it again.
--
-- Recording WHICH kind of request the occurrence stands for is what lets the
-- next pass reissue that exact step identity until the Gateway gives a definite
-- answer. Once it has, a late duplicate carries a journaled identity and
-- applies nothing.
--
-- '' means the tenant has no request in flight: its occurrence is spent and the
-- next desired change takes a new one.
--
-- pending_payload is the request itself, stored so the reissue is BYTE-IDENTICAL
-- to the original rather than a new body sent under the old identity. A tenant
-- whose last login disappears while its publication is in flight still has
-- something to replay, and a changed-intent refusal stays what it should be - an
-- anomaly - instead of becoming the ordinary way an occurrence is closed.
--
-- It holds principal IDENTIFIERS and the revision that names them, and nothing
-- else: no password, no hash, no credential of any kind ever enters this column.
ALTER TABLE duckgres_trino_pool_publications
    ADD COLUMN IF NOT EXISTS pending_intent TEXT NOT NULL DEFAULT ''
        CHECK (pending_intent IN ('', 'principals', 'revoke')),
    ADD COLUMN IF NOT EXISTS pending_payload JSONB NOT NULL DEFAULT '{}'
        CHECK (jsonb_typeof(pending_payload) = 'object');

-- +goose Down
ALTER TABLE duckgres_trino_pool_publications
    DROP COLUMN IF EXISTS pending_payload,
    DROP COLUMN IF EXISTS pending_intent;
