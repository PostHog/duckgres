# PostgreSQL builtin-compatibility — implementation status

This branch implements the fixable findings from the audit in
[`pg-builtin-compat-gaps.md`](./pg-builtin-compat-gaps.md): PostgreSQL builtins
that real clients/ORMs/metadata queries call but DuckDB either lacks or
implements with divergent (sometimes silently-wrong) semantics. Each fix follows
the `array_lower` (PR #705) pattern — a DuckDB macro/transform wired through
`server/catalog.go`, the transpiler `Classify` list, and both `pgcatalog.go`
qualification maps — and is gated by TDD against DuckDB v1.5.2 plus an
end-to-end assertion in `tests/e2e-mw-dev/harness.sh` (`pg_compat_functions`,
run on both cnpg + ext backends in DuckLake mode).

## Implemented (48 functions/operators + 2 transpiler fixes)

**Batch A — clean scalar macros (24):** `set_config`, `uuid_generate_v4`,
`statement_timestamp`, `pg_get_function_arguments`, `pg_get_function_result`,
`pg_get_function_identity_arguments`, `pg_get_triggerdef`, `pg_jit_available`,
`row_security_active`, `pg_collation_for`, `pg_input_is_valid`, `to_regclass`,
`to_regtype`, `to_regproc`, `jsonb_pretty`, `to_ascii`, `convert_from`,
`width_bucket`, `scale`, `min_scale`, `masklen`, `hostmask`, `set_masklen`,
`inet_same_family`.

**Batch B — array + interval macros (11):** `array_positions`, `array_replace`,
`array_fill`, `trim_array`, `array_dims`, `date_bin`, `make_interval`,
`justify_hours`, `justify_days`, `justify_interval`, `overlaps` (OVERLAPS keyword).

**Batch C — builtin-shadowing macros (3):** `decode`, `encode` (2-arg PG bytea
semantics; the broken identity mappings in `functions.go` were removed),
`inet_server_addr`.

**Batch D — set-returning table macros (9):** `json_array_elements`,
`jsonb_array_elements`, `json_array_elements_text`, `jsonb_each`,
`json_each_text`, `pg_options_to_table`, `aclexplode`, `pg_get_keywords`,
`pg_identify_object`.

**Batch F — operator rewrites (2):** `@>` (jsonb containment → `json_contains`,
array `@>` left native), `#>>` (literal text[] path → `json_extract_string`).

**Batch G — type shim (1):** PG curly-brace array literal casts
(`'{1,2,3}'::int[]` → `ARRAY[...]::int[]`) via a sound PG-array-literal parser
(handles quoted/embedded-comma/NULL elements; bails on multi-dimensional).

**Transpiler correctness fixes surfaced during integration / live-DuckLake QA:**
- `Classify` now flags `@>` / `#>>` for `OperatorTransform` (previously these
  rewrote only when a query *also* contained `->`/`~`/`||` — a latent bug).
- `TypeCastTransform.walkAndTransform` now recurses into `A_Indirection`
  (subscript) nodes, so casts inside `(expr)[n]` are transformed.
- `TypeCastTransform.walkSelectStmt` now recurses into `ValuesLists`, so array
  casts inside `INSERT … VALUES ('{1,2}'::int[])` are transformed.
- The `inet` helpers (`masklen`/`hostmask`/`set_masklen`/`inet_same_family`) are
  text-based: the transpiler maps PG `inet` → `text`, so the DuckDB INET type /
  `family()` / `host()` are not available through the pipeline.

**Known limitation:** an array literal with **no** explicit cast in an `INSERT …
VALUES` row (e.g. `INSERT INTO t(arr) VALUES ('{a,b}')`) is not rewritten — the
target column type isn't known at transpile time. Use `'{a,b}'::text[]` or
`ARRAY['a','b']`. The explicit-cast forms are handled.

## Deferred to a follow-up PR (Batch E — 7 Go `functions.go` transforms)

Each is a hand-written `pg_query` AST rewrite with documented PG-divergence
corners that warrant focused review; verified specs + test vectors are captured
in the audit. Not macro-able (self-recursion / variadic / type-directed):

- **`format`** (`%I`/`%L`/`%s` printf) — highest value (silent-corruption fix);
  needs a literal-template parser emitting `quote_ident`/`quote_literal`/cast.
- **`substr`** (negative-offset window), **`substring`** (`FROM` regex / `FROM..FOR`),
  **`overlay`**, **`date_trunc`** (3-arg timezone form), **`cardinality`**
  (multi-dim total-element count), **`isfinite`** (interval → true).

The misleading identity mappings for `substr`/`substring`/`width_bucket` remain
in `functions.go` for now (width_bucket is already correctly shadowed by the
Batch A macro; substr/substring are addressed in the Batch E follow-up).

## Infeasible (8 — documented, intentionally not shipped)

A faithful emulation is impossible with DuckDB primitives, and a lossy shim would
return **silently-wrong data** — worse than the current hard error. Per the
audit's guiding principle, these error honestly rather than lie:

- **`jsonb_set` / `jsonb_insert` / `jsonb_strip_nulls`** — PG operates recursively
  on nested paths; a top-level-only shim silently diverges on nested input.
- **`json_populate_record`** — type-directed (output columns derive from a target
  rowtype); not expressible as a name alias.
- **`jsonb_path_query`** — PG jsonpath is a distinct language from DuckDB's JSON
  path syntax.
- **`parse_ident`** — quote-aware/quote-stripping/case-folding; a `string_split`
  shim is unsound.
- **`daterange` / `numrange`** — PG range types are a full type+operator subsystem
  absent from DuckDB; the constructor is macro-able but `&&`/`@>`/`<@`/`=` range
  operators are not.
