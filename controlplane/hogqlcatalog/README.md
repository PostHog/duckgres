# HogQL semantic catalog transport

This package serves the HogQL-only semantic overlay consumed by Trino. DuckLake
remains authoritative for physical schemas, tables, columns, and their types.
Schema version 2 manifests add the declarative metadata needed to lower HogQL:

- logical tables, physical fields, properties, and relationship join keys;
- optional generic property lookup recipes with typed source and key arguments;
- optional relationship predicates with explicit source and target field scopes;
- expression fields built from typed field, literal, function, operator, and
  cast recipes;
- lazy tables with bounded relationship paths and typed projection recipes;
- virtual tables that project another declared relation;
- saved-query aliases with opaque provenance IDs, typed executable relation
  targets, and result fields, plus physical materialized-view references;
- actions and cohorts represented by a declarative predicate or a typed
  relation-membership recipe;
- function signatures and the capabilities supported by their Trino lowering;
- modifier defaults and, where applicable, their Trino session-property name.

The contract never carries SQL fragments or code. Physical names use structured
identifiers, expression recipes use a closed discriminated union, and literal
payloads have an explicit type signature and encoding. Saved-query IDs are
opaque provenance; their required logical, virtual, or materialized target is
the complete locally executable version 2 form. Query text is not part of this
transport, so reads never require a hot-path metadata RPC.

## Physical metadata assembly

`PhysicalMetadataProvider` is the boundary for an authoritative DuckLake
metadata reader. `BuildPhysicalSnapshot` translates one complete provider result
into a normalized version 2 snapshot: tables and columns are ordered
deterministically, exact structured identifiers are preserved, and each column
carries its provider-supplied Trino type signature, nullability, and star
visibility. HogQL logical types are derived from the Trino type family. Types
that do not have a HogQL family remain `UNKNOWN` without changing the exact
Trino signature. Nullability and star visibility use explicit dispositions, so
an omitted provider value cannot silently become `NOT NULL` or hidden.

The provider result is all-or-nothing. Missing inventories, a mismatched
catalog, noncanonical identifiers, duplicate tables, columns, or ordinals, and
empty or noncanonical type-signature text fail before a snapshot can be
published. Full Trino type syntax is parsed by the Trino consumer; this boundary
preserves the provider's exact signature rather than approximating it. This
package does not synthesize tables when DuckLake metadata is unavailable. The
Trino catalog HTTP client implements this provider by calling the authenticated,
versioned coordinator endpoint with the provisioner's existing rotated admin
Basic credential. The adapter strictly decodes the response, preserves original
connector ordinals after visibility filtering, and rejects protocol, schema,
catalog, or visibility inconsistencies before translation.

The Trino provisioner refreshes both newly created and existing tenant catalogs.
New catalogs request an immediate refresh. Existing catalogs use a persisted
five-minute refresh schedule, so the ten-second reconcile loop does not scan
every connector inventory on every tick. A failed fetch or publication retries
after 30 seconds. An identical physical inventory keeps the current generation.

Each refresh first acquires a one-minute per-catalog lease in Postgres. The
lease transaction commits before the HTTP request starts. Its opaque token and
monotonic epoch fence publication, so an expired older fetch cannot publish
after a newer lease holder. Publication verifies the unexpired token while
holding the same short per-catalog advisory transaction lock used for semantic
publication. No database transaction, Kubernetes lock, or catalog-client lock
is held across the Trino request. The Duckgres generation records publication
order. It does not claim that Trino supplied a monotonically versioned schema
snapshot.

Physical refresh matches logical tables by their structured physical qualified
name and fields by their structured physical column name. It replaces exact
connector types, logical type families, nullability, star visibility, and field
order. It preserves logical table and field names, properties, relationships,
and every root semantic definition. Multiple logical tables may project the
same physical table; each projection is preserved and refreshed. New physical
tables and columns receive default logical names. Removed physical members,
name collisions, and broken semantic references fail validation before
publication.

Fetch, merge, validation, and storage failures leave the last good generation
available. When no generation exists yet and another replica owns the refresh
lease, the tenant remains provisioning until that lease publishes. To recover,
fix the reported Trino metadata or catalog error and allow the scheduled retry
to run. Do not edit an existing manifest generation or the lease row manually.

## API

The control plane exposes one internal-token-authenticated compatibility resource:

- `PUT /v1/hogql/compatibility/semantic-catalog` publishes one immutable generation.
- `GET /v1/hogql/compatibility/semantic-catalog?protocolVersion=1&languageVersion=:version&catalog=:catalog&catalogDelimited=:boolean`
  reads the highest published generation.
- Adding `&generation=:generation` to that `GET` reads an exact generation.

`GET` accepts either the rotated read-only token or the rotated admin token.
`PUT` accepts only the admin token. Both current and fallback rotation values
are valid during a rotation window, and invalid or missing credentials fail
before the catalog handler runs. Trino should mount a read-only token and read
it from `hogql.semantic-catalog.authentication-token-file`; publishers retain
the separate admin credential.

Every manifest must include `protocolVersion: 1`, `schemaVersion: 2`, a supported language
version, a positive monotonically increasing generation, and all fields in the
typed JSON contract. Unknown fields, unknown references, executable SQL, missing
generations, and catalog or version mismatches fail closed.

The original six semantic-metadata arrays are required, including when empty.
The additive `lazyTables`, `actions`, and `cohorts` arrays may be omitted by an
older version 2 publisher. Optional
recipe payloads are omitted unless selected by `kind`; exactly one payload must
match that discriminator. Definition names are case-insensitively unique in
their namespace, and logical, virtual, saved-query, and materialized-view names
share one relation namespace.

A property may add `keyTypeSignature`, `valueTypeSignature`, and a
`lookupRecipe`. The recipe must reference both `PROPERTY_SOURCE` and
`PROPERTY_KEY`. These placeholders let the compiler insert a resolved physical
storage field and the query's typed property key without interpolating SQL.
Lookup recipes use the same closed literal, function, operator, and cast forms
as expression fields. `SUBSCRIPT` is the stock two-argument representation for
map access. `JSON_OBJECT_LOOKUP` accepts a VARCHAR JSON object and VARCHAR key;
the consumer lowers it through `json_parse`, a cast to `map(varchar, json)`, and
a subscript without constructing a JSON path. The PostHog v0 property recipe
declares a VARCHAR result, so the consumer's outer value cast exposes HogQL's
scalar string semantics. A `PROPERTY_LOOKUP` expression names a declared
property and carries one typed key expression.

Physical refresh applies the built-in PostHog v0 profile when exactly one
logical projection of a physical `events` table and one of a physical `persons`
table share a schema and expose compatible `properties`, `person_id`, and `id`
columns. The profile adds event and person scalar-property definitions and the
many-to-one `events.person` relationship. It leaves existing semantic members
unchanged and stays inert for missing, ambiguous, or incompatible shapes.
The profile publishes no actions, cohorts, saved queries, modifiers, lazy-table
definitions, or function declarations. The Trino compiler owns the frozen v0
function registry, while this manifest supplies only catalog-derived table,
property, and relationship semantics.

Relationship join keys remain required and describe equijoins. An optional
`joinPredicate` adds a typed condition when the equijoin does not capture the
complete relationship. Join predicates use `SCOPED_FIELD_REFERENCE` with an
explicit `SOURCE` or `TARGET` side, so a consumer never infers which relation a
field belongs to.

A lazy table belongs to one logical table, follows a non-empty relationship
path, and declares its visible output through typed projection recipes. This
metadata gives the compiler enough information to select requested projections
before it emits the joins. An action or cohort has exactly one representation:
a predicate on its declared logical table, or a relation membership with
explicit source and target fields. Relation membership may target a logical,
virtual, saved-query, or materialized relation already present in the snapshot.

Validation resolves expression-field and virtual-table references before a
snapshot is published. Dependency cycles fail closed. All expression, property,
join, lazy-projection, action, and cohort recipes share a limit of 64 levels and
4,096 nodes per snapshot. Virtual relation chains and lazy relationship paths
are limited to 64 levels, and a snapshot may declare at most 10,000 semantic
definitions. Relationship cycles remain valid because bidirectional logical
relationships are normal data-model structure rather than expansion recipes.
Version 2 expression fields may reference only fields on their owning logical
table. A future cross-table recipe must carry an explicit relationship path;
consumers must never infer a join.

Function declarations state whether lowering uses a stock Trino function, a
registered UDF, or a compiler rewrite. Stock and UDF declarations omit
`rewrite` and provide a non-empty structured `trinoName`. Rewrite declarations
provide an empty `trinoName` and one identifier from the closed contract below.
All rewrites are deterministic and disable distinct, ordering, and filter
traits. Aggregate rewrites may enable window invocation; scalar rewrites may
not. Only `IS_NULL` and `IS_NOT_NULL` require a `boolean` return type.

Aggregate rewrite signatures are:

- one argument: `COUNT_IF`, `COUNT_DISTINCT`, `GROUP_UNIQ_ARRAY`, `UNIQ_EXACT`;
- two arguments: `ANY_IF`, `AVG_IF`, `GROUP_UNIQ_ARRAY_IF`, `MAX_IF`,
  `MEDIAN_IF`, `MIN_IF`, `QUANTILE`, `QUANTILE_EXACT`, `SUM_IF`,
  `UNIQ_EXACT_IF`, `UNIQ_IF`;
- three arguments: `ARG_MAX_IF`, `ARG_MIN_IF`, `QUANTILE_IF`;
- two or three arguments: `GROUP_ARRAY_IF`.

Scalar rewrite signatures are:

- no arguments: `TODAY`;
- one argument: `ARRAY_ENUMERATE`, `ARRAY_SUM`, `ASSUME_NOT_NULL`,
  `CAST_BIGINT`, `CAST_DATE`, `CAST_DOUBLE`, `CAST_SMALLINT`, `CAST_UUID`,
  `CAST_VARCHAR`, `DATE_TRUNC_DAY`, `DATE_TRUNC_HOUR`, `DATE_TRUNC_MONTH`,
  `DATE_TRUNC_WEEK`, `EMPTY`, `FLOAT_OR_ZERO`, `INTERVAL_DAY`,
  `INTERVAL_MONTH`, `INT_OR_ZERO`, `IS_NULL`, `IS_NOT_NULL`, `MD5`, `NOT`,
  `NOT_EMPTY`, `PARSE_TIMESTAMP`, `START_WEEK`, `TO_JSON_STRING`,
  `TO_UNIX_TIMESTAMP`;
- two arguments: `ADD_DAYS`, `ADD_MONTHS`, `ARRAY_ELEMENT`, `ARRAY_FILTER`,
  `ARRAY_FIRST`, `ARRAY_MAP`, `DATE_PART`, `DECIMAL_CAST`, `DIVIDE_DECIMAL`,
  `EQUALS`, `FLOAT_OR_DEFAULT`, `GREATER`, `GREATER_OR_EQUAL`, `HAS`,
  `IN_ARRAY`, `INT_DIV`, `JSON_EXTRACT_TYPED`, `JSON_HAS`,
  `JSON_KEYS_AND_VALUES`, `JSON_VALUE`, `LESS_OR_EQUAL`, `LIKE`, `MINUS`,
  `MULTIPLY`, `MULTIPLY_DECIMAL`, `NOT_EQUALS`, `PLUS`, `REGEX_EXTRACT`,
  `REGEX_EXTRACT_ALL`, `SPLIT_CHAR`, `SPLIT_STRING`, `SUBTRACT_MONTHS`,
  `SUBTRACT_YEARS`, `SURVEY_RESPONSE`, `TUPLE_ELEMENT`;
- three arguments: `ARRAY_SLICE`, `REGEX_REPLACE_ALL`, `REGEX_REPLACE_ONE`;
- one or two arguments: `ARRAY_SORT`, `CAST_TIMESTAMP`, `RANGE`;
- two or three arguments: `DATE_ADD`;
- variadic with at least one argument: `TUPLE`;
- variadic with at least two arguments: `JSON_EXTRACT_BOOL`,
  `JSON_EXTRACT_FLOAT`, `JSON_EXTRACT_INT`, `JSON_EXTRACT_RAW`,
  `JSON_EXTRACT_UINT`;
- either one fixed argument or variadic with at least two arguments:
  `JSON_EXTRACT_ARRAY_RAW`, `JSON_EXTRACT_KEYS`, `JSON_EXTRACT_STRING`,
  `JSON_KEYS_AND_VALUES_RAW`, `JSON_LENGTH`;
- either two fixed arguments or variadic with at least two arguments: `AND`;
- variadic with at least three arguments: `MULTI_IF`. Invocations must contain
  condition/result pairs followed by one default value, so their actual arity
  is odd.

For a variadic manifest signature, `argumentTypes` contains one more entry than
the minimum accepted invocation arity. Missing, unknown, or
implementation-incompatible rewrite values, kinds, signatures, and traits fail
validation. Modifier behavior is one of compiler handling, a named Trino
session property, a safe no-op, or explicitly unsupported. A consumer applies
modifier defaults only after pinning this snapshot. An explicit request value
replaces the declared default and must have the same canonical type signature.
Explicit modifiers therefore require a snapshot even when the query otherwise
uses no semantic relation. Compiler modifiers require a compiler handler,
session-property modifiers use a separate typed override channel, safe no-ops
are validated and ignored, and unsupported modifiers fail when explicitly
requested. An omitted unsupported modifier is inert. Consumers must still fail
closed when a requested capability is not declared and must not interpolate
modifier values into SQL.

Published generations are persisted in the config-store Postgres database.
Rows are append-only and the latest snapshot is selected by generation, so all
control-plane replicas and restarted processes observe the same history. An
identical retry of the latest generation is idempotent; changing an existing
generation or publishing a lower generation is rejected.

Schema version 2 is intentionally incompatible with schema version 1. Publishers
must switch only after every reader accepts version 2; mixed-version operation
requires a separate compatibility endpoint or dual publication outside this
contract.

To roll back semantic behavior, publish the prior semantic content under a new,
higher generation. Do not update or delete a published generation in place.
