#!/usr/bin/env bash
#
# copy-ducklake-to-iceberg.sh
#
# Copy tables from the `ducklake` catalog into the `iceberg` (Lakekeeper REST)
# catalog of a duckgres managed warehouse.
#
# SAFE BY DEFAULT: prints the SQL it would run and exits. Pass --execute to run.
#
# ── Why it works the way it does (hard-won) ──────────────────────────────────
# The DuckDB iceberg extension + Lakekeeper, reached through duckgres, have
# sharp edges that dictate this exact sequence:
#   * `CREATE TABLE iceberg.x AS SELECT ...` creates an EMPTY table — the data
#     is NOT written. So we create the shell, then INSERT.
#   * `CREATE OR REPLACE TABLE` is NOT supported ("use separate Drop and
#     Create"). So idempotency is done with CREATE IF NOT EXISTS + DELETE.
#   * `COPY FROM DATABASE ducklake TO iceberg` (the DuckLake-native bulk copy)
#     works but (a) requires the target schema to already exist, and (b) is one
#     all-or-nothing statement — a single Iceberg-incompatible column type
#     aborts the whole thing. This script copies table-by-table instead so one
#     bad table is logged and skipped.
#   * duckgres routes each NEW connection to an arbitrary warm worker, and
#     Lakekeeper commits have visibility lag — so catalog state is inconsistent
#     ACROSS connections. We therefore run the whole CATALOG-MUTATING batch
#     (CREATE/DELETE/INSERT) in ONE psql session. Read-only PLANNING queries
#     against the stable `ducklake` source (counts, quantiles, column types)
#     don't touch iceberg catalog state, so they may run in their own sessions.
#
# ── Why we copy in CHUNKS (the big-table story) ──────────────────────────────
# A single `INSERT INTO iceberg.x SELECT * FROM ducklake.x` over a huge table
# holds the TLS connection open for minutes with ZERO bytes flowing (psql is
# blocked waiting for CommandComplete). There is no statement timeout in
# duckgres — but cloud load balancers drop idle TCP connections, so the copy
# dies partway. Splitting one table into many bounded INSERTs fixes this:
#   * each chunk finishes fast and emits a CommandComplete — that traffic keeps
#     the connection's idle timer from firing;
#   * a dropped connection only loses the in-flight chunk, not the whole table;
#   * each chunk is INDEPENDENTLY idempotent (DELETE its range, then INSERT its
#     range), so resume is at chunk granularity, not whole-table.
#
# How a table is chunked:
#   * We pick a KEY COLUMN to range over. Auto-detected per table (prefer an
#     integer/bigint id-like column, else a timestamp/date column); override
#     with --chunk-column. Tables with no usable key (or row count <= one
#     chunk) are copied in a single whole-table INSERT, exactly as before.
#   * We size the number of chunks from the row count and --chunk-rows, then
#     compute distribution-aware boundaries with approx_quantile(key, ...).
#     Quantiles (not equal-width ranges) keep chunk sizes even under skew —
#     critical for PostHog data, which is heavily weighted toward recent time.
#   * Boundaries are emitted as native-typed SQL literals so the per-chunk
#     `WHERE key >= lo AND key < hi` predicate prunes DuckLake parquet files
#     (≈ one source scan total, not one scan per chunk). A trailing
#     `key IS NULL` chunk catches NULL keys so coverage is complete.
#   * The iceberg target is unpartitioned (the DuckDB iceberg extension cannot
#     write partitioned tables); chunking is purely on the READ side.
#
# Per chunk, the proven sequence is (CREATE SCHEMA / shell emitted once/table):
#   CREATE SCHEMA IF NOT EXISTS iceberg."s";
#   CREATE TABLE  IF NOT EXISTS iceberg."s"."t" AS SELECT * FROM ducklake."s"."t" LIMIT 0;
#   DELETE FROM   iceberg."s"."t" WHERE <chunk predicate>;   -- idempotent for THIS range
#   INSERT INTO   iceberg."s"."t" SELECT * FROM ducklake."s"."t" WHERE <chunk predicate>;
#
# ── Resume (the connection WILL drop on big runs) ────────────────────────────
# Recovery is just: run the exact same command again. Two mechanisms decide
# what to skip:
#   1. ROW-COUNT PRE-CHECK (authoritative, table-level): before copying, we
#      compare each existing destination table's TOTAL row count to its ducklake
#      source. Equal => already fully copied => skip the whole table. Disable
#      with --no-verify (e.g. if counting is too slow on huge tables).
#   2. PROGRESS FILE (fast path, chunk-level): after each run we parse the log
#      and record every CHUNK whose INSERT actually returned (and every TABLE
#      whose chunks all returned). On the next run those are skipped. This is
#      what makes a half-finished huge table resume mid-table. Path printed
#      each run. Re-doing a chunk is always safe (DELETE range + INSERT range),
#      so even a stale boundary set never duplicates rows.
# --restart ignores both and re-copies everything.
#
# Connection comes from libpq env vars. duckgres routes on the TLS SNI, so
# PGHOST must be the per-org hostname (<org>.dw.<zone>):
#   export PGHOST=<org>.dw.us.postwh.com
#   export PGUSER=...  PGPASSWORD=...  PGDATABASE=...  PGSSLMODE=require
#
# Usage:
#   ./copy-ducklake-to-iceberg.sh [--execute] [--schema NAME] [--table NAME] [--limit N]
#                                 [--chunk-column NAME] [--chunk-rows N] [--max-chunks N]
#                                 [--no-chunk] [--progress FILE] [--restart] [--no-verify]
#
#   --schema NAME    only this source schema (STRONGLY recommended for a first
#                    run — `ducklake` can hold thousands of tables / lots of data)
#   --table  NAME    only this table (use with --schema)
#   --limit  N       only the first N matched tables (smoke run)
#   --chunk-column NAME  force this column as the chunk key for every table that
#                    has it with a usable (integer/timestamp/date) type; tables
#                    without it fall back to auto-detect.
#   --chunk-rows N   target rows per chunk (default 5000000). Smaller = more,
#                    shorter statements (safer against idle drops, more overhead).
#   --max-chunks N   cap chunks per table (default 512).
#   --no-chunk       disable chunking entirely (one whole-table INSERT per table,
#                    the old behavior).
#   --execute        actually run (default prints the plan + SQL).
#   --progress FILE  progress file for resume (default: derived from host+db).
#   --restart        ignore progress + count-check; re-copy everything.
#   --no-verify      skip the table-level row-count pre-check.
#
set -uo pipefail

SRC=ducklake
DST=iceberg
EXECUTE=0; SCHEMA=""; TABLE=""; LIMIT=0; PROGRESS=""; RESTART=0; VERIFY=1
CHUNK_COLUMN=""; CHUNK_ROWS=5000000; MAX_CHUNKS=512; NOCHUNK=0

while [ $# -gt 0 ]; do
  case "$1" in
    --execute) EXECUTE=1 ;;
    --schema)  SCHEMA="${2:?--schema needs a value}"; shift ;;
    --table)   TABLE="${2:?--table needs a value}"; shift ;;
    --limit)   LIMIT="${2:?--limit needs a value}"; shift ;;
    --chunk-column) CHUNK_COLUMN="${2:?--chunk-column needs a value}"; shift ;;
    --chunk-rows)   CHUNK_ROWS="${2:?--chunk-rows needs a value}"; shift ;;
    --max-chunks)   MAX_CHUNKS="${2:?--max-chunks needs a value}"; shift ;;
    --no-chunk) NOCHUNK=1 ;;
    --progress) PROGRESS="${2:?--progress needs a value}"; shift ;;
    --restart) RESTART=1 ;;
    --no-verify) VERIFY=0 ;;
    -h|--help) sed -n '2,120p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

: "${PGHOST:?set PGHOST to the org hostname, e.g. <org>.dw.us.postwh.com}"
: "${PGUSER:?set PGUSER}"; : "${PGDATABASE:?set PGDATABASE}"; : "${PGPASSWORD:?set PGPASSWORD}"
export PGSSLMODE="${PGSSLMODE:-require}"

q()  { psql -X -w -tA -F $'\t' -v ON_ERROR_STOP=1 -c "$1"; }
qid(){ printf '"%s"' "${1//\"/\"\"}"; }

echo "# $SRC -> $DST   host=$PGHOST db=$PGDATABASE user=$PGUSER"
echo "# mode: $([ $EXECUTE = 1 ] && echo EXECUTE || echo 'DRY RUN (pass --execute to run)')"
[ -n "$SCHEMA" ] && echo "# schema=$SCHEMA"; [ -n "$TABLE" ] && echo "# table=$TABLE"; [ "$LIMIT" != 0 ] && echo "# limit=$LIMIT"
if [ "$NOCHUNK" = 1 ]; then
  echo "# chunking: DISABLED (--no-chunk; one INSERT per table)"
else
  echo "# chunking: target ~$CHUNK_ROWS rows/chunk, max $MAX_CHUNKS chunks/table${CHUNK_COLUMN:+, forced key '$CHUNK_COLUMN'}"
fi

if [ "$(q "SELECT count(*) FROM duckdb_databases() WHERE database_name='$DST';")" != "1" ]; then
  echo "ERROR: catalog '$DST' not attached — reconnect (you may be on a worker activated before iceberg was enabled)." >&2
  exit 1
fi

# Progress file: keyed by the "schema.table" string (whole table done) and by
# "schema.table#<label>" (one chunk done). Both share one namespace.
sanitize(){ printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_'; }
PROGRESS="${PROGRESS:-${TMPDIR:-/tmp}/ducklake2iceberg.$(sanitize "$PGHOST").$(sanitize "$PGDATABASE").progress}"
if [ "$RESTART" = 1 ]; then : > "$PROGRESS"; echo "# --restart: cleared progress file"; fi
echo "# progress: $PROGRESS"

declare -A DONE
if [ "$RESTART" = 0 ] && [ -s "$PROGRESS" ]; then
  while IFS= read -r line; do [ -n "$line" ] && DONE["$line"]=1; done < "$PROGRESS"
  echo "# resume(progress): ${#DONE[@]} entr(y/ies) recorded copied in a prior run"
fi

where="database='$SRC'"
[ -n "$SCHEMA" ] && where="$where AND schema='$SCHEMA'"
[ -n "$TABLE" ]  && where="$where AND name='$TABLE'"
mapfile -t ROWS < <(q "SELECT schema, name FROM (SHOW ALL TABLES) WHERE $where ORDER BY schema, name;")
[ "${#ROWS[@]}" = 0 ] && { echo "No matching tables in $SRC."; exit 0; }

# Parse matched tables into parallel arrays and apply --limit up front.
declare -a SCH TBL
for row in "${ROWS[@]}"; do
  [ -z "$row" ] && continue
  SCH+=("${row%%$'\t'*}"); TBL+=("${row#*$'\t'}")
done
if [ "$LIMIT" != 0 ] && [ "$LIMIT" -lt "${#SCH[@]}" ]; then
  SCH=("${SCH[@]:0:$LIMIT}"); TBL=("${TBL[@]:0:$LIMIT}")
fi

# ── Table-level row-count pre-check (authoritative resume signal) ────────────
# For every matched table that already EXISTS in the destination, compare dst vs
# src TOTAL row counts. Equal => fully copied => skip the whole table (and all
# its chunks). Tables absent from the destination are definitely not done.
if [ "$VERIFY" = 1 ] && [ "$RESTART" = 0 ]; then
  declare -A ICE
  while IFS= read -r k; do [ -n "$k" ] && ICE["$k"]=1; done \
    < <(q "SELECT schema || '.' || name FROM (SHOW ALL TABLES) WHERE database='$DST';")
  CNT="$(mktemp -t ducklake2iceberg.XXXXXX.cnt.sql)"
  ncheck=0
  {
    echo "\\set ON_ERROR_STOP off"
    for i in "${!SCH[@]}"; do
      key="${SCH[$i]}.${TBL[$i]}"
      [ -n "${DONE["$key"]:-}" ] && continue          # already known done (progress)
      [ -z "${ICE["$key"]:-}" ] && continue           # not in dest => must copy
      sq=$(qid "${SCH[$i]}"); tq=$(qid "${TBL[$i]}")
      printf "SELECT '%s', (SELECT count(*) FROM %s.%s.%s), (SELECT count(*) FROM %s.%s.%s);\n" \
        "$key" "$SRC" "$sq" "$tq" "$DST" "$sq" "$tq"
      ncheck=$((ncheck+1))
    done
  } > "$CNT"
  if [ "$ncheck" -gt 0 ]; then
    echo "# verify: comparing total row counts for $ncheck table(s) already present in $DST ..."
    matched_done=0; seen=0
    while IFS=$'\t' read -r key src dst; do
      [ -z "$key" ] && continue
      seen=$((seen+1))
      if [ -n "$src" ] && [ "$src" = "$dst" ]; then
        DONE["$key"]=1; matched_done=$((matched_done+1))
        printf '#   [%d/%d] skip  %-55s src=%s dst=%s\n' "$seen" "$ncheck" "$key" "$src" "$dst"
      else
        printf '#   [%d/%d] COPY  %-55s src=%s dst=%s\n' "$seen" "$ncheck" "$key" "$src" "${dst:-?}"
      fi
    done < <(psql -X -w -tA -F $'\t' -f "$CNT" 2>/dev/null)
    echo "# verify: $matched_done/$ncheck already fully copied (counts match) — will skip; $((ncheck - matched_done)) need (re-)copy"
  fi
  rm -f "$CNT"
fi

# Working set = matched tables not already fully done (table-level).
declare -a WSCH WTBL
for i in "${!SCH[@]}"; do
  [ -n "${DONE["${SCH[$i]}.${TBL[$i]}"]:-}" ] && continue
  WSCH+=("${SCH[$i]}"); WTBL+=("${TBL[$i]}")
done
if [ "${#WSCH[@]}" = 0 ]; then
  echo "# nothing to do — all matched tables already copied."; exit 0
fi

# ── Plan chunks for the working set ──────────────────────────────────────────
# Per table we resolve: a chunk key column + its type class, a source row count,
# the number of chunks, and (in --execute) the quantile boundaries. All of this
# is READ-ONLY against the stable ducklake source, so it's fine outside the
# single catalog-mutating session below.
declare -A KEYCOL KEYCLASS NCHUNKS BOUNDS   # keyed by "schema.table"

if [ "$NOCHUNK" = 0 ]; then
  # 1) Column types for all ducklake columns, one query. data_type examples:
  #    BIGINT, INTEGER, HUGEINT, TIMESTAMP, TIMESTAMP WITH TIME ZONE, DATE.
  #    We pick the highest-scoring usable column per working table.
  classify(){ # $1 = UPPERCASE data_type -> echoes: int | ts | tstz | "" (unusable)
    case "$1" in
      BIGINT|INTEGER|SMALLINT|TINYINT|HUGEINT|UBIGINT|UINTEGER|USMALLINT|UTINYINT|UHUGEINT) echo int ;;
      "TIMESTAMP WITH TIME ZONE"|TIMESTAMPTZ) echo tstz ;;
      TIMESTAMP|TIMESTAMP_NS|TIMESTAMP_MS|TIMESTAMP_S|DATE) echo ts ;;
      *) echo "" ;;
    esac
  }
  score(){ # $1 = class, $2 = lowercase column name -> preference (higher wins)
    case "$1" in
      int)
        [ "$2" = id ] && { echo 100; return; }
        case "$2" in *_id|*id) echo 90 ;; *) echo 70 ;; esac ;;
      ts|tstz)
        case "$2" in timestamp|_timestamp|created_at|inserted_at|event_time|created|time) echo 60 ;; *) echo 50 ;; esac ;;
      *) echo 0 ;;
    esac
  }

  declare -A WANT
  for i in "${!WSCH[@]}"; do WANT["${WSCH[$i]}.${WTBL[$i]}"]=1; done
  declare -A BESTSCORE
  while IFS=$'\t' read -r s t col dtype; do
    [ -z "$s" ] && continue
    key="$s.$t"
    [ -z "${WANT["$key"]:-}" ] && continue
    cls="$(classify "$(printf '%s' "$dtype" | tr '[:lower:]' '[:upper:]')")"
    [ -z "$cls" ] && continue
    lcol="$(printf '%s' "$col" | tr '[:upper:]' '[:lower:]')"
    if [ -n "$CHUNK_COLUMN" ]; then
      # Forced key: only accept the named column (case-insensitive).
      [ "$lcol" = "$(printf '%s' "$CHUNK_COLUMN" | tr '[:upper:]' '[:lower:]')" ] || continue
      KEYCOL["$key"]="$col"; KEYCLASS["$key"]="$cls"; continue
    fi
    sc="$(score "$cls" "$lcol")"
    if [ "$sc" -gt "${BESTSCORE["$key"]:-0}" ]; then
      BESTSCORE["$key"]="$sc"; KEYCOL["$key"]="$col"; KEYCLASS["$key"]="$cls"
    fi
  done < <(q "SELECT table_schema, table_name, column_name, data_type
              FROM information_schema.columns
              WHERE table_catalog='$SRC'
              ORDER BY table_schema, table_name, ordinal_position;")

  # 2) Source row counts for the working set (one psql call). Used to size chunks.
  CNT2="$(mktemp -t ducklake2iceberg.XXXXXX.srccnt.sql)"
  {
    echo "\\set ON_ERROR_STOP off"
    for i in "${!WSCH[@]}"; do
      sq=$(qid "${WSCH[$i]}"); tq=$(qid "${WTBL[$i]}")
      printf "SELECT '%s.%s', count(*) FROM %s.%s.%s;\n" "${WSCH[$i]}" "${WTBL[$i]}" "$SRC" "$sq" "$tq"
    done
  } > "$CNT2"
  echo "# plan: counting source rows for ${#WSCH[@]} table(s) to size chunks ..."
  declare -A SRCCNT
  while IFS=$'\t' read -r key cnt; do
    [ -z "$key" ] && continue
    SRCCNT["$key"]="$cnt"
  done < <(psql -X -w -tA -F $'\t' -f "$CNT2" 2>/dev/null)
  rm -f "$CNT2"

  # Decide chunk count per table.
  for i in "${!WSCH[@]}"; do
    key="${WSCH[$i]}.${WTBL[$i]}"
    n="${SRCCNT["$key"]:-}"
    if [ -z "${KEYCOL["$key"]:-}" ] || [ -z "$n" ] || [ "$n" -le "$CHUNK_ROWS" ]; then
      NCHUNKS["$key"]=1                                  # whole-table single INSERT
      continue
    fi
    nc=$(( (n + CHUNK_ROWS - 1) / CHUNK_ROWS ))
    [ "$nc" -gt "$MAX_CHUNKS" ] && nc="$MAX_CHUNKS"
    [ "$nc" -lt 2 ] && nc=2
    NCHUNKS["$key"]="$nc"
  done

  # 3) Quantile boundaries (EXECUTE only — this is the one extra read pass over
  #    the source, so we skip it for dry runs and show templated SQL instead).
  if [ "$EXECUTE" = 1 ]; then
    QB="$(mktemp -t ducklake2iceberg.XXXXXX.bounds.sql)"
    nb=0
    {
      echo "\\set ON_ERROR_STOP off"
      for i in "${!WSCH[@]}"; do
        key="${WSCH[$i]}.${WTBL[$i]}"
        nc="${NCHUNKS["$key"]}"; [ "$nc" -lt 2 ] && continue
        sq=$(qid "${WSCH[$i]}"); tq=$(qid "${WTBL[$i]}"); kq=$(qid "${KEYCOL["$key"]}")
        # probs = 1/nc .. (nc-1)/nc
        probs="$(awk -v n="$nc" 'BEGIN{for(i=1;i<n;i++){printf (i>1?",":""); printf "%.8f", i/n}}')"
        case "${KEYCLASS["$key"]}" in
          int)
            printf "SELECT '%s', array_to_string(list_transform(approx_quantile(%s.%s.%s, [%s]), x -> x::HUGEINT::VARCHAR), '|');\n" \
              "$key" "$SRC" "$sq" "$tq".."$kq" "$probs" 2>/dev/null
            # NOTE: column ref is SRC.sq.tq.kq -> assembled below to avoid quoting confusion
            ;;
        esac
      done
    } > /dev/null  # placeholder; real generation below
    # Build the boundary query file explicitly (clearer than the inline attempt).
    : > "$QB"; echo "\\set ON_ERROR_STOP off" >> "$QB"
    for i in "${!WSCH[@]}"; do
      key="${WSCH[$i]}.${WTBL[$i]}"
      nc="${NCHUNKS["$key"]}"; [ "$nc" -lt 2 ] && continue
      sq=$(qid "${WSCH[$i]}"); tq=$(qid "${WTBL[$i]}"); kq=$(qid "${KEYCOL["$key"]}")
      colref="$SRC.$sq.$tq.$kq"
      probs="$(awk -v n="$nc" 'BEGIN{for(i=1;i<n;i++){printf (i>1?",":""); printf "%.8f", i/n}}')"
      case "${KEYCLASS["$key"]}" in
        int)
          printf "SELECT '%s', array_to_string(list_transform(approx_quantile(%s, [%s]), x -> x::HUGEINT::VARCHAR), '|') FROM %s.%s.%s;\n" \
            "$key" "$kq" "$probs" "$SRC" "$sq" "$tq" >> "$QB" ;;
        ts|tstz)
          printf "SELECT '%s', array_to_string(list_transform(approx_quantile(epoch_us(%s), [%s]), x -> strftime(make_timestamp(x::BIGINT), '%%Y-%%m-%%d %%H:%%M:%%S.%%f')), '|') FROM %s.%s.%s;\n" \
            "$key" "$kq" "$probs" "$SRC" "$sq" "$tq" >> "$QB" ;;
      esac
      nb=$((nb+1))
    done
    if [ "$nb" -gt 0 ]; then
      echo "# plan: computing quantile boundaries for $nb large table(s) (one source pass each) ..."
      while IFS=$'\t' read -r key blist; do
        [ -z "$key" ] && continue
        BOUNDS["$key"]="$blist"
        # Degenerate result (no boundaries) -> fall back to whole-table copy.
        [ -z "$blist" ] && NCHUNKS["$key"]=1
      done < <(psql -X -w -tA -F $'\t' -f "$QB" 2>/dev/null)
    fi
    rm -f "$QB"
  fi
fi

# Wrap a boundary value as a native-typed SQL literal for the given key class.
lit(){ # $1 = class, $2 = value
  case "$1" in
    int)  printf '%s' "$2" ;;
    ts)   printf "TIMESTAMP '%s'" "$2" ;;
    tstz) printf "TIMESTAMPTZ '%s+00'" "$2" ;;
  esac
}

# ── Build the catalog-mutating SQL (one session) ─────────────────────────────
# PLAN[key] records the chunk labels we emit for each table, so after the run we
# can tell when ALL of a table's chunks completed and mark the table done.
declare -A PLAN
SQL="$(mktemp -t ducklake2iceberg.XXXXXX.sql)"; trap 'rm -f "$SQL"' EXIT
planned=0; skipped=0
{
  echo "\\set ON_ERROR_STOP off"
  echo "\\timing on"
  last=""
  for i in "${!WSCH[@]}"; do
    schema="${WSCH[$i]}"; table="${WTBL[$i]}"; key="$schema.$table"
    sq=$(qid "$schema"); tq=$(qid "$table")
    nc="${NCHUNKS["$key"]:-1}"; cls="${KEYCLASS["$key"]:-}"; kcol="${KEYCOL["$key"]:-}"
    kq=""; [ -n "$kcol" ] && kq=$(qid "$kcol")

    # Assemble (label, predicate) pairs for this table.
    declare -a LBL PRED
    LBL=(); PRED=()
    if [ "$nc" -lt 2 ] || [ -z "${BOUNDS["$key"]:-}" ]; then
      LBL+=("1/1"); PRED+=("")                          # whole table, no predicate
    else
      IFS='|' read -r -a B <<< "${BOUNDS["$key"]}"
      # De-dup boundaries (preserve order); coverage stays complete regardless.
      declare -a UB; UB=(); prev=""
      for b in "${B[@]}"; do [ "$b" != "$prev" ] && UB+=("$b"); prev="$b"; done
      kbm1=$(( ${#UB[@]} ))                              # number of boundaries
      tot=$(( kbm1 + 1 ))
      for ((c=0;c<tot;c++)); do
        if [ "$c" = 0 ]; then
          PRED+=("$kq < $(lit "$cls" "${UB[0]}")")
        elif [ "$c" = "$kbm1" ]; then
          PRED+=("$kq >= $(lit "$cls" "${UB[$((c-1))]}")")
        else
          PRED+=("$kq >= $(lit "$cls" "${UB[$((c-1))]}") AND $kq < $(lit "$cls" "${UB[$c]}")")
        fi
        LBL+=("$((c+1))/$tot")
      done
      LBL+=("null"); PRED+=("$kq IS NULL")               # catch NULL keys
    fi
    PLAN["$key"]="${LBL[*]}"

    # Emit schema + shell once per table (only if any chunk is actually planned).
    emitted_header=0
    for c in "${!LBL[@]}"; do
      label="${LBL[$c]}"; pred="${PRED[$c]}"
      if [ -n "${DONE["$key"]:-}" ] || [ -n "${DONE["$key#$label"]:-}" ]; then
        skipped=$((skipped+1)); continue
      fi
      if [ "$emitted_header" = 0 ]; then
        [ "$schema" != "$last" ] && { echo "CREATE SCHEMA IF NOT EXISTS $DST.$sq;"; last="$schema"; }
        echo "CREATE TABLE IF NOT EXISTS $DST.$sq.$tq AS SELECT * FROM $SRC.$sq.$tq LIMIT 0;"
        emitted_header=1
      fi
      echo "\\echo >>> $key#$label"
      if [ -z "$pred" ]; then
        echo "DELETE FROM $DST.$sq.$tq;"
        echo "INSERT INTO $DST.$sq.$tq SELECT * FROM $SRC.$sq.$tq;"
      else
        echo "DELETE FROM $DST.$sq.$tq WHERE $pred;"
        echo "INSERT INTO $DST.$sq.$tq SELECT * FROM $SRC.$sq.$tq WHERE $pred;"
      fi
      planned=$((planned+1))
    done
  done
} > "$SQL"

# Per-table plan summary (key column + chunk count) so a dry run is informative.
echo "# plan summary:"
for i in "${!WSCH[@]}"; do
  key="${WSCH[$i]}.${WTBL[$i]}"
  nc="${NCHUNKS["$key"]:-1}"; kcol="${KEYCOL["$key"]:-}"
  if [ "$nc" -lt 2 ]; then
    reason="single INSERT"
    [ -z "$kcol" ] && reason="$reason (no usable chunk key)"
    [ -n "$kcol" ] && reason="$reason (rows<=chunk-rows)"
    [ "$NOCHUNK" = 1 ] && reason="single INSERT (--no-chunk)"
    printf '#   %-55s %s\n' "$key" "$reason"
  else
    printf '#   %-55s %s chunks on %s (%s)\n' "$key" "$nc" "$kcol" "${KEYCLASS["$key"]}"
  fi
done

echo "# $planned chunk-statement(s) to run this run ($skipped already done)"
[ "$planned" = 0 ] && { echo "# nothing to do — all matched chunks already copied."; exit 0; }

if [ $EXECUTE = 0 ]; then
  echo "# --- generated SQL (dry run; boundaries computed at --execute time) ---"
  grep -vE '^\\' "$SQL"
  exit 0
fi

LOG="$(mktemp -t ducklake2iceberg.XXXXXX.log)"
psql -X -w -f "$SQL" 2>&1 | tee "$LOG"; rc=${PIPESTATUS[0]}

# A chunk is complete only when its INSERT returned. Record completed chunk keys.
awk '/^>>> /{cur=substr($0,5)} /^INSERT /{if(cur!=""){print cur; cur=""}}' "$LOG" >> "$PROGRESS"
# Then mark a TABLE done when every label in its plan has completed.
{
  for key in "${!PLAN[@]}"; do
    alldone=1
    for label in ${PLAN["$key"]}; do
      grep -qxF "$key#$label" "$PROGRESS" || { alldone=0; break; }
    done
    [ "$alldone" = 1 ] && echo "$key"
  done
} >> "$PROGRESS"
sort -u -o "$PROGRESS" "$PROGRESS"

completed=$(awk '/^>>> /{cur=substr($0,5)} /^INSERT /{if(cur!=""){c++; cur=""}} END{print c+0}' "$LOG")
errs=$(grep -c '^ERROR' "$LOG" || true)
remaining=$((planned - completed))

echo
echo "# done: $planned chunk-statement(s) planned, $completed completed, $errs error(s), $remaining not reached"
if [ "$errs" -gt 0 ]; then
  echo "# chunks that errored (NOT marked done — will retry on next run):"
  grep -B1 '^ERROR' "$LOG" | grep '^>>> ' | sed 's/^/#   /'
fi
if [ "$rc" != 0 ]; then
  echo "# psql exited $rc — the connection likely dropped mid-run." >&2
fi
if [ "$rc" != 0 ] || [ "$remaining" -gt 0 ] || [ "$errs" -gt 0 ]; then
  echo "# INCOMPLETE — re-run the SAME command to resume from where it stopped." >&2
  rm -f "$LOG"; exit 1
fi
rm -f "$LOG"
echo "# all $completed chunk(s) copied successfully."
