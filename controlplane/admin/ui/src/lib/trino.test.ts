import { describe, expect, it } from "vitest";
import {
  groupTrinoQueries,
  isWaitingTrinoQuery,
  sortTrinoQueries,
  trinoSqlSummary,
  trinoStateBucket,
  TRINO_DEGRADED_FAILURE_RATIO,
  TRINO_LONG_RUNNING_MS,
  isActiveTrinoQuery,
  summarizeTrinoNodes,
  summarizeTrinoQueries,
  trinoOrgsNeedingAttention,
  trinoQueryFlag,
  trinoScanEfficiency,
  trinoStateVariant,
  trinoUnavailableMessage,
  trinoUnavailableReason,
} from "./trino";
import type {
  TrinoNode,
  TrinoOrgStatus,
  TrinoQuery,
  TrinoStatus,
} from "@/types/api";

function query(over: Partial<TrinoQuery> = {}): TrinoQuery {
  return {
    query_id: "q1",
    state: "RUNNING",
    org: "org-a",
    principal: "db_a",
    source: "trino-cli",
    resource_group: "global.tier_free",
    query: "SELECT 1",
    elapsed_ms: 1_000,
    queued_ms: 0,
    cpu_ms: 0,
    physical_input_bytes: 0,
    internal_network_bytes: 0,
    peak_memory_bytes: 0,
    spilled_bytes: 0,
    processed_input_rows: 0,
    total_drivers: 0,
    queued_drivers: 0,
    running_drivers: 0,
    completed_drivers: 0,
    fully_blocked: false,
    progress_percentage: null,
    ...over,
  };
}

function node(over: Partial<TrinoNode> = {}): TrinoNode {
  return {
    uri: "http://10.0.0.1:8080",
    age_ms: 0,
    recent_failures: 0,
    recent_successes: 100,
    recent_failure_ratio: 0,
    failed: false,
    ...over,
  };
}

function status(over: Partial<TrinoStatus> = {}): TrinoStatus {
  return {
    cell: { id: "legacy" },
    available: true,
    queries_by_state: {},
    blocked_queries: 0,
    node_stats: true,
    nodes: 0,
    failed_nodes: 0,
    orgs_by_state: {},
    total_orgs: 0,
    ...over,
  };
}

function org(over: Partial<TrinoOrgStatus> = {}): TrinoOrgStatus {
  return {
    org: "org-a",
    principal: "db_a",
    catalog: "org_db_a",
    tier: "free",
    cell: "legacy",
    state: "ready",
    running_queries: 0,
    queued_queries: 0,
    ...over,
  };
}

describe("isActiveTrinoQuery", () => {
  it("counts every one of Trino's seven non-terminal states", () => {
    // Trino has nine states and only two are terminal. An allowlist of the
    // interesting-looking ones silently drops a query stuck in PLANNING or
    // WAITING_FOR_RESOURCES — which, on a DuckLake-backed cell where
    // planning talks to a per-tenant Postgres, is exactly what an operator
    // opens this page to find.
    for (const state of [
      "QUEUED",
      "WAITING_FOR_RESOURCES",
      "DISPATCHING",
      "PLANNING",
      "STARTING",
      "RUNNING",
      "FINISHING",
    ]) {
      expect(isActiveTrinoQuery(query({ state }))).toBe(true);
    }
    for (const state of ["FINISHED", "FAILED"]) {
      expect(isActiveTrinoQuery(query({ state }))).toBe(false);
    }
  });
});

describe("trinoQueryFlag", () => {
  it("returns null for an ordinary running query", () => {
    expect(trinoQueryFlag(query())).toBeNull();
  });

  it("ranks a failure above everything else", () => {
    expect(
      trinoQueryFlag(query({ state: "FAILED", fully_blocked: true })),
    ).toBe("failed");
  });

  it("ranks fully-blocked above long-running", () => {
    // A blocked query is a cell-level problem (metadata store, S3) wearing
    // one query's clothes, so it must not be mislabelled as merely slow.
    const q = query({
      fully_blocked: true,
      elapsed_ms: TRINO_LONG_RUNNING_MS * 10,
    });
    expect(trinoQueryFlag(q)).toBe("blocked");
  });

  it("flags a queued query regardless of its elapsed time", () => {
    expect(trinoQueryFlag(query({ state: "QUEUED", elapsed_ms: 10 }))).toBe(
      "queued",
    );
  });

  it("flags a long-running query only once it crosses the threshold", () => {
    expect(
      trinoQueryFlag(query({ elapsed_ms: TRINO_LONG_RUNNING_MS - 1 })),
    ).toBeNull();
    expect(trinoQueryFlag(query({ elapsed_ms: TRINO_LONG_RUNNING_MS }))).toBe(
      "long_running",
    );
  });

  it("never flags a finished query as long-running", () => {
    // Otherwise every completed heavy query lights up the list forever.
    expect(
      trinoQueryFlag(
        query({ state: "FINISHED", elapsed_ms: TRINO_LONG_RUNNING_MS * 10 }),
      ),
    ).toBeNull();
  });
});

describe("summarizeTrinoQueries", () => {
  it("counts states and sums the cell's current draw", () => {
    const s = summarizeTrinoQueries([
      query({
        state: "RUNNING",
        physical_input_bytes: 100,
        cpu_ms: 10,
        elapsed_ms: 1_000,
      }),
      query({
        state: "RUNNING",
        fully_blocked: true,
        physical_input_bytes: 200,
        cpu_ms: 20,
        elapsed_ms: 5_000,
      }),
      query({ state: "QUEUED", elapsed_ms: 50 }),
      query({ state: "FAILED", physical_input_bytes: 5 }),
      query({
        state: "FINISHED",
        physical_input_bytes: 1_000,
        elapsed_ms: 900_000,
      }),
    ]);
    expect(s.total).toBe(5);
    expect(s.running).toBe(2);
    expect(s.queued).toBe(1);
    expect(s.failed).toBe(1);
    expect(s.blocked).toBe(1);
    expect(s.scannedBytes).toBe(1_305);
    expect(s.cpuMs).toBe(30);
  });

  it("measures 'longest' over in-flight queries only", () => {
    // A finished query's elapsed time says nothing about current load; if it
    // counted, the headline would be pinned by whatever ran overnight.
    const s = summarizeTrinoQueries([
      query({ state: "FINISHED", elapsed_ms: 900_000 }),
      query({ state: "RUNNING", elapsed_ms: 3_000 }),
    ]);
    expect(s.longestMs).toBe(3_000);
  });

  it("handles an empty cell", () => {
    const s = summarizeTrinoQueries([]);
    expect(s).toEqual({
      total: 0,
      running: 0,
      queued: 0,
      blocked: 0,
      failed: 0,
      scannedBytes: 0,
      cpuMs: 0,
      longestMs: 0,
    });
  });
});

describe("summarizeTrinoNodes", () => {
  it("separates failed from merely degraded", () => {
    // Degraded is the early warning; failed is what the coordinator has
    // already acted on. A node cannot be both.
    const h = summarizeTrinoNodes([
      node(),
      node({ uri: "b", recent_failure_ratio: TRINO_DEGRADED_FAILURE_RATIO }),
      node({ uri: "c", failed: true, recent_failure_ratio: 1 }),
    ]);
    expect(h.total).toBe(3);
    expect(h.failed).toBe(1);
    expect(h.degraded).toBe(1);
    expect(h.worstFailureRatio).toBe(1);
  });

  it("does not double-count a failed node as degraded", () => {
    const h = summarizeTrinoNodes([
      node({ failed: true, recent_failure_ratio: 0.9 }),
    ]);
    expect(h.failed).toBe(1);
    expect(h.degraded).toBe(0);
  });
});

describe("trinoUnavailableReason", () => {
  it("is null for a healthy cell", () => {
    expect(trinoUnavailableReason(status())).toBeNull();
  });

  it("distinguishes an unconfigured deployment from a broken one", () => {
    // Different fix: one is "this cluster has no Trino", the other is an
    // incident. Collapsing them sends an operator to the wrong system.
    const none = status({
      cell: { id: "" },
      available: false,
    });
    expect(trinoUnavailableReason(none)).toBe("no_cell");
    expect(trinoUnavailableMessage("no_cell")).toContain(
      "DUCKGRES_TRINO_CELLS_FILE",
    );
  });

  it("calls out an OPA authorization failure separately from an outage", () => {
    // A 403 means the bundle has not rolled out or the observer grant is
    // missing — fixed in the control plane, not in the cluster.
    const denied = status({
      available: false,
      error:
        "GET /v1/query: 403 forbidden — the __duckgres_observer principal is not authorized",
    });
    expect(trinoUnavailableReason(denied)).toBe("unauthorized");
    expect(trinoUnavailableMessage("unauthorized")).toContain("OPA bundle");
  });

  it("treats anything else as unreachable", () => {
    const down = status({
      available: false,
      error: "dial tcp: connection refused",
    });
    expect(trinoUnavailableReason(down)).toBe("unreachable");
    expect(trinoUnavailableMessage("unreachable")).toContain("config store");
  });
});

describe("trinoOrgsNeedingAttention", () => {
  it("surfaces failed orgs and never-provisioned orgs", () => {
    const rows = [
      org({ org: "ok", state: "ready", ready_at: "2026-08-01T00:00:00Z" }),
      org({ org: "broken", state: "failed" }),
      org({ org: "new", state: "pending" }),
      // Re-reconciling after a successful provision: a tick in flight, not
      // trouble. Flagging it would keep the warning permanently lit.
      org({
        org: "reconciling",
        state: "provisioning",
        ready_at: "2026-08-01T00:00:00Z",
      }),
    ];
    expect(trinoOrgsNeedingAttention(rows).map((o) => o.org)).toEqual([
      "broken",
      "new",
    ]);
  });
});

describe("trinoScanEfficiency", () => {
  it("is null before any rows have been processed", () => {
    // A query that has just started has no ratio to report; 0 would render
    // as perfect pruning.
    expect(
      trinoScanEfficiency(
        query({ processed_input_rows: 0, physical_input_bytes: 1_000 }),
      ),
    ).toBeNull();
  });

  it("reports bytes read per row returned", () => {
    // The DuckLake pruning signal: gigabytes read for a handful of rows
    // means predicates are not pruning files.
    expect(
      trinoScanEfficiency(
        query({ processed_input_rows: 10, physical_input_bytes: 1_000 }),
      ),
    ).toBe(100);
  });
});

describe("trinoStateVariant", () => {
  it("maps queued to a warning, because that is the slowness operators chase", () => {
    expect(trinoStateVariant("QUEUED")).toBe("warning");
    expect(trinoStateVariant("RUNNING")).toBe("success");
    expect(trinoStateVariant("FAILED")).toBe("destructive");
  });

  it("falls back rather than throwing on a state it does not know", () => {
    expect(trinoStateVariant("SOME_FUTURE_STATE")).toBe("outline");
    expect(trinoStateVariant("WAITING_FOR_RESOURCES")).toBe("warning");
  });
});

describe("summarizeTrinoNodes on an announce-only cell", () => {
  // The announce inventory returns URIs and nothing else, so every heartbeat
  // field arrives as a zero that means "not measured". Summarizing it as
  // health is how a console ends up telling an operator the fleet is fine
  // on the strength of data the coordinator never sent.
  const announced: TrinoNode[] = [
    {
      uri: "http://10.0.0.1:8080",
      age_ms: 0,
      recent_failures: 0,
      recent_successes: 0,
      recent_failure_ratio: 0,
      failed: false,
    },
    {
      uri: "http://10.0.0.2:8080",
      age_ms: 0,
      recent_failures: 0,
      recent_successes: 0,
      recent_failure_ratio: 0,
      failed: false,
    },
  ];

  it("counts membership but refuses to claim health", () => {
    const h = summarizeTrinoNodes(announced, "announce");
    expect(h.total).toBe(2);
    expect(h.healthKnown).toBe(false);
    expect(h.failed).toBe(0);
    expect(h.degraded).toBe(0);
  });

  it("still reports health when the failure detector is the source", () => {
    const h = summarizeTrinoNodes(
      [{ ...announced[0], failed: true }],
      "failure_detector",
    );
    expect(h.healthKnown).toBe(true);
    expect(h.failed).toBe(1);
  });

  // An older payload has no source field. Defaulting to failure_detector
  // keeps the pre-existing cells rendering exactly as before.
  it("defaults to the failure detector when the source is absent", () => {
    expect(summarizeTrinoNodes(announced).healthKnown).toBe(true);
  });
});

describe("summarizeTrinoNodes from system.runtime.nodes", () => {
  const node = (
    uri: string,
    version: string,
    state: string,
    coordinator = false,
  ) => ({
    uri,
    version,
    state,
    coordinator,
    node_id: uri,
    age_ms: 0,
    recent_failures: 0,
    recent_successes: 0,
    recent_failure_ratio: 0,
    failed: false,
  });

  it("surfaces version skew, which is the point of this source", () => {
    const h = summarizeTrinoNodes(
      [node("a", "476", "ACTIVE", true), node("b", "477", "ACTIVE")],
      "system_table",
    );
    expect(h.detailKnown).toBe(true);
    expect(h.versions).toEqual(["476", "477"]);
    expect(h.total).toBe(2);
  });

  it("collapses a single version and counts non-active nodes", () => {
    const h = summarizeTrinoNodes(
      [
        node("a", "476", "ACTIVE"),
        node("b", "476", "SHUTTING_DOWN"),
        node("c", "476", "INACTIVE"),
      ],
      "system_table",
    );
    expect(h.versions).toEqual(["476"]);
    expect(h.inactive).toBe(2);
  });

  // It carries no heartbeat ratios, so it must not claim the failure
  // detector's kind of health any more than the announce inventory does.
  it("does not claim heartbeat health", () => {
    const h = summarizeTrinoNodes([node("a", "476", "ACTIVE")], "system_table");
    expect(h.healthKnown).toBe(false);
    expect(h.failed).toBe(0);
    expect(h.degraded).toBe(0);
  });
});

describe("waiting states", () => {
  it("counts every pre-execution state as queued, not only QUEUED", () => {
    const states = ["QUEUED", "WAITING_FOR_RESOURCES", "DISPATCHING", "PLANNING", "STARTING", "RUNNING", "FINISHING"];
    const s = summarizeTrinoQueries(states.map((state, i) => query({ query_id: `q${i}`, state })));
    expect(s.queued).toBe(5);
    expect(s.running).toBe(2);
  });
  it("flags a query waiting for resources", () => {
    expect(isWaitingTrinoQuery(query({ state: "WAITING_FOR_RESOURCES" }))).toBe(true);
    expect(trinoQueryFlag(query({ state: "WAITING_FOR_RESOURCES" }))).toBe("queued");
    expect(trinoStateBucket(query({ state: "PLANNING" }))).toBe("waiting");
    expect(trinoStateBucket(query({ state: "FINISHING" }))).toBe("running");
    expect(trinoStateBucket(query({ state: "FAILED" }))).toBe("failed");
  });
});

describe("trinoSqlSummary", () => {
  it("strips the SQLMesh plan comment and names the target", () => {
    const s = trinoSqlSummary(
      '/* SQLMESH_PLAN: 4b1d */ INSERT INTO "org_portola"."sqlmesh_trino"."events__123" ("a") SELECT 1',
    );
    expect(s).toMatchObject({
      verb: "INSERT INTO",
      target: "org_portola.sqlmesh_trino.events__123",
      tag: "SQLMESH_PLAN: 4b1d",
    });
    expect(s.text.startsWith("INSERT INTO")).toBe(true);
  });
  it("reports DDL kind and skips IF NOT EXISTS", () => {
    expect(trinoSqlSummary("CREATE TABLE IF NOT EXISTS a.b AS SELECT 1")).toMatchObject({
      verb: "CREATE TABLE",
      target: "a.b",
    });
    expect(trinoSqlSummary("create or replace view x.y as select 1")).toMatchObject({
      verb: "CREATE VIEW",
      target: "x.y",
    });
    expect(trinoSqlSummary("DROP TABLE IF EXISTS s.t")).toMatchObject({ verb: "DROP TABLE", target: "s.t" });
  });
  it("treats WITH as a read and finds its first relation", () => {
    expect(trinoSqlSummary("-- note\nWITH c AS (SELECT * FROM s.events) SELECT * FROM c")).toMatchObject({
      verb: "SELECT",
      target: "s.events",
      tag: "note",
    });
  });
  it("handles empty and unknown text", () => {
    expect(trinoSqlSummary("")).toMatchObject({ verb: "", target: "", tag: "" });
    expect(trinoSqlSummary("SHOW CATALOGS")).toMatchObject({ verb: "SHOW", target: "" });
  });
});

describe("groupTrinoQueries", () => {
  it("rolls up per key, busiest first", () => {
    const groups = groupTrinoQueries(
      [
        query({ query_id: "1", org: "a", cpu_ms: 10 }),
        query({ query_id: "2", org: "b", state: "QUEUED" }),
        query({ query_id: "3", org: "b", cpu_ms: 5, physical_input_bytes: 7 }),
        query({ query_id: "4", org: "b", state: "FINISHED", elapsed_ms: 99_999 }),
      ],
      (q) => q.org,
    );
    expect(groups.map((g) => g.key)).toEqual(["b", "a"]);
    expect(groups[0]).toMatchObject({ total: 3, running: 1, queued: 1, cpuMs: 5, scannedBytes: 7, longestMs: 1_000 });
  });
});

describe("sortTrinoQueries", () => {
  const rows = [
    query({ query_id: "a", elapsed_ms: 5, cpu_ms: 3, state: "QUEUED" }),
    query({ query_id: "b", elapsed_ms: 9, cpu_ms: 1 }),
    query({ query_id: "c", elapsed_ms: 1, cpu_ms: 2, progress_percentage: 50 }),
  ];
  it("sorts by a numeric key in both directions without mutating", () => {
    expect(sortTrinoQueries(rows, "cpu", "desc").map((q) => q.query_id)).toEqual(["a", "c", "b"]);
    expect(sortTrinoQueries(rows, "cpu", "asc").map((q) => q.query_id)).toEqual(["b", "c", "a"]);
    expect(rows.map((q) => q.query_id)).toEqual(["a", "b", "c"]);
  });
  it("puts running before waiting and breaks ties by elapsed", () => {
    expect(sortTrinoQueries(rows, "state", "asc").map((q) => q.query_id)).toEqual(["b", "c", "a"]);
  });
  it("sorts unknown progress below zero", () => {
    expect(sortTrinoQueries(rows, "progress", "desc")[0].query_id).toBe("c");
  });
});
