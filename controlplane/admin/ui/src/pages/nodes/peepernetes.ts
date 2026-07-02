/*
 * peepernetes — live cluster node/pod visualizer, ported into the admin console.
 *
 * Faithful port of the standalone single-file app. The original consumed native
 * Kubernetes watch streams directly (served same-origin by `kubectl proxy`). The
 * browser here can't reach the K8s API, so instead it POLLS the admin backend's
 * projected topology endpoints (/api/v1/cluster/{nodes,pods,events,nodepools})
 * and diffs each snapshot against the previous one to synthesize the ADDED /
 * DELETED events the ticker shows — every visual feature (node cards, pod chips,
 * filters, counters, reclaim countdown, unscheduled tray, event stream) is
 * preserved. All state/DOM lives under the mount root so it tears down cleanly.
 *
 * The imperative DOM code mirrors the original almost line-for-line on purpose:
 * keeping it recognizably the same eases future diffing against upstream.
 */

import { setClusterCounts } from "@/lib/clusterCounts";

const API = "/api/v1/cluster";

// mountPeepernetes builds the whole view inside `root` and starts polling.
// Returns a cleanup function that stops all timers/fetches and clears the DOM.
export function mountPeepernetes(root: HTMLElement): () => void {
  const $ = (s: string): any => root.querySelector(s);

  root.classList.add("peeper");
  // Header carries only the filters + live indicator, left-aligned. The brand
  // and the nodes/workers/placeholders/pending counters were lifted OUT of this
  // header into the shared admin Topbar (see setClusterCounts + Topbar.tsx).
  root.innerHTML = `
  <div class="hdr">
    <div class="toggles">
      <div class="fctl"><span class="fl">GROUP BY</span>
        <select id="group-by">
          <option value="pool" selected>node pool</option>
          <option value="ns">namespace</option>
          <option value="deploy">deployment</option>
        </select>
      </div>
      <div class="fctl"><span class="fl">NAMESPACE</span>
        <select id="ns-filter"><option value="">all</option></select>
      </div>
      <div class="fctl"><span class="fl">NODE POOL</span>
        <div class="pick" id="pool-pick">
          <button id="pool-btn" type="button">pools ▾</button>
          <div class="menu" id="pool-menu"></div>
        </div>
      </div>
      <div class="fctl"><span class="fl">SYSTEM PODS</span>
        <select id="sys-mode">
          <option value="hide" selected>hidden</option>
          <option value="dot">dots</option>
          <option value="expand">expanded</option>
        </select>
      </div>
    </div>
    <div id="conn"><div class="dot"></div><span>CONNECTING</span></div>
  </div>

  <div class="main">
    <section id="tray">
      <div class="pool-head"><span class="name">⚠ Unscheduled</span><span class="count" id="tray-count"></span></div>
      <div class="pods" id="tray-pods"></div>
    </section>
    <div id="pools"></div>
  </div>

  <div id="ticker">
    <div class="th">EVENT STREAM</div>
    <div id="events"></div>
    <div id="legend">
      <span><i style="background:var(--headroom); border:1px dashed var(--headroom); background:transparent"></i>PLACEHOLDER (pause / negative priority)</span>
      <span><i style="background:var(--system)"></i>SYSTEM (daemonsets, kube-*)</span>
      <span>APPS — one color per deployment</span>
    </div>
  </div>`;

  // ── lifecycle bookkeeping (for cleanup) ────────────────
  let stopped = false;
  const timers: number[] = [];
  const controllers: AbortController[] = [];
  const every = (ms: number, fn: () => void) => {
    timers.push(window.setInterval(fn, ms));
  };

  // ── state ──────────────────────────────────────────────
  const nodes = new Map<string, any>();
  const pods = new Map<string, any>();
  let dirty = false;
  const pollOk = { nodes: false, pods: false };

  // ── k8s quantity parsing ───────────────────────────────
  function parseCpu(q: string): number {
    if (!q) return 0;
    if (q.endsWith("m")) return +q.slice(0, -1);
    return +q * 1000; // millicores
  }
  const MEM_SUF: Record<string, number> = { Ki: 2 ** 10, Mi: 2 ** 20, Gi: 2 ** 30, Ti: 2 ** 40, k: 1e3, K: 1e3, M: 1e6, G: 1e9, T: 1e12 };
  function parseMem(q: string): number {
    if (!q) return 0;
    const m = q.match(/^([0-9.]+)([A-Za-z]*)$/);
    if (!m) return 0;
    return +m[1] * (MEM_SUF[m[2]] || 1);
  }
  function fmtCpu(mc: number): string { return mc >= 1000 ? (mc / 1000).toFixed(mc % 1000 ? 1 : 0) + " CPU" : mc + "m"; }
  function fmtMem(b: number): string {
    if (b < 2 ** 30) return Math.round(b / 2 ** 20) + " Mi";
    const g = Math.round(b / 2 ** 30 * 10) / 10;
    return (g % 1 ? g.toFixed(1) : g.toFixed(0)) + " Gi";
  }
  function podReq(pod: any): { cpu: number; mem: number } {
    let cpu = 0, mem = 0;
    for (const c of pod.spec.containers || []) {
      cpu += parseCpu(c.resources?.requests?.cpu);
      mem += parseMem(c.resources?.requests?.memory);
    }
    return { cpu, mem };
  }
  function age(ts: string): string {
    const s = Math.max(0, (Date.now() - Date.parse(ts)) / 1000);
    if (s < 90) return Math.round(s) + "s";
    if (s < 5400) return Math.round(s / 60) + "m";
    if (s < 2 * 86400) return (s / 3600).toFixed(1) + "h";
    return Math.round(s / 86400) + "d";
  }

  // ── classification (universal) ─────────────────────────
  const PAUSE_IMG = /(^|\/)pause(:|@|$)/;
  function isPlaceholder(p: any): boolean {
    if ((p.metadata.labels || {})["cluster-autoscaler.kubernetes.io/overprovisioning"] !== undefined) return true;
    if (typeof p.spec.priority === "number" && p.spec.priority < 0) return true;
    const cs = p.spec.containers || [];
    return cs.length > 0 && cs.every((c: any) => PAUSE_IMG.test(c.image || ""));
  }
  function classify(pod: any): string {
    if (isPlaceholder(pod)) return "headroom";
    if ((pod.metadata.ownerReferences || []).some((o: any) => o.kind === "DaemonSet")) return "system";
    if ((pod.metadata.annotations || {})["kubernetes.io/config.mirror"]) return "system";
    if (pod.metadata.namespace.startsWith("kube-") || pod.metadata.namespace === "karpenter") return "system";
    return "a:" + deployKey(pod);
  }
  const kindOrder = (k: string) => k === "headroom" ? 1 : k === "system" ? 5 : 0;
  const kindClass = (k: string) => k === "headroom" ? "k-headroom" : k === "system" ? "k-system" : "k-app";
  const ROLE_HUES: [RegExp, number][] = [
    [/(^|[-_])(workers?|executors?|runners?)([-_]|$)/, 192],
    [/control[-_]?plane|controller|coordinator|scheduler/, 270],
  ];
  function appColor(key: string): string {
    for (const [re, hue] of ROLE_HUES) if (re.test(key)) return `hsl(${hue} 75% 62%)`;
    let h = 2166136261;
    for (let i = 0; i < key.length; i++) { h ^= key.charCodeAt(i); h = Math.imul(h, 16777619); }
    const t = (h >>> 0) % 265;
    const hue = t < 75 ? 20 + t : 150 + (t - 75);
    return `hsl(${hue} 75% 62%)`;
  }
  function nodePool(node: any): string {
    return (node.metadata.labels || {})["karpenter.sh/nodepool"] || "static";
  }
  const POOL_ORDER = (p: string) =>
    p === "duckgres-workers" ? 0 : p === "duckgres-cp" ? 1 : p === "static" ? 9 : 5;

  // pool multi-select: null = default (duckgres pools), "all", or a Set
  let poolSel: null | "all" | Set<string> = null;
  function poolVisible(p: string): boolean {
    if (poolSel === null) return p.startsWith("duckgres");
    if (poolSel === "all") return true;
    return poolSel.has(p);
  }
  function observedPools(): string[] {
    const s = new Set<string>();
    for (const n of nodes.values()) s.add(nodePool(n));
    for (const g of goneNodes.values()) s.add(nodePool(g.node));
    if (poolSel instanceof Set) for (const p of poolSel) s.add(p);
    return [...s].sort((a, b) => (POOL_ORDER(a) - POOL_ORDER(b)) || a.localeCompare(b));
  }
  function poolOfNodeName(name: string): string | null {
    for (const n of nodes.values()) if (n.metadata.name === name) return nodePool(n);
    for (const g of goneNodes.values()) if (g.node.metadata.name === name) return nodePool(g.node);
    return null;
  }
  function syncPoolMenu(): void {
    let html = `<label><input type="checkbox" id="pool-all" ${poolSel === "all" ? "checked" : ""}> all</label><div class="sep"></div>`;
    for (const p of observedPools())
      html += `<label><input type="checkbox" data-pool="${esc(p)}" ${poolVisible(p) ? "checked" : ""}> ${esc(p)}</label>`;
    const menu = $("#pool-menu");
    if (menu.innerHTML !== html) menu.innerHTML = html;
    const btn = $("#pool-btn");
    btn.textContent = (poolSel === null ? "duckgres" :
      poolSel === "all" ? "all" :
      poolSel.size === 0 ? "none" :
      poolSel.size === 1 ? [...poolSel][0] : poolSel.size + " selected") + " ▾";
    btn.classList.toggle("active", poolSel !== "all");
  }

  // ── connection indicator ───────────────────────────────
  function setConn(): void {
    const ok = pollOk.nodes && pollOk.pods;
    $("#conn").classList.toggle("live", ok);
    $("#conn span").textContent = ok ? "LIVE" : "RECONNECTING";
  }

  // ── snapshot polling: fetch a projected list, diff vs the store to emit
  //    ADDED/DELETED for the ticker, and replace the store. The FIRST poll of
  //    each stream seeds silently (matches the original watch, whose initial
  //    LIST populates state without spamming history). ───────────────────────
  function startPoll(
    path: string,
    store: Map<string, any>,
    onEvent: (type: string, o: any) => void,
    connKey: "nodes" | "pods" | null,
    intervalMs: number,
  ): void {
    let firstLoad = true;
    const tick = async () => {
      if (stopped) return;
      const ctrl = new AbortController();
      controllers.push(ctrl);
      try {
        const resp = await fetch(path, { cache: "no-store", signal: ctrl.signal, headers: { Accept: "application/json" } });
        if (!resp.ok) throw new Error("http " + resp.status);
        const list = await resp.json();
        const items: any[] = list.items || [];
        const next = new Set<string>();
        for (const o of items) {
          const uid = o.metadata.uid;
          next.add(uid);
          const had = store.has(uid);
          store.set(uid, o);
          if (!firstLoad && !had) onEvent("ADDED", o);
        }
        for (const [uid, o] of [...store]) {
          if (!next.has(uid)) {
            store.delete(uid);
            if (!firstLoad) onEvent("DELETED", o);
          }
        }
        firstLoad = false;
        dirty = true;
        if (connKey) { pollOk[connKey] = true; setConn(); }
      } catch {
        if (connKey) { pollOk[connKey] = false; setConn(); }
      } finally {
        const i = controllers.indexOf(ctrl);
        if (i >= 0) controllers.splice(i, 1);
      }
    };
    void tick();
    every(intervalMs, () => void tick());
  }

  // ── karpenter nodepool config (for empty-node reclaim countdown) ──
  const poolCfg = new Map<string, { policy: string; afterMs: number | null }>();
  function parseDur(s: string): number | null {
    if (!s || s === "Never") return null;
    let ms = 0;
    for (const m of s.matchAll(/(\d+)([hms])/g)) ms += +m[1] * ({ h: 3600e3, m: 60e3, s: 1e3 } as any)[m[2]];
    return ms || null;
  }
  async function loadPoolCfg(): Promise<void> {
    if (stopped) return;
    try {
      const l = await (await fetch(`${API}/nodepools`, { cache: "no-store" })).json();
      for (const np of l.items || []) {
        const d = np.spec?.disruption || {};
        poolCfg.set(np.metadata.name, { policy: d.consolidationPolicy || "", afterMs: parseDur(d.consolidateAfter) });
      }
      dirty = true;
    } catch { /* karpenter absent / transient — countdowns simply won't show */ }
  }
  void loadPoolCfg();
  every(60e3, () => void loadPoolCfg());

  // Karpenter "WhenEmpty" ignores daemonset-owned and mirror pods
  function ignoredForEmpty(p: any): boolean {
    return (p.metadata.ownerReferences || []).some((o: any) => o.kind === "DaemonSet") ||
      !!(p.metadata.annotations || {})["kubernetes.io/config.mirror"];
  }
  const emptySince = new Map<string, { ts: number; approx: boolean }>();
  try { for (const [k, v] of JSON.parse(sessionStorage.getItem("peepernetes-emptysince") || "[]")) emptySince.set(k, v); } catch { /* ignore */ }
  // How long a node has been draining. deletionTimestamp is authoritative when a
  // node is actually being deleted; a cordon / karpenter-disrupt taint has no
  // server-side start time, so we record first-seen client-side (persisted so it
  // survives a reload; `approx` when we can't know it started exactly then).
  const drainSince = new Map<string, { ts: number; approx: boolean }>();
  try { for (const [k, v] of JSON.parse(sessionStorage.getItem("peepernetes-drainsince") || "[]")) drainSince.set(k, v); } catch { /* ignore */ }
  let podsLoadedAt = 0;
  let nodesLoadedAt = 0;
  function fmtLeft(ms: number): string { return ms >= 60e3 ? Math.ceil(ms / 60e3) + "m" : Math.ceil(ms / 1e3) + "s"; }
  // Elapsed duration (floored, s/m/h/d) — for how long something has been in a state.
  function fmtDur(ms: number): string {
    const s = Math.max(0, ms / 1000);
    if (s < 90) return Math.round(s) + "s";
    if (s < 5400) return Math.round(s / 60) + "m";
    if (s < 2 * 86400) return (s / 3600).toFixed(1) + "h";
    return Math.round(s / 86400) + "d";
  }
  // Container image(s) a pod runs. Short form drops the registry/repo path and
  // keeps the final `name:tag` (or `name@sha…`); "+N" when a pod has sidecars.
  function shortImage(img: string): string { return (img.split("/").pop() || img); }
  function podImages(pod: any): string[] {
    return (pod.spec.containers || []).map((c: any) => c.image || "").filter(Boolean);
  }
  function imageLabel(pod: any): string {
    const imgs = podImages(pod);
    if (!imgs.length) return "";
    const s = shortImage(imgs[0]);
    return imgs.length > 1 ? `${s} +${imgs.length - 1}` : s;
  }

  // ── event ticker ───────────────────────────────────────
  const evBox = $("#events");
  // Escapes for BOTH text and attribute contexts (esc(p) is interpolated into a
  // data-pool="…" attribute), so quotes must be escaped too or a pool/label name
  // containing a double-quote could break out of the attribute (CodeQL XSS).
  const esc = (s: any): string => String(s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  const evLog: any[] = [];
  function renderEvRow(e: any, animate: boolean): HTMLDivElement {
    const row = document.createElement("div");
    row.className = "ev " + e.cls;
    if (!animate) row.style.animation = "none";
    row.innerHTML = `<span class="t">${e.t}</span><span class="a">${e.action}</span><span class="o">${e.text}</span>`;
    if (e.color) (row.querySelector(".o") as HTMLElement).style.color = e.color;
    return row;
  }
  function logEvent(action: string, text: string, kind: string | null, cls?: string): void {
    cls = cls || (action === "ADDED" ? "add" : action === "DELETED" ? "del" : "mod");
    const color = !kind ? "" :
      kind === "headroom" ? "var(--headroom)" :
      kind === "system" ? "var(--dim)" :
      kind.startsWith("a:") ? appColor(kind.slice(2)) : kind;
    const e = { t: new Date().toTimeString().slice(0, 8), action, text, cls, color };
    evLog.push(e);
    while (evLog.length > 200) evLog.shift();
    try { sessionStorage.setItem("peepernetes-evlog", JSON.stringify(evLog)); } catch { /* ignore */ }
    evBox.prepend(renderEvRow(e, true));
    while (evBox.children.length > 200) evBox.lastChild!.remove();
  }
  // restore history from a prior mount/reload, oldest first, with a separator
  try {
    const prev = JSON.parse(sessionStorage.getItem("peepernetes-evlog") || "[]");
    if (prev.length) {
      evLog.push(...prev);
      for (const e of prev) evBox.prepend(renderEvRow(e, false));
      evBox.prepend(renderEvRow({ t: new Date().toTimeString().slice(0, 8), action: "·", text: "— view reopened, history restored —", cls: "mod", color: "" }, false));
    }
  } catch { /* ignore */ }

  function nsFilter(): string { return $("#ns-filter").value; }
  const sysMode = (): string => $("#sys-mode").value; // hide | dot | expand

  function onPodEvent(type: string, pod: any): void {
    if (type === "MODIFIED") return;
    const ns = nsFilter();
    if (ns && pod.metadata.namespace !== ns) return;
    const k = classify(pod);
    if (k === "system" && sysMode() !== "expand") return;
    logEvent(type, `pod <b>${esc(pod.metadata.name)}</b> · ${esc(pod.metadata.namespace)} · ${esc(pod.spec.nodeName || "unscheduled")}`, k);
  }

  const goneNodes = new Map<string, { node: any; until: number }>();
  const NODE_LINGER_MS = 10000;
  function onNodeEvent(type: string, node: any): void {
    if (type === "DELETED") goneNodes.set(node.metadata.uid, { node, until: Date.now() + NODE_LINGER_MS });
    else goneNodes.delete(node.metadata.uid);
    if (type === "MODIFIED") return;
    if (!poolVisible(nodePool(node))) return;
    const l = node.metadata.labels || {};
    const inst = [l["node.kubernetes.io/instance-type"], l["karpenter.sh/capacity-type"]].filter(Boolean).join(" ");
    logEvent(type, `node <b>${esc(node.metadata.name)}</b> · pool ${esc(nodePool(node))}${inst ? " · " + esc(inst) : ""}`, "var(--worker)");
  }
  function nodeDraining(n: any): boolean {
    return !!n.metadata.deletionTimestamp || !!n.spec?.unschedulable ||
      (n.spec?.taints || []).some((t: any) => t.key.startsWith("karpenter.sh/disrupt"));
  }

  // ── K8s Events stream: surface WHY pods/nodes churn ────
  const EV_REASONS = /^(Preempted|Evicted|TaintManagerEviction|FailedScheduling|Nominated|ScalingReplicaSet|NodeNotReady|DisruptionTerminating|DisruptionLaunching|Launched|Drain)/;
  const evStore = new Map<string, any>();
  function onK8sEvent(type: string, e: any): void {
    if (type !== "ADDED") return;
    if (!EV_REASONS.test(e.reason || "")) return;
    const io = e.involvedObject || {};
    const ns = nsFilter();
    if (ns && io.namespace && io.namespace !== ns) return;
    if (sysMode() !== "expand" && (io.namespace || "").startsWith("kube-")) return;
    if (io.kind === "Node") {
      const p = poolOfNodeName(io.name);
      if (p && !poolVisible(p)) return;
    }
    if (io.kind === "NodeClaim" && !poolVisible(io.name.replace(/-[^-]+$/, ""))) return;
    logEvent(e.reason, `${(io.kind || "").toLowerCase()} <b>${esc(io.name || "")}</b> · ${esc(e.message || "")}`, null, "rsn");
  }

  // ── rendering ──────────────────────────────────────────
  const poolsBox: HTMLElement = $("#pools");
  const seen = new Set<string>();
  let firstPaint = true;
  let lastCountsKey = "";

  function nodeReady(node: any): boolean {
    return (node.status?.conditions || []).some((c: any) => c.type === "Ready" && c.status === "True");
  }

  function podChip(pod: any, _k: string): HTMLDivElement {
    const el = document.createElement("div");
    el.className = "pod " + kindClass(_k);
    el.dataset.uid = pod.metadata.uid;
    el.innerHTML = `<span class="pn"></span><span class="pi"></span><span class="pm"></span>`;
    if (!firstPaint && !seen.has(pod.metadata.uid)) el.classList.add("fresh");
    seen.add(pod.metadata.uid);
    setTimeout(() => el.classList.remove("fresh"), 1700);
    return el;
  }
  function updateChip(el: HTMLElement, pod: any, k: string): void {
    const phase = pod.status?.phase || "?";
    if (k.startsWith("a:")) el.style.setProperty("--c", appColor(k.slice(2)));
    else el.style.removeProperty("--c");
    const terminating = !!pod.metadata.deletionTimestamp;
    el.classList.toggle("dot", k === "system" && sysMode() === "dot");
    el.classList.toggle("terminating", terminating);
    el.classList.toggle("pending", phase === "Pending" && !terminating);
    el.classList.toggle("failed", phase === "Failed");
    const { cpu, mem } = podReq(pod);
    (el.querySelector(".pn") as HTMLElement).textContent = pod.metadata.name;
    // Every pod shows its running image (short name:tag; full path in the tooltip).
    const imgs = podImages(pod);
    (el.querySelector(".pi") as HTMLElement).textContent = imageLabel(pod);
    // While terminating, the meta line shows how long it's been draining down
    // (since deletionTimestamp — authoritative). Otherwise req · age as before.
    (el.querySelector(".pm") as HTMLElement).textContent = terminating
      ? `terminating ${fmtDur(Date.now() - Date.parse(pod.metadata.deletionTimestamp))}`
      : `${cpu || mem ? fmtCpu(cpu) + " · " + fmtMem(mem) : phase.toLowerCase()}${cpu || mem ? " · " + age(pod.metadata.creationTimestamp) : ""}`;
    el.title = `${pod.metadata.namespace}/${pod.metadata.name}\nphase: ${phase}` +
      (terminating ? ` (terminating ${fmtDur(Date.now() - Date.parse(pod.metadata.deletionTimestamp))})` : "") +
      `\nnode: ${pod.spec.nodeName || "—"}\nreq: ${fmtCpu(cpu)} / ${fmtMem(mem)}` +
      (imgs.length ? `\nimage: ${imgs.join("\n       ")}` : "");
  }

  // reconcile a flex container of pod chips against a desired pod list
  function reconcilePods(container: HTMLElement, list: [any, string][]): void {
    const want = new Map(list.map(([p, k]) => [p.metadata.uid, [p, k]]));
    for (const el of [...container.children] as HTMLElement[]) {
      const uid = el.dataset.uid;
      if (!uid) continue; // free-space ghost — caller-managed
      if (!want.has(uid)) {
        if (!el.classList.contains("dying")) {
          el.classList.add("dying");
          setTimeout(() => el.remove(), 550);
        }
      }
    }
    let prev: HTMLElement | null = null;
    for (const [p, k] of list) {
      let el: HTMLElement = container.querySelector(`[data-uid="${p.metadata.uid}"]`) as HTMLElement ||
        root.querySelector(`.pod[data-uid="${p.metadata.uid}"]`) as HTMLElement; // adopt from old node on reschedule
      if (!el) el = podChip(p, k);
      el.className = el.className.replace(/k-\w+/, kindClass(k));
      updateChip(el, p, k);
      el.classList.remove("dying");
      if (prev ? prev.nextElementSibling !== el : container.firstElementChild !== el)
        container.insertBefore(el, prev ? prev.nextElementSibling : container.firstElementChild);
      prev = el;
    }
  }

  function syncNsOptions(): void {
    const sel = $("#ns-filter");
    const have = new Set([...sel.options].map((o: any) => o.value));
    const want = new Set([...pods.values()].map((p) => p.metadata.namespace));
    for (const ns of [...want].sort()) {
      if (have.has(ns)) continue;
      const o = document.createElement("option");
      o.value = o.textContent = ns;
      let before: any = null;
      for (const e of [...sel.options].slice(1)) if (e.value > ns) { before = e; break; }
      sel.insertBefore(o, before);
    }
    for (const o of [...sel.options].slice(1) as any[])
      if (!want.has(o.value) && o.value !== sel.value) o.remove();
    sel.classList.toggle("active", !!sel.value);
  }

  function deployKey(p: any): string {
    const o = (p.metadata.ownerReferences || [])[0];
    if (!o) return p.metadata.name.replace(/-\d+$/, "");
    if (o.kind === "ReplicaSet") return o.name.replace(/-[a-z0-9]{5,10}$/, "");
    return o.name;
  }
  function renderFlatGroups(mode: string, flatPods: [any, string][], sortPods: (l: [any, string][]) => void): void {
    const poolOf = new Map<string, string>();
    for (const n of nodes.values()) poolOf.set(n.metadata.name, nodePool(n));
    const groups = new Map<string, [any, string][]>();
    for (const [p, k] of flatPods) {
      if (!poolVisible(poolOf.get(p.spec.nodeName) || "static")) continue;
      const key = mode === "ns" ? p.metadata.namespace : deployKey(p);
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push([p, k]);
    }
    for (const el of [...poolsBox.children] as HTMLElement[])
      if (!groups.has(el.dataset.pool!)) el.remove();
    let prevSec: HTMLElement | null = null;
    for (const gname of [...groups.keys()].sort()) {
      let sec = poolsBox.querySelector(`[data-pool="${CSS.escape(gname)}"]`) as HTMLElement;
      if (!sec) {
        sec = document.createElement("section");
        sec.className = "pool"; sec.dataset.pool = gname; sec.dataset.mode = mode;
        sec.innerHTML = `<div class="pool-head"><span class="name"></span><span class="count"></span></div><div class="pods flat"></div>`;
        (sec.querySelector(".name") as HTMLElement).textContent = gname;
      }
      const pos: Element | null = prevSec ? prevSec.nextElementSibling : poolsBox.firstElementChild;
      if (pos !== sec) poolsBox.insertBefore(sec, pos);
      prevSec = sec;
      const list = groups.get(gname)!;
      sortPods(list);
      let cpu = 0, mem = 0;
      for (const [p] of list) { const r = podReq(p); cpu += r.cpu; mem += r.mem; }
      (sec.querySelector(".count") as HTMLElement).textContent =
        `${list.length} pod${list.length === 1 ? "" : "s"} · ${fmtCpu(cpu)} · ${fmtMem(mem)}`;
      reconcilePods(sec.querySelector(".pods") as HTMLElement, list);
    }
  }

  function render(): void {
    syncNsOptions();
    syncPoolMenu();
    const ns = nsFilter();

    if (pods.size && !podsLoadedAt) podsLoadedAt = Date.now();
    if (nodes.size && !nodesLoadedAt) nodesLoadedAt = Date.now();

    const poolOfNode = new Map<string, string>();
    for (const n of nodes.values()) poolOfNode.set(n.metadata.name, nodePool(n));
    const byNode = new Map<string, [any, string][]>();
    const flatPods: [any, string][] = [];
    const unsched: [any, string][] = [];
    let nPods = 0, nHead = 0, nPend = 0;
    let pCpu = 0, pMem = 0, hCpu = 0, hMem = 0;
    for (const p of pods.values()) {
      const k = classify(p);
      if (ns && p.metadata.namespace !== ns) continue;
      const counted = !(k === "system" && sysMode() === "hide") &&
        (!p.spec.nodeName || poolVisible(poolOfNode.get(p.spec.nodeName) || "static"));
      if (counted && !p.metadata.deletionTimestamp && p.status?.phase !== "Succeeded") {
        if (k === "headroom") {
          nHead++;
          const r = podReq(p); hCpu += r.cpu; hMem += r.mem;
        } else {
          nPods++;
          const r = podReq(p); pCpu += r.cpu; pMem += r.mem;
        }
      }
      if (counted && p.status?.phase === "Pending") nPend++;
      if (p.status?.phase === "Succeeded") continue;
      if (k === "system" && sysMode() === "hide") continue;
      if (!p.spec.nodeName) { if (p.status?.phase === "Pending") unsched.push([p, k]); continue; }
      if (!byNode.has(p.spec.nodeName)) byNode.set(p.spec.nodeName, []);
      byNode.get(p.spec.nodeName)!.push([p, k]);
      flatPods.push([p, k]);
    }
    const sortPods = (l: [any, string][]) => l.sort((a, b) =>
      (kindOrder(a[1]) - kindOrder(b[1])) || a[1].localeCompare(b[1]) ||
      a[0].metadata.name.localeCompare(b[0].metadata.name));
    for (const l of byNode.values()) sortPods(l);
    sortPods(unsched);

    // Header counters now live in the shared admin Topbar; build the CPU/MEM
    // total strings here and push everything (incl. the node count below) via
    // setClusterCounts once nNodes is known.
    const pct = (a: number, b: number) => b ? Math.round(a / b * 100) + "%" : "–";
    const workerReq = nPods ? `${fmtCpu(pCpu)} · ${fmtMem(pMem)}` : "";
    const placeholderReq = nHead ? `${fmtCpu(hCpu)} · ${fmtMem(hMem)} · ${pct(hCpu, pCpu)}/${pct(hMem, pMem)} of workers` : "";

    $("#tray").classList.toggle("show", unsched.length > 0);
    $("#tray-count").textContent = unsched.length ? unsched.length + " pod(s) waiting for a node" : "";
    reconcilePods($("#tray-pods"), unsched);

    const goneUids = new Set<string>();
    const allNodes = [...nodes.values()];
    for (const [uid, g] of goneNodes) {
      if (Date.now() > g.until) { goneNodes.delete(uid); continue; }
      if (nodes.has(uid)) continue;
      goneUids.add(uid);
      allNodes.push(g.node);
    }

    const pools = new Map<string, any[]>();
    for (const n of allNodes) {
      const pool = nodePool(n);
      if (!poolVisible(pool)) continue;
      if (!pools.has(pool)) pools.set(pool, []);
      pools.get(pool)!.push(n);
    }
    let nNodes = 0;
    for (const l of pools.values()) for (const n of l) if (!goneUids.has(n.metadata.uid)) nNodes++;
    // Only publish to the Topbar when a value actually changed (avoids a Topbar
    // re-render on every poll tick when the numbers are unchanged).
    const countsKey = [nNodes, nPods, nHead, nPend, workerReq, placeholderReq].join("|");
    if (countsKey !== lastCountsKey) {
      lastCountsKey = countsKey;
      setClusterCounts({ nodes: nNodes, workers: nPods, placeholders: nHead, pending: nPend, workerReq, placeholderReq });
    }
    const poolNames = [...pools.keys()].sort((a, b) => (POOL_ORDER(a) - POOL_ORDER(b)) || a.localeCompare(b));

    const gmode = $("#group-by").value;
    for (const el of [...poolsBox.children] as HTMLElement[])
      if ((el.dataset.mode || "pool") !== gmode) el.remove();

    if (gmode !== "pool") {
      renderFlatGroups(gmode, flatPods, sortPods);
    } else {
      for (const el of [...poolsBox.children] as HTMLElement[])
        if (!pools.has(el.dataset.pool!)) el.remove();
      let prevPool: HTMLElement | null = null;
      for (const pname of poolNames) {
        let sec = poolsBox.querySelector(`[data-pool="${CSS.escape(pname)}"]`) as HTMLElement;
        if (!sec) {
          sec = document.createElement("section");
          sec.className = "pool"; sec.dataset.pool = pname; sec.dataset.mode = "pool";
          sec.innerHTML = `<div class="pool-head"><span class="name"></span><span class="count"></span></div><div class="nodes"></div>`;
          (sec.querySelector(".name") as HTMLElement).textContent = pname;
          (sec.querySelector(".name") as HTMLElement).classList.toggle("hot", pname === "duckgres-workers");
        }
        const secPos: Element | null = prevPool ? prevPool.nextElementSibling : poolsBox.firstElementChild;
        if (secPos !== sec) poolsBox.insertBefore(sec, secPos);
        prevPool = sec;

        const list = pools.get(pname)!.sort((a, b) =>
          Date.parse(b.metadata.creationTimestamp) - Date.parse(a.metadata.creationTimestamp));
        (sec.querySelector(".count") as HTMLElement).textContent = list.length + (list.length === 1 ? " node" : " nodes");
        const grid = sec.querySelector(".nodes") as HTMLElement;

        const wantNodes = new Set(list.map((n) => n.metadata.uid));
        for (const el of [...grid.children] as HTMLElement[]) {
          if (!wantNodes.has(el.dataset.uid!) && !el.classList.contains("dying")) {
            el.classList.add("dying");
            setTimeout(() => el.remove(), 650);
          }
        }
        let prevNode: HTMLElement | null = null;
        for (const n of list) {
          let card = grid.querySelector(`[data-uid="${n.metadata.uid}"]`) as HTMLElement;
          if (!card) {
            card = document.createElement("div");
            card.className = "node"; card.dataset.uid = n.metadata.uid;
            card.innerHTML = `
              <div class="node-top"><span class="node-name"></span><span class="node-meta"></span><span class="node-exp"></span><span class="node-age"></span></div>
              <div class="bars">
                <div class="bar"><span class="lbl">CPU</span><div class="track"><div class="fill"></div></div><span class="val"></span></div>
                <div class="bar"><span class="lbl">MEM</span><div class="track"><div class="fill"></div></div><span class="val"></span></div>
              </div>
              <div class="pods"></div>`;
            if (!firstPaint && !seen.has(n.metadata.uid)) {
              card.classList.add("fresh");
              setTimeout(() => card.classList.remove("fresh"), 3000);
            }
            seen.add(n.metadata.uid);
          }
          card.classList.remove("dying");
          const isGone = goneUids.has(n.metadata.uid);
          card.classList.toggle("gone", isGone);
          card.classList.toggle("draining", !isGone && nodeDraining(n));
          card.classList.toggle("notready", !isGone && !nodeDraining(n) && !nodeReady(n));
          if (isGone || !nodeDraining(n)) drainSince.delete(n.metadata.name);
          (card.querySelector(".node-name") as HTMLElement).textContent = n.metadata.name.replace(".ec2.internal", "");
          (card.querySelector(".node-meta") as HTMLElement).textContent =
            (n.metadata.labels?.["node.kubernetes.io/instance-type"] || "") +
            ((n.metadata.labels?.["karpenter.sh/capacity-type"] === "spot") ? " · spot" : "");
          (card.querySelector(".node-age") as HTMLElement).textContent = "↑ " + age(n.metadata.creationTimestamp);

          let cpu = 0, mem = 0;
          for (const p of pods.values()) {
            if (p.spec.nodeName !== n.metadata.name || p.status?.phase === "Succeeded" || p.status?.phase === "Failed") continue;
            const r = podReq(p); cpu += r.cpu; mem += r.mem;
          }
          const aCpu = parseCpu(n.status?.allocatable?.cpu), aMem = parseMem(n.status?.allocatable?.memory);
          const bars = card.querySelectorAll(".bar");
          const setBar = (bar: Element, used: number, alloc: number, fmtPair: (u: number, a: number) => string) => {
            const p = alloc ? Math.min(100, used / alloc * 100) : 0;
            const fill = bar.querySelector(".fill") as HTMLElement;
            fill.style.width = p + "%";
            fill.classList.toggle("warn", p > 70 && p <= 92);
            fill.classList.toggle("crit", p > 92);
            (bar.querySelector(".val") as HTMLElement).textContent = fmtPair(used, alloc);
          };
          const n1 = (x: number) => (x % 1 ? x.toFixed(1) : x.toFixed(0));
          const cores = (mc: number) => n1(Math.round(mc / 100) / 10);
          const gibi = (b: number) => n1(Math.round(b / 2 ** 30 * 10) / 10);
          setBar(bars[0], cpu, aCpu, (u, a) => `${cores(u)} / ${cores(a)} CPU`);
          setBar(bars[1], mem, aMem, (u, a) => `${gibi(u)} / ${gibi(a)} Gi`);

          const expEl = card.querySelector(".node-exp") as HTMLElement;
          const cfg = poolCfg.get(pname);
          let expTxt = "";
          expEl.classList.remove("due", "draining", "bad", "gone");
          if (isGone) {
            expTxt = "TERMINATED";
            expEl.classList.add("gone");
            expEl.title = "node deleted — shown briefly for context";
          } else if (nodeDraining(n)) {
            // How long it's been draining: deletionTimestamp is authoritative;
            // otherwise fall back to when we first saw it draining (approx if it
            // was already draining at load — real start unknown, so mark it "≥").
            let startMs: number, approx = false;
            if (n.metadata.deletionTimestamp) {
              startMs = Date.parse(n.metadata.deletionTimestamp);
              drainSince.delete(n.metadata.name);
            } else {
              let rec = drainSince.get(n.metadata.name);
              if (!rec) { rec = { ts: Date.now(), approx: !!nodesLoadedAt && Date.now() - nodesLoadedAt < 3000 }; drainSince.set(n.metadata.name, rec); }
              startMs = rec.ts; approx = rec.approx;
            }
            expTxt = `DRAINING ${approx ? "≥" : ""}${fmtDur(Date.now() - startMs)}`;
            expEl.classList.add("draining");
            expEl.title = "node is cordoned / being disrupted — pods drain, then it terminates" +
              (approx ? "\n(was already draining at page load — actual start may be earlier)" : "");
          } else if (!nodeReady(n)) {
            expTxt = "NOT READY";
            expEl.classList.add("bad");
            expEl.title = "node Ready condition is not True";
          } else if (podsLoadedAt && cfg?.afterMs && cfg.policy.startsWith("WhenEmpty")) {
            let occupied = false;
            for (const p of pods.values()) {
              if (p.spec.nodeName !== n.metadata.name) continue;
              const ph = p.status?.phase;
              if (ph === "Succeeded" || ph === "Failed") continue;
              if (!ignoredForEmpty(p)) { occupied = true; break; }
            }
            if (occupied) emptySince.delete(n.metadata.name);
            else {
              let rec = emptySince.get(n.metadata.name);
              if (!rec) { rec = { ts: Date.now(), approx: Date.now() - podsLoadedAt < 3000 }; emptySince.set(n.metadata.name, rec); }
              const left = rec.ts + cfg.afterMs - Date.now();
              expTxt = left > 0 ? `♻ ${rec.approx ? "≤" : ""}${fmtLeft(left)}` : "♻ reclaim due";
              expEl.classList.toggle("due", left <= 0);
              expEl.title = `node is empty — Karpenter ${cfg.policy} reclaims after ${Math.round(cfg.afterMs / 60e3)}m empty` +
                (rec.approx ? "\n(was already empty at page load — actual deadline may be sooner)" : "");
            }
          }
          expEl.textContent = expTxt;

          const podsBox2 = card.querySelector(".pods") as HTMLElement;
          reconcilePods(podsBox2, byNode.get(n.metadata.name) || []);

          let ghost = podsBox2.querySelector(".pod.free") as HTMLElement;
          const freeCpu = Math.max(0, aCpu - cpu), freeMem = Math.max(0, aMem - mem);
          const freeShare = (aCpu || aMem) ? Math.max(0, 1 - Math.max(aCpu ? cpu / aCpu : 0, aMem ? mem / aMem : 0)) : 0;
          if (freeShare > 0.04 && !isGone) {
            if (!ghost) {
              ghost = document.createElement("div");
              ghost.className = "pod free";
              ghost.innerHTML = `<span class="pn">free</span><span class="pm"></span>`;
            }
            (ghost.querySelector(".pm") as HTMLElement).textContent = `${fmtCpu(freeCpu)} · ${fmtMem(freeMem)}`;
            ghost.title = "unrequested capacity";
            if (podsBox2.lastElementChild !== ghost) podsBox2.appendChild(ghost);
          } else if (ghost) ghost.remove();

          const cardPos: Element | null = prevNode ? prevNode.nextElementSibling : grid.firstElementChild;
          if (cardPos !== card) grid.insertBefore(card, cardPos);
          prevNode = card;
        }
      }
    }
    firstPaint = false;
    try { sessionStorage.setItem("peepernetes-emptysince", JSON.stringify([...emptySince])); } catch { /* ignore */ }
    try { sessionStorage.setItem("peepernetes-drainsince", JSON.stringify([...drainSince])); } catch { /* ignore */ }
  }

  // render loop: coalesce bursts, repaint ages every 5s
  every(120, () => { if (dirty) { dirty = false; render(); } });
  every(5000, () => { dirty = true; });

  // filters persist across mounts/reloads
  function saveFilters(): void {
    localStorage.setItem("peepernetes-filters", JSON.stringify({
      sys: sysMode(), ns: $("#ns-filter").value, group: $("#group-by").value,
      pools: poolSel === null ? null : poolSel === "all" ? "all" : [...poolSel],
    }));
  }
  try {
    const f = JSON.parse(localStorage.getItem("peepernetes-filters") || "{}");
    $("#sys-mode").value = f.sys === true ? "expand" : ["hide", "dot", "expand"].includes(f.sys) ? f.sys : "hide";
    if (["pool", "ns", "deploy"].includes(f.group)) $("#group-by").value = f.group;
    if (f.pools === "all") poolSel = "all";
    else if (Array.isArray(f.pools)) poolSel = new Set(f.pools);
    if (f.ns) {
      const o = document.createElement("option");
      o.value = o.textContent = f.ns;
      $("#ns-filter").append(o);
      $("#ns-filter").value = f.ns;
    }
  } catch { /* ignore */ }

  const changeHandlers: { id: string; fn: () => void }[] = [];
  for (const id of ["sys-mode", "ns-filter", "group-by"]) {
    const fn = () => { saveFilters(); dirty = true; };
    $("#" + id).addEventListener("change", fn);
    changeHandlers.push({ id, fn });
  }

  // pool picker interactions
  const poolBtnFn = (e: Event) => { e.stopPropagation(); $("#pool-menu").classList.toggle("open"); };
  $("#pool-btn").addEventListener("click", poolBtnFn);
  const docClickFn = (e: Event) => { if (!(e.target as HTMLElement).closest("#pool-pick")) $("#pool-menu").classList.remove("open"); };
  document.addEventListener("click", docClickFn);
  const poolMenuFn = (e: Event) => {
    const t = e.target as HTMLInputElement;
    if (t.id === "pool-all") {
      poolSel = t.checked ? "all" : new Set();
    } else {
      if (poolSel === null) poolSel = new Set(observedPools().filter((p) => p.startsWith("duckgres")));
      else if (poolSel === "all") poolSel = new Set(observedPools());
      if (t.checked) poolSel.add(t.dataset.pool!); else poolSel.delete(t.dataset.pool!);
    }
    saveFilters(); syncPoolMenu(); dirty = true;
  };
  $("#pool-menu").addEventListener("change", poolMenuFn);

  // ── start the polling streams ──────────────────────────
  startPoll(`${API}/nodes`, nodes, onNodeEvent, "nodes", 2000);
  startPoll(`${API}/pods`, pods, onPodEvent, "pods", 2000);
  startPoll(`${API}/events`, evStore, onK8sEvent, null, 4000);

  // ── cleanup ────────────────────────────────────────────
  return () => {
    stopped = true;
    for (const id of timers) clearInterval(id);
    for (const c of controllers) c.abort();
    document.removeEventListener("click", docClickFn);
    setClusterCounts(null); // Topbar drops the node/worker counters when we leave
    root.classList.remove("peeper");
    root.innerHTML = "";
  };
}
