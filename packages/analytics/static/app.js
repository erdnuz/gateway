"use strict";

// Gate live traffic dashboard. One request per refresh to /analytics/overview
// (everything the page shows) and one to /analytics/scaling (Prometheus).

const $ = (id) => document.getElementById(id);
const SVG = "http://www.w3.org/2000/svg";
const REFRESH_MS = 5000;

const OUTCOMES = [
  { key: "ok", label: "OK", color: "--s-ok" },
  { key: "rate_limited", label: "Rate limited (429)", color: "--s-limited" },
  { key: "client_error", label: "Client error (4xx)", color: "--s-client" },
  { key: "upstream_error", label: "Upstream error (5xx)", color: "--s-upstream" },
];

const state = {
  range: "1h",
  service: "",
  tier: "",
  edge: "",
  token: readStorage("gate.analytics.token"),
  hidden: new Set(), // outcome series toggled off in the legend
  sort: { key: "requests", dir: -1 },
  data: null,
  scaling: null,
  updatedAt: 0,
  timer: null,
  inflight: false,
};

// ---------------------------------------------------------------- utilities

function readStorage(key) {
  try { return localStorage.getItem(key) || ""; } catch { return ""; }
}
function writeStorage(key, value) {
  try { value ? localStorage.setItem(key, value) : localStorage.removeItem(key); } catch { /* private mode */ }
}
function cssVar(name) {
  return getComputedStyle(document.body).getPropertyValue(name).trim();
}
function el(tag, attrs = {}, text) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k === "style") node.style.cssText = v;
    else node.setAttribute(k, v);
  }
  if (text !== undefined) node.textContent = text;
  return node;
}
function svg(tag, attrs = {}) {
  const node = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  return node;
}

const fmt = {
  int(v) { return Math.round(v || 0).toLocaleString(); },
  compact(v) {
    const n = Number(v || 0);
    if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(n >= 1e7 ? 0 : 1)}M`;
    if (Math.abs(n) >= 1e4) return `${(n / 1e3).toFixed(n >= 1e5 ? 0 : 1)}K`;
    return Math.round(n).toLocaleString();
  },
  rate(v) {
    const n = Number(v || 0);
    if (n >= 100) return n.toFixed(0);
    if (n >= 10) return n.toFixed(1);
    return n.toFixed(2);
  },
  ms(v) {
    const n = Number(v || 0);
    if (n >= 1000) return `${(n / 1000).toFixed(2)} s`;
    if (n >= 100) return `${n.toFixed(0)} ms`;
    if (n >= 10) return `${n.toFixed(1)} ms`;
    return `${n.toFixed(2)} ms`;
  },
  pct(v, digits = 1) {
    const n = Number(v || 0) * 100;
    if (n > 0 && n < 0.1) return "<0.1%";
    return `${n.toFixed(digits)}%`;
  },
};

function timeLabel(t, bucketSec) {
  const d = new Date(t);
  if (bucketSec >= 3600) return d.toLocaleString(undefined, { weekday: "short", hour: "2-digit", minute: "2-digit" });
  if (bucketSec < 60) return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
}

// niceScale picks a round tick step (1, 2, 2.5 or 5 × 10^n) for about four
// ticks, and the axis maximum as a whole number of steps.
function niceScale(v, ticks = 4) {
  if (!(v > 0)) return { max: 1, step: 0.25 };
  const raw = v / ticks;
  const p = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * p).find((x) => x >= raw);
  return { max: step * Math.ceil(v / step - 1e-9), step };
}

// ---------------------------------------------------------------- data

function query() {
  const p = new URLSearchParams({ range: state.range });
  if (state.service) p.set("service", state.service);
  if (state.tier) p.set("tier", state.tier);
  if (state.edge) p.set("edge_id", state.edge);
  return p.toString();
}

async function getJSON(path) {
  const headers = state.token ? { Authorization: `Bearer ${state.token}` } : {};
  const res = await fetch(path, { headers });
  if (res.status === 401) {
    const err = new Error("unauthorized");
    err.status = 401;
    throw err;
  }
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  return res.json();
}

async function refresh({ showLoading = false } = {}) {
  if (state.inflight) return;
  state.inflight = true;
  if (showLoading) $("main").classList.add("loading");
  try {
    const [overview, scaling] = await Promise.all([
      getJSON(`/analytics/overview?${query()}`),
      getJSON("/analytics/scaling").catch(() => ({ enabled: false })),
    ]);
    state.data = overview;
    state.scaling = scaling;
    state.updatedAt = Date.now();
    $("error").hidden = true;
    $("liveDot").className = "dot on";
    render();
  } catch (err) {
    $("liveDot").className = "dot off";
    if (err.status === 401) {
      $("authHint").hidden = false;
      if (!$("settings").open) $("settings").showModal();
      showError("The analytics API needs a token. Add it in Settings.");
    } else {
      showError(`Could not load analytics (${err.message}). Retrying…`);
    }
  } finally {
    state.inflight = false;
    $("main").classList.remove("loading");
  }
}

function showError(msg) {
  $("error").textContent = msg;
  $("error").hidden = false;
}

// ---------------------------------------------------------------- render

function render() {
  const d = state.data;
  if (!d) return;
  $("empty").hidden = (d.kpis.requests?.value || 0) > 0;
  renderFilters(d.filters);
  renderKPIs(d);
  renderScaling(state.scaling);
  renderTraffic(d);
  renderLatency(d);
  renderTierBars(d.by_tier);
  renderEdgeBars(d.by_edge);
  renderServices(d.by_service);
  renderRecent(d.recent);
}

function renderFilters(filters) {
  const fill = (select, values, current) => {
    const keep = new Set([current, ...values].filter(Boolean));
    const existing = [...select.options].slice(1).map((o) => o.value);
    if (existing.join("\u0000") === [...keep].join("\u0000")) return;
    select.length = 1;
    for (const v of [...keep].sort()) select.add(new Option(v, v));
    select.value = current;
  };
  fill($("fService"), filters.services, state.service);
  fill($("fTier"), filters.tiers, state.tier);
  fill($("fEdge"), filters.edges, state.edge);
}

// KPI tiles: value, delta vs the previous window (coloured by whether the
// direction is good), and a sparkline where a per-interval series exists.
function renderKPIs(d) {
  const k = d.kpis;
  const s = d.series;
  const total = (p) => p.ok + p.rate_limited + p.client_error + p.upstream_error;
  const tiles = [
    {
      hero: true, label: `Requests · last ${d.range}`, kpi: k.requests, value: fmt.compact(k.requests.value),
      meta: `${fmt.rate(k.rps.value)} req/s`, good: 0, spark: s.map(total),
    },
    {
      label: "Success rate", kpi: k.success_rate, value: fmt.pct(k.success_rate.value, 2), points: true, good: +1,
      meta: "not 5xx", spark: s.map((p) => { const n = total(p); return n ? 1 - p.upstream_error / n : null; }),
    },
    {
      label: "p99 latency", kpi: k.p99_ms, value: fmt.ms(k.p99_ms.value), good: -1,
      meta: `p50 ${fmt.ms(k.p50_ms.value)}`, spark: s.map((p) => (total(p) ? p.p99_ms : null)),
    },
    {
      label: "Gateway overhead p90", kpi: k.gateway_overhead_p90_ms, value: fmt.ms(k.gateway_overhead_p90_ms.value), good: -1,
      meta: "time added beyond the upstream",
    },
    {
      label: "Rate limited", kpi: k.rate_limited_share, value: fmt.pct(k.rate_limited_share.value), points: true, good: 0,
      meta: "answered 429", spark: s.map((p) => { const n = total(p); return n ? p.rate_limited / n : null; }),
    },
    {
      label: "Cache hit rate", kpi: k.cache_hit_rate, value: fmt.pct(k.cache_hit_rate.value), points: true, good: +1,
      meta: "served from edge cache",
    },
  ];
  const root = $("kpis");
  root.replaceChildren(...tiles.map(tile));
}

function tile(t) {
  const node = el("article", { class: `tile${t.hero ? " hero" : ""}` });
  node.append(el("span", { class: "label" }, t.label), el("span", { class: "value" }, t.value));
  const meta = el("span", { class: "meta" });
  const delta = deltaOf(t.kpi, t.points, t.good);
  if (delta) meta.append(el("span", { class: `delta ${delta.cls}` }, delta.text), el("span", {}, `vs previous ${state.data.range}`));
  if (t.meta) meta.append(el("span", {}, `· ${t.meta}`));
  node.append(meta);
  // A trend needs a few points; fewer read as stray marks.
  if (t.spark && t.spark.filter((v) => v !== null).length >= 3 && t.spark.some((v) => v > 0)) node.append(sparkline(t.spark));
  return node;
}

function deltaOf(kpi, points, good) {
  const { value: v, previous: p } = kpi;
  if (!p && !v) return null;
  if (!p) return { text: "new", cls: "flat" };
  const change = points ? (v - p) * 100 : ((v - p) / p) * 100;
  if (Math.abs(change) < 0.05) return { text: "±0", cls: "flat" };
  const sign = change > 0 ? "+" : "−";
  const text = points ? `${sign}${Math.abs(change).toFixed(1)} pp` : `${sign}${Math.abs(change).toFixed(change > -10 && change < 10 ? 1 : 0)}%`;
  const cls = good === 0 ? "flat" : (change > 0) === (good > 0) ? "good" : "bad";
  return { text: `${change > 0 ? "▲" : "▼"} ${text}`, cls };
}

function sparkline(values) {
  const w = 160, h = 28, pad = 4;
  const pts = values.map((v, i) => [i, v]).filter(([, v]) => v !== null);
  const max = Math.max(...pts.map(([, v]) => v));
  const min = Math.min(...pts.map(([, v]) => v));
  const x = (i) => pad + (i / Math.max(values.length - 1, 1)) * (w - 2 * pad);
  const y = (v) => h - pad - (max === min ? 0.5 : (v - min) / (max - min)) * (h - 2 * pad);
  const root = svg("svg", { class: "spark", viewBox: `0 0 ${w} ${h}`, preserveAspectRatio: "none", "aria-hidden": "true" });
  root.append(svg("path", { d: pts.map(([i, v], n) => `${n ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(""), "vector-effect": "non-scaling-stroke" }));
  const [li, lv] = pts[pts.length - 1];
  root.append(svg("circle", { cx: x(li), cy: y(lv), r: 3 }));
  return root;
}

// The architecture's point, in one sentence: traffic grows on the edges while
// the hub's share of the work stays tiny.
function renderScaling(sc) {
  const box = $("scaling");
  if (!sc || !sc.enabled) { box.hidden = true; return; }
  box.hidden = false;
  const line = $("scalingLine");
  const stats = $("scalingStats");
  if (sc.error || !sc.values) {
    line.textContent = "Prometheus is configured but not answering yet.";
    stats.replaceChildren();
    return;
  }
  const v = sc.values;
  const hubCalls = (v.hub_lease_rps || 0) + (v.key_lookup_rps || 0);
  const edges = Math.round(v.edges_serving || v.edges_connected || 0);
  const hubs = Math.max(1, Math.round(v.hubs_serving || 0));
  line.replaceChildren(
    el("b", {}, `${fmt.rate(v.edge_rps)} req/s`), document.createTextNode(` served by ${edges} edge${edges === 1 ? "" : "s"}; the hub${hubs === 1 ? "" : "s"} handled `),
    el("b", {}, `${fmt.rate(hubCalls)} calls/s`), document.createTextNode(` (${fmt.pct(sc.hub_work_per_request)} of requests). `),
    document.createTextNode("Edges spend leased quota locally, so hub work grows with leases, not requests."),
  );
  const items = [
    ["Edges serving", String(edges)],
    ["Edges connected to hubs", String(Math.round(v.edges_connected || 0))],
    ["Hub lease RPCs/s", fmt.rate(v.hub_lease_rps)],
    ["Hub work per request", fmt.pct(sc.hub_work_per_request, 2)],
    ["Quota exhausted (WAITING)/s", fmt.rate(v.waiting_rps)],
  ];
  stats.replaceChildren(...items.map(([label, value]) => {
    const div = el("div");
    div.append(el("dt", {}, label), el("dd", {}, value));
    return div;
  }));
}

// ---------------------------------------------------------------- charts

function frame(container, height) {
  const width = Math.max(container.clientWidth, 280);
  const m = { top: 8, right: 8, bottom: 22, left: 48 };
  const root = svg("svg", { viewBox: `0 0 ${width} ${height}`, role: "img" });
  container.replaceChildren(root);
  return { root, m, w: width - m.left - m.right, h: height - m.top - m.bottom, width, height };
}

function yAxis(f, scale, format) {
  const { max, step } = scale;
  const g = svg("g", { class: "grid" });
  const ticks = Math.round(max / step);
  for (let i = 0; i <= ticks; i++) {
    const v = step * i;
    const y = f.m.top + f.h - (v / max) * f.h;
    if (i > 0) g.append(svg("line", { x1: f.m.left, x2: f.m.left + f.w, y1: y, y2: y }));
    const t = svg("text", { x: f.m.left - 8, y: y + 4, "text-anchor": "end", class: "tick" });
    t.textContent = format(v);
    f.root.append(t);
  }
  f.root.prepend(g);
  f.root.append(svg("line", { class: "baseline", x1: f.m.left, x2: f.m.left + f.w, y1: f.m.top + f.h, y2: f.m.top + f.h }));
}

function xAxis(f, series, bucketSec, xOf) {
  const n = series.length;
  const want = Math.max(2, Math.floor(f.w / 110));
  const step = Math.max(1, Math.ceil(n / want));
  for (let i = 0; i < n; i += step) {
    const t = svg("text", { x: xOf(i), y: f.height - 4, "text-anchor": "middle", class: "tick" });
    t.textContent = timeLabel(series[i].t, bucketSec);
    f.root.append(t);
  }
}

// Stacked columns per interval, one per outcome, with a 2px surface gap
// between segments and a 4px rounded top.
function renderTraffic(d) {
  const series = d.series;
  const shown = OUTCOMES.filter((o) => !state.hidden.has(o.key));
  $("trafficSub").textContent = `Requests per ${bucketName(d.bucket_seconds)}, last ${d.range}`;
  renderLegend($("trafficLegend"), OUTCOMES, true);

  const container = $("trafficChart");
  const f = frame(container, container.clientHeight || 260);
  const totals = series.map((p) => shown.reduce((s, o) => s + p[o.key], 0));
  const scale = niceScale(Math.max(...totals, 1));
  const max = scale.max;
  yAxis(f, scale, fmt.compact);
  const band = f.w / Math.max(series.length, 1);
  const barW = Math.max(1, Math.min(24, band * 0.7));
  const xOf = (i) => f.m.left + band * i + band / 2;
  xAxis(f, series, d.bucket_seconds, xOf);
  const yOf = (v) => (v / max) * f.h;
  const gap = barW >= 6 ? 2 : 0;

  series.forEach((p, i) => {
    const g = svg("g", { class: "col-group" });
    let base = f.m.top + f.h;
    const visible = shown.filter((o) => p[o.key] > 0);
    visible.forEach((o, j) => {
      const hgt = yOf(p[o.key]);
      const top = j === visible.length - 1;
      const y = base - hgt;
      const segH = Math.max(0, hgt - (j > 0 ? gap : 0));
      const x = xOf(i) - barW / 2;
      g.append(svg("path", { class: "seg", d: columnPath(x, y, barW, segH, top ? Math.min(4, barW / 2, segH) : 0), fill: cssVar(o.color) }));
      base = y;
    });
    // The hit target is the whole interval band, not the painted pixels.
    const hit = svg("rect", { class: "col-hit", x: f.m.left + band * i, y: f.m.top, width: band, height: f.h, tabindex: -1 });
    hit.addEventListener("pointermove", (ev) => {
      g.classList.add("hover");
      showTooltip(ev, timeLabel(p.t, d.bucket_seconds), [
        ...OUTCOMES.map((o) => ({ color: cssVar(o.color), value: fmt.int(p[o.key]), label: o.label })).reverse(),
        { value: fmt.int(totals[i]), label: "total shown" },
      ]);
    });
    hit.addEventListener("pointerleave", () => { g.classList.remove("hover"); hideTooltip(); });
    g.append(hit);
    f.root.append(g);
  });
  f.root.setAttribute("aria-label", `Requests per interval by outcome, last ${d.range}`);
  renderTrafficTable(d);
}

function columnPath(x, y, w, h, r) {
  if (h <= 0) return "";
  r = Math.max(0, Math.min(r, h));
  return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z`;
}

function bucketName(sec) {
  if (sec >= 3600) return `${sec / 3600} h`;
  if (sec >= 60) return `${sec / 60} min`;
  return `${sec} s`;
}

function renderTrafficTable(d) {
  const table = el("table", { class: "data" });
  const head = el("tr");
  head.append(el("th", {}, "Interval"), ...OUTCOMES.map((o) => el("th", { class: "num" }, o.label)), el("th", { class: "num" }, "p50"), el("th", { class: "num" }, "p99"));
  table.append(head);
  for (const p of [...d.series].reverse()) {
    if (!(p.ok + p.rate_limited + p.client_error + p.upstream_error)) continue;
    const tr = el("tr");
    tr.append(el("td", {}, timeLabel(p.t, d.bucket_seconds)), ...OUTCOMES.map((o) => el("td", { class: "num" }, fmt.int(p[o.key]))),
      el("td", { class: "num" }, fmt.ms(p.p50_ms)), el("td", { class: "num" }, fmt.ms(p.p99_ms)));
    table.append(tr);
  }
  const wrap = el("div", { class: "table-wrap" });
  wrap.append(table);
  $("trafficTable").replaceChildren(wrap);
}

// p50 and p99 on one axis (same unit), with a snapping crosshair.
function renderLatency(d) {
  const lines = [
    { key: "p50_ms", label: "p50", color: "--s-p50" },
    { key: "p99_ms", label: "p99", color: "--s-p99" },
  ];
  renderLegend($("latencyLegend"), lines, false);
  const series = d.series;
  const active = series.map((p) => p.ok + p.rate_limited + p.client_error + p.upstream_error > 0);
  const container = $("latencyChart");
  const f = frame(container, container.clientHeight || 200);
  const scale = niceScale(Math.max(...series.map((p) => p.p99_ms), 1));
  const max = scale.max;
  yAxis(f, scale, (v) => fmt.ms(v).replace(" ", ""));
  const n = series.length;
  const xOf = (i) => f.m.left + (n <= 1 ? f.w / 2 : (i / (n - 1)) * f.w);
  const yOf = (v) => f.m.top + f.h - (v / max) * f.h;
  xAxis(f, series, d.bucket_seconds, xOf);

  for (const l of lines) {
    // Gaps where an interval had no traffic, rather than a fake zero.
    let path = "";
    let pen = false;
    series.forEach((p, i) => {
      if (!active[i]) { pen = false; return; }
      path += `${pen ? "L" : "M"}${xOf(i).toFixed(1)},${yOf(p[l.key]).toFixed(1)}`;
      pen = true;
    });
    f.root.append(svg("path", { class: "series-line", d: path, stroke: cssVar(l.color) }));
    const last = active.lastIndexOf(true);
    if (last >= 0) f.root.append(svg("circle", { class: "end-dot", cx: xOf(last), cy: yOf(series[last][l.key]), r: 4, fill: cssVar(l.color) }));
  }

  const cross = svg("line", { class: "crosshair", y1: f.m.top, y2: f.m.top + f.h, visibility: "hidden" });
  const hit = svg("rect", { x: f.m.left, y: f.m.top, width: f.w, height: f.h, fill: "transparent" });
  f.root.append(cross, hit);
  hit.addEventListener("pointermove", (ev) => {
    const box = f.root.getBoundingClientRect();
    const px = ((ev.clientX - box.left) / box.width) * f.width;
    const i = Math.max(0, Math.min(n - 1, Math.round(((px - f.m.left) / f.w) * (n - 1))));
    cross.setAttribute("x1", xOf(i));
    cross.setAttribute("x2", xOf(i));
    cross.setAttribute("visibility", "visible");
    const p = series[i];
    showTooltip(ev, timeLabel(p.t, d.bucket_seconds), active[i]
      ? lines.map((l) => ({ color: cssVar(l.color), value: fmt.ms(p[l.key]), label: l.label })).reverse()
      : [{ value: "no traffic", label: "" }]);
  });
  hit.addEventListener("pointerleave", () => { cross.setAttribute("visibility", "hidden"); hideTooltip(); });
  f.root.setAttribute("aria-label", "Latency p50 and p99 per interval");
}

function renderLegend(root, items, toggles) {
  root.replaceChildren(...items.map((it) => {
    const li = el("li");
    const swatch = el("span", { class: toggles ? "sw" : "ln", style: `background: var(${it.color})` });
    if (!toggles) { li.append(swatch, document.createTextNode(it.label)); return li; }
    const b = el("button", { type: "button", "aria-pressed": String(!state.hidden.has(it.key)) });
    b.append(swatch, document.createTextNode(it.label));
    b.addEventListener("click", () => {
      state.hidden.has(it.key) ? state.hidden.delete(it.key) : state.hidden.add(it.key);
      renderTraffic(state.data);
    });
    li.append(b);
    return li;
  }));
}

// ---------------------------------------------------------------- bars & tables

function renderTierBars(rows) {
  const root = $("tierBars");
  renderLegend($("tierLegend"), [
    { label: "Allowed", color: "--s-ok" },
    { label: "Rate limited", color: "--s-limited" },
  ], false);
  $("tierLegend").querySelectorAll(".ln").forEach((s) => (s.className = "sw"));
  if (!rows.length) { root.replaceChildren(el("p", { class: "empty-note" }, "No traffic in this range.")); return; }
  const max = Math.max(...rows.map((r) => r.requests));
  root.replaceChildren(...rows.map((r) => {
    const limited = Math.round(r.requests * r.rate_limited_share);
    const allowed = r.requests - limited;
    const row = el("div", { class: "bar-row" });
    const track = el("div", { class: "track" });
    const seg = (n, color, label) => {
      if (n <= 0) return;
      const s = el("span", { style: `width:${(n / max) * 100}%; background: var(${color})` });
      s.addEventListener("pointermove", (ev) => showTooltip(ev, `Tier ${r.name}`, [{ color: cssVar(color), value: fmt.int(n), label }]));
      s.addEventListener("pointerleave", hideTooltip);
      track.append(s);
    };
    seg(allowed, "--s-ok", "allowed");
    seg(limited, "--s-limited", "rate limited (429)");
    row.append(el("span", { class: "name", title: r.name }, r.name), track,
      el("span", { class: "val" }, `${fmt.compact(r.requests)} · ${fmt.pct(r.rate_limited_share)} 429`));
    return row;
  }));
}

function renderEdgeBars(rows) {
  const root = $("edgeBars");
  if (!rows.length) { root.replaceChildren(el("p", { class: "empty-note" }, "No traffic in this range.")); return; }
  const max = Math.max(...rows.map((r) => r.share));
  root.replaceChildren(...rows.map((r) => {
    const row = el("div", { class: "bar-row" });
    const track = el("div", { class: "track" });
    const s = el("span", { style: `width:${(r.share / max) * 100}%; background: var(--s-ok)` });
    s.addEventListener("pointermove", (ev) => showTooltip(ev, r.name, [
      { color: cssVar("--s-ok"), value: fmt.int(r.requests), label: "requests" },
      { value: fmt.ms(r.p90_ms), label: "p90 latency" },
    ]));
    s.addEventListener("pointerleave", hideTooltip);
    track.append(s);
    row.append(el("span", { class: "name", title: r.name }, r.name), track, el("span", { class: "val" }, `${fmt.pct(r.share)} · ${fmt.compact(r.requests)}`));
    return row;
  }));
}

const SERVICE_COLUMNS = [
  { key: "name", label: "Service", text: (r) => r.name },
  { key: "requests", label: "Requests", num: true, text: (r) => fmt.int(r.requests), bar: true },
  { key: "error_share", label: "Errors", num: true, text: (r) => fmt.pct(r.error_share) },
  { key: "rate_limited_share", label: "429s", num: true, text: (r) => fmt.pct(r.rate_limited_share) },
  { key: "p90_ms", label: "p90", num: true, text: (r) => fmt.ms(r.p90_ms) },
  { key: "cache_hit_rate", label: "Cache hits", num: true, text: (r) => fmt.pct(r.cache_hit_rate) },
];

function renderServices(rows) {
  const table = $("services");
  const { key, dir } = state.sort;
  const sorted = [...rows].sort((a, b) => (typeof a[key] === "string" ? a[key].localeCompare(b[key]) : a[key] - b[key]) * dir);
  const max = Math.max(1, ...rows.map((r) => r.requests));
  const head = el("tr");
  for (const c of SERVICE_COLUMNS) {
    const arrow = c.key === key ? (dir > 0 ? " ↑" : " ↓") : "";
    const th = el("th", { class: `sortable${c.num ? " num" : ""}`, "aria-sort": c.key === key ? (dir > 0 ? "ascending" : "descending") : "none" }, c.label + arrow);
    th.addEventListener("click", () => {
      state.sort = { key: c.key, dir: c.key === key ? -dir : c.num ? -1 : 1 };
      renderServices(rows);
    });
    head.append(th);
  }
  const body = sorted.map((r) => {
    const tr = el("tr");
    for (const c of SERVICE_COLUMNS) {
      const td = el("td", { class: c.num ? "num" : "" }, c.text(r));
      if (c.bar) td.append(el("span", { class: "inline-bar", style: `width:${Math.max(2, (r.requests / max) * 60)}px` }));
      tr.append(td);
    }
    return tr;
  });
  if (!body.length) {
    const tr = el("tr");
    tr.append(el("td", { colspan: SERVICE_COLUMNS.length, class: "empty-note" }, "No traffic in this range."));
    body.push(tr);
  }
  table.replaceChildren(head, ...body);
}

function statusColor(code) {
  if (code === 429) return "--s-limited";
  if (code >= 500) return "--s-upstream";
  if (code >= 400) return "--s-client";
  return "--s-ok";
}

function renderRecent(rows) {
  const table = $("recent");
  const head = el("tr");
  for (const [label, num] of [["Time"], ["Method"], ["Service"], ["Tier"], ["Edge"], ["Status"], ["Latency", true], ["Cache"]]) {
    head.append(el("th", { class: num ? "num" : "" }, label));
  }
  const body = rows.map((r) => {
    const tr = el("tr");
    const status = el("span", { class: "pill", style: `--c: var(${statusColor(r.status)})` }, String(r.status || "–"));
    const st = el("td");
    st.append(status);
    tr.append(
      el("td", {}, new Date(r.time).toLocaleTimeString()), el("td", {}, r.method), el("td", {}, r.service),
      el("td", {}, r.tier || "–"), el("td", {}, r.edge_id || "–"), st,
      el("td", { class: "num" }, fmt.ms(r.latency_ms)), el("td", {}, r.cache_hit ? "hit" : ""),
    );
    return tr;
  });
  if (!body.length) {
    const tr = el("tr");
    tr.append(el("td", { colspan: 8, class: "empty-note" }, "Nothing yet."));
    body.push(tr);
  }
  table.replaceChildren(head, ...body);
}

// ---------------------------------------------------------------- tooltip

function showTooltip(ev, title, rows) {
  const tip = $("tooltip");
  const nodes = [el("div", { class: "tt-title" }, title)];
  for (const r of rows) {
    const row = el("div", { class: "tt-row" });
    row.append(r.color ? el("span", { class: "key", style: `background:${r.color}` }) : el("span"), el("b", {}, r.value), el("span", {}, r.label));
    nodes.push(row);
  }
  tip.replaceChildren(...nodes);
  tip.hidden = false;
  const pad = 14;
  const { innerWidth: W, innerHeight: H } = window;
  const rect = tip.getBoundingClientRect();
  let x = ev.clientX + pad;
  let y = ev.clientY + pad;
  if (x + rect.width > W - 8) x = ev.clientX - rect.width - pad;
  if (y + rect.height > H - 8) y = ev.clientY - rect.height - pad;
  tip.style.left = `${x}px`;
  tip.style.top = `${y}px`;
}

function hideTooltip() { $("tooltip").hidden = true; }

// ---------------------------------------------------------------- wiring

function readHash() {
  const p = new URLSearchParams(location.hash.slice(1));
  state.range = p.get("range") || state.range;
  state.service = p.get("service") || "";
  state.tier = p.get("tier") || "";
  state.edge = p.get("edge") || "";
}

function writeHash() {
  const p = new URLSearchParams({ range: state.range });
  if (state.service) p.set("service", state.service);
  if (state.tier) p.set("tier", state.tier);
  if (state.edge) p.set("edge", state.edge);
  history.replaceState(null, "", `#${p}`);
}

function syncControls() {
  for (const b of $("ranges").querySelectorAll("button")) b.setAttribute("aria-checked", String(b.dataset.range === state.range));
  $("fService").value = state.service;
  $("fTier").value = state.tier;
  $("fEdge").value = state.edge;
}

function changed() {
  writeHash();
  syncControls();
  refresh({ showLoading: true });
}

function schedule() {
  clearInterval(state.timer);
  state.timer = setInterval(() => { if (!document.hidden) refresh(); }, REFRESH_MS);
}

function tickUpdated() {
  if (!state.updatedAt) return;
  const s = Math.round((Date.now() - state.updatedAt) / 1000);
  $("updated").textContent = s < 2 ? "live · updated just now" : `live · updated ${s}s ago`;
}

async function setupSettings() {
  const dialog = $("settings");
  $("settingsBtn").addEventListener("click", () => { $("token").value = state.token; dialog.showModal(); });
  dialog.addEventListener("close", () => {
    if (dialog.returnValue !== "save") return;
    state.token = $("token").value.replace(/^Bearer\s+/i, "").trim();
    writeStorage("gate.analytics.token", state.token);
    refresh({ showLoading: true });
  });
  try {
    const features = await (await fetch("/analytics/features")).json();
    if (features.testing_clear_enabled) {
      const btn = $("clearBtn");
      btn.hidden = false;
      btn.addEventListener("click", async () => {
        if (!confirm("Delete all analytics events?")) return;
        await fetch("/analytics/clear", { method: "POST", headers: state.token ? { Authorization: `Bearer ${state.token}` } : {} });
        dialog.close();
        refresh({ showLoading: true });
      });
    }
  } catch { /* optional */ }
}

function boot() {
  readHash();
  syncControls();
  $("ranges").addEventListener("click", (ev) => {
    const r = ev.target.closest("button")?.dataset.range;
    if (r && r !== state.range) { state.range = r; changed(); }
  });
  for (const [id, field] of [["fService", "service"], ["fTier", "tier"], ["fEdge", "edge"]]) {
    $(id).addEventListener("change", (ev) => { state[field] = ev.target.value; changed(); });
  }
  window.addEventListener("hashchange", () => { readHash(); syncControls(); refresh({ showLoading: true }); });
  document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });
  let resizeTimer;
  new ResizeObserver(() => { clearTimeout(resizeTimer); resizeTimer = setTimeout(render, 120); }).observe($("main"));
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", render);
  setupSettings();
  refresh({ showLoading: true });
  schedule();
  setInterval(tickUpdated, 1000);
}

boot();
