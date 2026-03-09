const byId = (id) => document.getElementById(id);

const state = {
  summary: null,
  groupedByService: {},
  groupedByPrefix: {},
  events: [],
};

function getConfig() {
  const defaultBase = `${window.location.protocol}//${window.location.host}`;
  return {
    base: (byId("apiBase").value || defaultBase).replace(/\/$/, ""),
    token: byId("token").value.trim(),
    limit: byId("limit").value || "500",
    service: byId("service").value.trim(),
    prefix: byId("prefix").value.trim(),
    tier: byId("tier").value.trim(),
    method: byId("method").value.trim(),
    responseCodeMin: byId("responseCodeMin").value.trim(),
    responseCodeMax: byId("responseCodeMax").value.trim(),
    cacheHit: byId("cacheHit").value.trim(),
    upstreamError: byId("upstreamError").value.trim(),
    totalLatencyMin: byId("totalLatencyMin").value.trim(),
    totalLatencyMax: byId("totalLatencyMax").value.trim(),
  };
}

function toQS(cfg) {
  const p = new URLSearchParams({ limit: cfg.limit });
  if (cfg.service) p.set("service", cfg.service);
  if (cfg.prefix) p.set("prefix", cfg.prefix);
  if (cfg.tier) p.set("tier", cfg.tier);
  if (cfg.method) p.set("method", cfg.method);
  if (cfg.responseCodeMin) p.set("response_code_min", cfg.responseCodeMin);
  if (cfg.responseCodeMax) p.set("response_code_max", cfg.responseCodeMax);
  if (cfg.cacheHit) p.set("cache_hit", cfg.cacheHit);
  if (cfg.upstreamError) p.set("upstream_error", cfg.upstreamError);
  if (cfg.totalLatencyMin) p.set("total_latency_ms_min", cfg.totalLatencyMin);
  if (cfg.totalLatencyMax) p.set("total_latency_ms_max", cfg.totalLatencyMax);
  return p;
}

async function callAPI(path, cfg) {
  const headers = {};
  if (cfg.token) headers.Authorization = `Bearer ${cfg.token}`;
  const res = await fetch(`${cfg.base}${path}`, { headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json();
}

function fmtMs(v) {
  return `${Number(v || 0).toFixed(1)} ms`;
}

function fmtBytes(v) {
  const n = Number(v || 0);
  if (n < 1024) return `${n.toFixed(0)} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

function fmtPct(part, total) {
  if (!total) return "0.0%";
  return `${((part / total) * 100).toFixed(1)}%`;
}

function setStatus(msg, bad = false) {
  const el = byId("status");
  el.textContent = msg;
  el.style.color = bad ? "#f46060" : "#99b6c8";
}

function renderStats(summary) {
  const cards = [
    ["Requests", String(summary.count || 0), ""],
    ["Avg Total", fmtMs(summary.avg_total_latency_ms), ""],
    ["P95 Rate Limiter", fmtMs(summary.p95_rate_limiter_latency_ms), ""],
    ["Avg Upstream", fmtMs(summary.avg_upstream_latency_ms), ""],
    ["Avg Limiter", fmtMs(summary.avg_rate_limiter_latency_ms), ""],
    ["Cache Hit Rate", `${Number(summary.cache_hit_rate_pct || 0).toFixed(1)}%`, Number(summary.cache_hit_rate_pct || 0) > 50 ? "ok" : ""],
    ["2xx Success Rate", `${Number(summary.success_rate_pct || 0).toFixed(1)}%`, Number(summary.success_rate_pct || 0) >= 95 ? "ok" : ""],
    ["Avg Request Size", fmtBytes(summary.avg_request_size_bytes), ""],
    ["Avg Response Size", fmtBytes(summary.avg_response_size_bytes), ""],
    ["Active Tiers", String(summary.active_tiers_count || 0), ""],
    [
      "Rate Limited",
      `${summary.rate_limited_count || 0} (${fmtPct(summary.rate_limited_count || 0, summary.count || 0)})`,
      (summary.rate_limited_count || 0) > 0 ? "bad" : "ok",
    ],
    [
      "Upstream Errors",
      `${summary.upstream_error_count || 0} (${fmtPct(summary.upstream_error_count || 0, summary.count || 0)})`,
      (summary.upstream_error_count || 0) > 0 ? "bad" : "ok",
    ],
  ];

  byId("stats").innerHTML = cards
    .map(([label, value, level]) => `
      <article class="card">
        <div class="label">${label}</div>
        <div class="value ${level}">${value}</div>
      </article>
    `)
    .join("");
}

function clearSVG(svg) {
  while (svg.firstChild) {
    svg.removeChild(svg.firstChild);
  }
}

function textNode(x, y, value, fill = "#99b6c8", size = 12, anchor = "start") {
  const t = document.createElementNS("http://www.w3.org/2000/svg", "text");
  t.setAttribute("x", x);
  t.setAttribute("y", y);
  t.setAttribute("fill", fill);
  t.setAttribute("font-size", String(size));
  t.setAttribute("text-anchor", anchor);
  t.textContent = value;
  return t;
}

function drawBarChart(svgId, points, metric, color) {
  const svg = byId(svgId);
  clearSVG(svg);
  if (!points.length) {
    svg.appendChild(textNode(450, 130, "No data", "#99b6c8", 14, "middle"));
    return;
  }

  const width = 900;
  const height = 260;
  const pad = { top: 16, right: 12, bottom: 58, left: 44 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;
  const max = Math.max(...points.map((p) => p[metric]), 1);
  const barW = innerW / points.length;

  for (let y = 0; y <= 4; y += 1) {
    const gy = pad.top + (innerH / 4) * y;
    const grid = document.createElementNS("http://www.w3.org/2000/svg", "line");
    grid.setAttribute("x1", pad.left);
    grid.setAttribute("y1", gy);
    grid.setAttribute("x2", width - pad.right);
    grid.setAttribute("y2", gy);
    grid.setAttribute("stroke", "rgba(153, 182, 200, 0.20)");
    grid.setAttribute("stroke-width", "1");
    svg.appendChild(grid);
  }

  for (let i = 0; i < points.length; i += 1) {
    const p = points[i];
    const val = p[metric];
    const h = (val / max) * innerH;
    const x = pad.left + i * barW + 6;
    const y = height - pad.bottom - h;

    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x", x);
    rect.setAttribute("y", y);
    rect.setAttribute("width", Math.max(6, barW - 12));
    rect.setAttribute("height", h);
    rect.setAttribute("rx", 5);
    rect.setAttribute("fill", color);
    rect.setAttribute("opacity", "0.92");
    svg.appendChild(rect);

    const label = p.key.length > 12 ? `${p.key.slice(0, 10)}..` : p.key;
    const lx = x + Math.max(6, barW - 12) / 2;
    svg.appendChild(textNode(lx, height - 36, label, "#c6dae8", 11, "middle"));
    svg.appendChild(textNode(lx, y - 4, Number(val).toFixed(0), "#eaf6ff", 11, "middle"));
  }

  svg.appendChild(textNode(8, 18, metric, "#99b6c8", 12));
}

function drawTimeline(svgId, events) {
  const svg = byId(svgId);
  clearSVG(svg);
  if (!events.length) {
    svg.appendChild(textNode(450, 130, "No events in selected window", "#99b6c8", 14, "middle"));
    return;
  }

  const width = 900;
  const height = 260;
  const pad = { top: 18, right: 10, bottom: 34, left: 42 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;
  const valuesTotal = events.map((e) => Number(e.total_latency_ms || 0));
  const valuesLimiter = events.map((e) => Math.max(Number(e.total_latency_ms || 0) - Number(e.upstream_latency_ms || 0), 0));
  const max = Math.max(...valuesTotal, ...valuesLimiter, 1);

  for (let y = 0; y <= 4; y += 1) {
    const gy = pad.top + (innerH / 4) * y;
    const grid = document.createElementNS("http://www.w3.org/2000/svg", "line");
    grid.setAttribute("x1", pad.left);
    grid.setAttribute("y1", gy);
    grid.setAttribute("x2", width - pad.right);
    grid.setAttribute("y2", gy);
    grid.setAttribute("stroke", "rgba(153, 182, 200, 0.20)");
    grid.setAttribute("stroke-width", "1");
    svg.appendChild(grid);
  }

  const area = document.createElementNS("http://www.w3.org/2000/svg", "path");
  let areaD = "";
  for (let i = 0; i < valuesTotal.length; i += 1) {
    const x = pad.left + (i / Math.max(valuesTotal.length - 1, 1)) * innerW;
    const y = pad.top + innerH - (valuesTotal[i] / max) * innerH;
    areaD += `${i === 0 ? "M" : "L"}${x} ${y} `;
  }
  areaD += `L${pad.left + innerW} ${pad.top + innerH} L${pad.left} ${pad.top + innerH} Z`;
  area.setAttribute("d", areaD.trim());
  area.setAttribute("fill", "rgba(48, 213, 200, 0.14)");
  svg.appendChild(area);

  const pathTotal = document.createElementNS("http://www.w3.org/2000/svg", "path");
  let totalD = "";
  for (let i = 0; i < valuesTotal.length; i += 1) {
    const x = pad.left + (i / Math.max(valuesTotal.length - 1, 1)) * innerW;
    const y = pad.top + innerH - (valuesTotal[i] / max) * innerH;
    totalD += `${i === 0 ? "M" : "L"}${x} ${y} `;
  }
  pathTotal.setAttribute("d", totalD.trim());
  pathTotal.setAttribute("fill", "none");
  pathTotal.setAttribute("stroke", "#30d5c8");
  pathTotal.setAttribute("stroke-width", "2.5");
  svg.appendChild(pathTotal);

  const pathLimiter = document.createElementNS("http://www.w3.org/2000/svg", "path");
  let limiterD = "";
  for (let i = 0; i < valuesLimiter.length; i += 1) {
    const x = pad.left + (i / Math.max(valuesLimiter.length - 1, 1)) * innerW;
    const y = pad.top + innerH - (valuesLimiter[i] / max) * innerH;
    limiterD += `${i === 0 ? "M" : "L"}${x} ${y} `;
  }
  pathLimiter.setAttribute("d", limiterD.trim());
  pathLimiter.setAttribute("fill", "none");
  pathLimiter.setAttribute("stroke", "#f4a261");
  pathLimiter.setAttribute("stroke-width", "2");
  pathLimiter.setAttribute("stroke-dasharray", "6 4");
  svg.appendChild(pathLimiter);

  svg.appendChild(textNode(8, 18, "total vs limiter latency (ms)", "#99b6c8", 12));
  svg.appendChild(textNode(44, height - 8, "older", "#99b6c8", 11));
  svg.appendChild(textNode(width - 14, height - 8, "newer", "#99b6c8", 11, "end"));
  svg.appendChild(textNode(44, 26, `max ${max.toFixed(1)} ms`, "#99b6c8", 11));
  svg.appendChild(textNode(width - 14, 18, "total", "#30d5c8", 11, "end"));
  svg.appendChild(textNode(width - 14, 34, "limiter", "#f4a261", 11, "end"));
}

function buildBuckets(events, bucketCount = 24) {
  if (!events.length) return [];
  const count = Math.min(bucketCount, events.length);
  const buckets = new Array(count).fill(null).map(() => []);
  for (let i = 0; i < events.length; i += 1) {
    const idx = Math.min(count - 1, Math.floor((i / Math.max(events.length, 1)) * count));
    buckets[idx].push(events[i]);
  }
  return buckets;
}

function drawStatusTimeline(svgId, events) {
  const svg = byId(svgId);
  clearSVG(svg);
  if (!events.length) {
    svg.appendChild(textNode(450, 130, "No status samples", "#99b6c8", 14, "middle"));
    return;
  }

  const buckets = buildBuckets(events, 24);
  const values2xx = buckets.map((b) => b.filter((e) => Number(e.response_code) >= 200 && Number(e.response_code) < 300).length);
  const values4xx = buckets.map((b) => b.filter((e) => Number(e.response_code) >= 400 && Number(e.response_code) < 500).length);
  const values5xx = buckets.map((b) => b.filter((e) => Number(e.response_code) >= 500).length);

  const width = 900;
  const height = 260;
  const pad = { top: 18, right: 10, bottom: 34, left: 42 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;
  const max = Math.max(...values2xx, ...values4xx, ...values5xx, 1);

  const drawLine = (values, stroke, dash = "") => {
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    let d = "";
    for (let i = 0; i < values.length; i += 1) {
      const x = pad.left + (i / Math.max(values.length - 1, 1)) * innerW;
      const y = pad.top + innerH - (values[i] / max) * innerH;
      d += `${i === 0 ? "M" : "L"}${x} ${y} `;
    }
    path.setAttribute("d", d.trim());
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", stroke);
    path.setAttribute("stroke-width", "2.2");
    if (dash) path.setAttribute("stroke-dasharray", dash);
    svg.appendChild(path);
  };

  drawLine(values2xx, "#4ad67d");
  drawLine(values4xx, "#f4a261", "6 4");
  drawLine(values5xx, "#f46060");
  svg.appendChild(textNode(8, 18, "status counts over time", "#99b6c8", 12));
  svg.appendChild(textNode(width - 14, 18, "2xx", "#4ad67d", 11, "end"));
  svg.appendChild(textNode(width - 14, 34, "4xx", "#f4a261", 11, "end"));
  svg.appendChild(textNode(width - 14, 50, "5xx", "#f46060", 11, "end"));
}

function drawSizeTimeline(svgId, events) {
  const svg = byId(svgId);
  clearSVG(svg);
  if (!events.length) {
    svg.appendChild(textNode(450, 130, "No payload samples", "#99b6c8", 14, "middle"));
    return;
  }

  const buckets = buildBuckets(events, 24);
  const req = buckets.map((b) => {
    if (!b.length) return 0;
    return b.reduce((sum, e) => sum + Number(e.request_size_bytes || 0), 0) / b.length;
  });
  const resp = buckets.map((b) => {
    if (!b.length) return 0;
    return b.reduce((sum, e) => sum + Number(e.response_size_bytes || 0), 0) / b.length;
  });

  const width = 900;
  const height = 260;
  const pad = { top: 18, right: 10, bottom: 34, left: 42 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;
  const max = Math.max(...req, ...resp, 1);

  const drawLine = (values, stroke, dash = "") => {
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    let d = "";
    for (let i = 0; i < values.length; i += 1) {
      const x = pad.left + (i / Math.max(values.length - 1, 1)) * innerW;
      const y = pad.top + innerH - (values[i] / max) * innerH;
      d += `${i === 0 ? "M" : "L"}${x} ${y} `;
    }
    path.setAttribute("d", d.trim());
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", stroke);
    path.setAttribute("stroke-width", "2.2");
    if (dash) path.setAttribute("stroke-dasharray", dash);
    svg.appendChild(path);
  };

  drawLine(req, "#30d5c8");
  drawLine(resp, "#9ab6ff", "6 4");
  svg.appendChild(textNode(8, 18, "avg payload bytes over time", "#99b6c8", 12));
  svg.appendChild(textNode(width - 14, 18, "request", "#30d5c8", 11, "end"));
  svg.appendChild(textNode(width - 14, 34, "response", "#9ab6ff", 11, "end"));
}

function summarizeGroupMap(mapObj) {
  return Object.entries(mapObj)
    .map(([key, v]) => ({ key, ...v }))
    .sort((a, b) => (b.count || 0) - (a.count || 0));
}

function normalizeEvents(rawEvents) {
  return rawEvents.map((e) => ({
    ...e,
    total_latency_ms: e.total_latency_ms ?? (Number(e.total_latency || 0) / 1_000_000),
    upstream_latency_ms: e.upstream_latency_ms ?? (Number(e.upstream_latency || 0) / 1_000_000),
  }));
}

function fmtTimestamp(value) {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString();
}

function fmtRatio(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "0.00";
  return Number(value).toFixed(2);
}

function fmtDurationFromNsToMs(value) {
  return `${(Number(value || 0) / 1_000_000).toFixed(3)} ms`;
}

function renderEventsTable(events) {
  const tbody = byId("eventsTable");
  if (!events.length) {
    tbody.innerHTML = '<tr><td colspan="14">No events in selected window</td></tr>';
    return;
  }

  tbody.innerHTML = events
    .slice(-100)
    .reverse()
    .map(
      (e) => `
      <tr>
        <td>${fmtTimestamp(e.timestamp)}</td>
        <td>${e.prefix || "-"}</td>
        <td>${e.service || "-"}</td>
        <td>${e.method || "-"}</td>
        <td>${e.tier || "-"}</td>
        <td>${fmtDurationFromNsToMs(e.total_latency)}</td>
        <td>${fmtDurationFromNsToMs(e.upstream_latency)}</td>
        <td>${e.cache_hit ? "true" : "false"}</td>
        <td>${Number(e.limit_used || 0)}</td>
        <td>${fmtRatio(e.limit_used_of_total)}</td>
        <td>${Number(e.request_size_bytes || 0)}</td>
        <td>${Number(e.response_size_bytes || 0)}</td>
        <td>${Number(e.response_code || 0)}</td>
        <td>${e.upstream_error ? "true" : "false"}</td>
      </tr>
    `,
    )
    .join("");
}

function renderSlowTable(events) {
  const buckets = new Map();
  for (const e of events) {
    const key = `${e.service || "-"}|${e.prefix || "-"}|${e.method || "-"}`;
    const item = buckets.get(key) || {
      service: e.service || "-",
      prefix: e.prefix || "-",
      method: e.method || "-",
      count: 0,
      totalMs: 0,
      rateLimited: 0,
      upstreamErrors: 0,
    };
    item.count += 1;
    item.totalMs += Number(e.total_latency_ms || 0);
    if (Number(e.response_code) === 429) item.rateLimited += 1;
    if (e.upstream_error) item.upstreamErrors += 1;
    buckets.set(key, item);
  }

  const rows = [...buckets.values()]
    .map((r) => ({ ...r, avg: r.count ? r.totalMs / r.count : 0 }))
    .sort((a, b) => b.avg - a.avg)
    .slice(0, 12);

  byId("slowTable").innerHTML = rows
    .map(
      (r) => `
        <tr>
          <td>${r.service}</td>
          <td>${r.prefix}</td>
          <td>${r.method}</td>
          <td>${r.avg.toFixed(1)} ms</td>
          <td>${r.rateLimited}</td>
          <td>${r.upstreamErrors}</td>
        </tr>
      `,
    )
    .join("");
}

async function refresh() {
  const cfg = getConfig();
  const qs = toQS(cfg);
  setStatus("Loading analytics window...");

  try {
    const [summary, groupedByService, groupedByPrefix, eventsRes] = await Promise.all([
      callAPI(`/analytics/summary?${qs.toString()}`, cfg),
      callAPI(`/analytics/summary?group_by=service&${qs.toString()}`, cfg),
      callAPI(`/analytics/summary?group_by=prefix&${qs.toString()}`, cfg),
      callAPI(`/analytics/events?${qs.toString()}`, cfg),
    ]);

    state.summary = summary;
    state.groupedByService = groupedByService;
    state.groupedByPrefix = groupedByPrefix;
    state.events = normalizeEvents(eventsRes.events || []);

    renderStats(state.summary);
    drawTimeline("timelineChart", state.events.slice(-120));
    drawStatusTimeline("statusTimelineChart", state.events.slice(-240));
    drawSizeTimeline("sizeTimelineChart", state.events.slice(-240));
    drawBarChart("serviceChart", summarizeGroupMap(state.groupedByService).slice(0, 10), "count", "#f4a261");
    drawBarChart(
      "prefixChart",
      summarizeGroupMap(state.groupedByPrefix)
        .map((x) => ({ ...x, rate_limited_count: x.rate_limited_count || 0 }))
        .sort((a, b) => b.rate_limited_count - a.rate_limited_count)
        .slice(0, 10),
      "rate_limited_count",
      "#f46060",
    );
    renderSlowTable(state.events);
    renderEventsTable(state.events);

    setStatus(`Updated ${new Date().toLocaleTimeString()} (${state.summary.count || 0} samples)`);
  } catch (err) {
    setStatus(`Failed to load data: ${err.message}`, true);
  }
}

function bootstrap() {
  byId("apiBase").value = `${window.location.protocol}//${window.location.host}`;
  byId("refreshBtn").addEventListener("click", refresh);
  refresh();
}

bootstrap();
