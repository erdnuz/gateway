const byId = (id) => document.getElementById(id);

const state = {
  authFailed: false,
  poller: null,
};

const palette = [
  "#7aa2ff",
  "#79e2c8",
  "#ffcf7a",
  "#ff9bb2",
  "#bda3ff",
  "#9ad7ff",
  "#8fe3a2",
  "#ffa8a8",
];

function setStatus(msg, bad = false) {
  const el = byId("status");
  if (!el) return;
  el.textContent = msg;
  el.style.color = bad ? "#ff7f8a" : "#aeb4c0";
}

function normalizeToken(token) {
  if (!token) return "";
  return token.replace(/^Bearer\s+/i, "").trim();
}

function toIso(raw) {
  const text = (raw || "").trim();
  if (!text) return "";
  const d = new Date(text);
  if (!Number.isFinite(d.getTime())) return "";
  return d.toISOString();
}

function getConfig() {
  const defaultBase = `${window.location.protocol}//${window.location.host}`;
  return {
    base: (byId("apiBase").value || defaultBase).replace(/\/$/, ""),
    token: normalizeToken(byId("token").value),
    interval: byId("interval").value,
    prefix: byId("filterPrefix").value.trim(),
    service: byId("filterService").value.trim(),
    edge: byId("filterEdge").value.trim(),
    tier: byId("filterTier").value.trim(),
    method: byId("filterMethod").value.trim(),
    rcMin: byId("filterRcMin").value.trim(),
    rcMax: byId("filterRcMax").value.trim(),
    cacheHit: byId("filterCacheHit").value,
    upstreamError: byId("filterUpstreamError").value,
    latencyMin: byId("filterLatencyMin").value.trim(),
    latencyMax: byId("filterLatencyMax").value.trim(),
    start: byId("filterStart").value,
    end: byId("filterEnd").value,
  };
}

function buildSummaryUrl(cfg) {
  const p = new URLSearchParams();
  p.set("interval", cfg.interval);
  if (cfg.prefix) p.set("prefix", cfg.prefix);
  if (cfg.service) p.set("service", cfg.service);
  if (cfg.edge) p.set("edge_id", cfg.edge);
  if (cfg.tier) p.set("tier", cfg.tier);
  if (cfg.method) p.set("method", cfg.method);
  if (cfg.rcMin) p.set("response_code_min", cfg.rcMin);
  if (cfg.rcMax) p.set("response_code_max", cfg.rcMax);
  if (cfg.cacheHit) p.set("cache_hit", cfg.cacheHit);
  if (cfg.upstreamError) p.set("upstream_error", cfg.upstreamError);
  if (cfg.latencyMin) p.set("total_latency_ms_min", cfg.latencyMin);
  if (cfg.latencyMax) p.set("total_latency_ms_max", cfg.latencyMax);
  const startIso = toIso(cfg.start);
  const endIso = toIso(cfg.end);
  if (startIso) p.set("start", startIso);
  if (endIso) p.set("end", endIso);
  return `${cfg.base}/analytics/summary?${p.toString()}`;
}

function authHeaders(cfg) {
  const headers = {};
  if (cfg.token) headers.Authorization = `Bearer ${cfg.token}`;
  return headers;
}

async function fetchJsonWithAuth(url, cfg) {
  const res = await fetch(url, { headers: authHeaders(cfg) });
  if (res.status === 401) {
    state.authFailed = true;
    byId("authBanner").hidden = false;
    if (state.poller) {
      clearInterval(state.poller);
      state.poller = null;
    }
    throw new Error("Unauthorized");
  }
  return res;
}

function fmtNumber(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return "0";
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  if (Math.abs(n) >= 1) return n.toFixed(2);
  return n.toFixed(4);
}

function fmtTrend(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return { cls: "trend-flat", text: "0.00%" };
  const pct = (n * 100).toFixed(2);
  if (n > 0) return { cls: "trend-up", text: `+${pct}%` };
  if (n < 0) return { cls: "trend-down", text: `${pct}%` };
  return { cls: "trend-flat", text: `${pct}%` };
}

function renderSummary(summary) {
  const grid = byId("summaryGrid");
  if (!summary || typeof summary !== "object") {
    grid.innerHTML = '<p class="empty">No summary data.</p>';
    return;
  }
  const keys = Object.keys(summary).sort();
  if (!keys.length) {
    grid.innerHTML = '<p class="empty">No summary metrics returned.</p>';
    return;
  }

  grid.innerHTML = keys
    .map((key) => {
      const metric = summary[key] || {};
      const trend = fmtTrend(metric.trend);
      return `
        <article class="summary-card">
          <p class="summary-name">${key}</p>
          <p class="summary-value">${fmtNumber(metric.last_value)}</p>
          <p class="summary-trend ${trend.cls}">${trend.text}</p>
        </article>
      `;
    })
    .join("");
}

function parseTime(value) {
  const t = new Date(value).getTime();
  return Number.isFinite(t) ? t : null;
}

function drawChart(svgId, legendId, points, lines, yLabel, formatter) {
  const svg = byId(svgId);
  const legend = byId(legendId);
  if (!svg || !legend) return;

  while (svg.firstChild) svg.removeChild(svg.firstChild);

  if (!points.length || !lines.length) {
    svg.appendChild(makeText(450, 140, "No data", "#aeb4c0", 14, "middle"));
    legend.innerHTML = "";
    return;
  }

  const w = 900;
  const h = 280;
  const pad = { top: 18, right: 14, bottom: 42, left: 56 };
  const iw = w - pad.left - pad.right;
  const ih = h - pad.top - pad.bottom;

  const xVals = points.map((p) => parseTime(p.time)).filter((t) => t !== null);
  if (!xVals.length) {
    svg.appendChild(makeText(450, 140, "No valid time points", "#aeb4c0", 14, "middle"));
    legend.innerHTML = "";
    return;
  }

  const xMin = Math.min(...xVals);
  const xMax = Math.max(...xVals);
  const xSpan = Math.max(1, xMax - xMin);

  const yVals = [];
  for (const line of lines) {
    for (const p of points) {
      const v = Number(p[line.key]);
      if (Number.isFinite(v)) yVals.push(v);
    }
  }
  const yMinRaw = yVals.length ? Math.min(...yVals) : 0;
  const yMaxRaw = yVals.length ? Math.max(...yVals) : 1;
  const yMin = Math.min(0, yMinRaw);
  const yMax = yMaxRaw === yMin ? yMin + 1 : yMaxRaw;
  const ySpan = yMax - yMin;

  for (let i = 0; i <= 4; i++) {
    const gy = pad.top + (ih * i) / 4;
    const v = yMax - (ySpan * i) / 4;
    svg.appendChild(makeLine(pad.left, gy, w - pad.right, gy, "rgba(255,255,255,0.08)", 1));
    svg.appendChild(makeText(pad.left - 8, gy + 4, formatter(v), "#aeb4c0", 11, "end"));
  }

  svg.appendChild(makeText(16, 18, yLabel, "#aeb4c0", 12, "start"));

  for (const line of lines) {
    let d = "";
    let hadPoint = false;
    for (const p of points) {
      const t = parseTime(p.time);
      const v = Number(p[line.key]);
      if (t === null || !Number.isFinite(v)) continue;
      const x = pad.left + ((t - xMin) / xSpan) * iw;
      const y = pad.top + ih - ((v - yMin) / ySpan) * ih;
      d += `${hadPoint ? "L" : "M"}${x} ${y} `;
      hadPoint = true;
    }
    if (!hadPoint) continue;

    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", d.trim());
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", line.color);
    path.setAttribute("stroke-width", "2");
    svg.appendChild(path);
  }

  svg.appendChild(makeText(pad.left, h - 14, new Date(xMin).toLocaleString(), "#aeb4c0", 10, "start"));
  svg.appendChild(makeText(w - pad.right, h - 14, new Date(xMax).toLocaleString(), "#aeb4c0", 10, "end"));

  legend.innerHTML = lines
    .map((line) => `<span class="legend-item"><span class="legend-swatch" style="background:${line.color}"></span>${line.label}</span>`)
    .join("");
}

function makeLine(x1, y1, x2, y2, stroke, width) {
  const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
  line.setAttribute("x1", x1);
  line.setAttribute("y1", y1);
  line.setAttribute("x2", x2);
  line.setAttribute("y2", y2);
  line.setAttribute("stroke", stroke);
  line.setAttribute("stroke-width", width);
  return line;
}

function makeText(x, y, text, fill, size, anchor) {
  const t = document.createElementNS("http://www.w3.org/2000/svg", "text");
  t.setAttribute("x", x);
  t.setAttribute("y", y);
  t.setAttribute("fill", fill);
  t.setAttribute("font-size", String(size));
  t.setAttribute("text-anchor", anchor);
  t.textContent = text;
  return t;
}

function keysFromDynamicSeries(rows) {
  const score = {};
  for (const row of rows) {
    for (const k of Object.keys(row)) {
      if (k === "time") continue;
      const v = Number(row[k] || 0);
      if (!Number.isFinite(v)) continue;
      score[k] = (score[k] || 0) + v;
    }
  }
  return Object.entries(score)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)
    .map((x) => x[0]);
}

function renderSeries(series) {
  const latency = Array.isArray(series?.latency) ? series.latency : [];
  drawChart(
    "latencyChart",
    "latencyLegend",
    latency,
    [
      { key: "latency_total_p90", label: "total p90", color: palette[0] },
      { key: "latency_upstream_p90", label: "upstream p90", color: palette[1] },
      { key: "latency_added_p90", label: "added p90", color: palette[2] },
      { key: "latency_total_p50", label: "total p50", color: palette[3] },
      { key: "latency_total_p95", label: "total p95", color: palette[4] },
    ],
    "ms",
    (v) => Number(v).toFixed(1),
  );

  const volume = Array.isArray(series?.volume) ? series.volume : [];
  drawChart(
    "volumeChart",
    "volumeLegend",
    volume,
    [
      { key: "request_avg", label: "request avg", color: palette[0] },
      { key: "response_avg", label: "response avg", color: palette[1] },
    ],
    "bytes",
    (v) => Number(v).toFixed(0),
  );

  const rates = Array.isArray(series?.rates) ? series.rates : [];
  drawChart(
    "ratesChart",
    "ratesLegend",
    rates,
    [
      { key: "cache_hit", label: "cache hit", color: palette[1] },
      { key: "upstream_err", label: "upstream err", color: palette[3] },
      { key: "rate_limited", label: "rate limited", color: palette[2] },
    ],
    "ratio",
    (v) => Number(v).toFixed(3),
  );

  const prefixes = Array.isArray(series?.prefixes) ? series.prefixes : [];
  const prefixKeys = keysFromDynamicSeries(prefixes);
  drawChart(
    "prefixesChart",
    "prefixesLegend",
    prefixes,
    prefixKeys.map((k, i) => ({ key: k, label: k, color: palette[i % palette.length] })),
    "count",
    (v) => Number(v).toFixed(0),
  );

  const services = Array.isArray(series?.services) ? series.services : [];
  const serviceKeys = keysFromDynamicSeries(services);
  drawChart(
    "servicesChart",
    "servicesLegend",
    services,
    serviceKeys.map((k, i) => ({ key: k, label: k, color: palette[i % palette.length] })),
    "count",
    (v) => Number(v).toFixed(0),
  );

  const edges = Array.isArray(series?.edges) ? series.edges : [];
  const edgeKeys = keysFromDynamicSeries(edges);
  drawChart(
    "edgesChart",
    "edgesLegend",
    edges,
    edgeKeys.map((k, i) => ({ key: k, label: k, color: palette[i % palette.length] })),
    "count",
    (v) => Number(v).toFixed(0),
  );
}

async function loadFeatures() {
  const cfg = getConfig();
  try {
    const res = await fetch(`${cfg.base}/analytics/features`);
    if (!res.ok) return;
    const data = await res.json();
    byId("clearBtn").hidden = !data.testing_clear_enabled;
  } catch {
    byId("clearBtn").hidden = true;
  }
}

async function clearAnalytics() {
  const cfg = getConfig();
  if (!window.confirm("Clear all analytics data?")) return;

  try {
    const res = await fetch(`${cfg.base}/analytics/clear`, {
      method: "POST",
      headers: authHeaders(cfg),
    });
    if (res.status === 403) {
      setStatus("Clear is disabled in this deployment.", true);
      return;
    }
    if (!res.ok) {
      setStatus(`Failed to clear analytics (${res.status}).`, true);
      return;
    }
    setStatus("Analytics cleared.");
    await refresh();
  } catch (err) {
    setStatus(`Failed to clear analytics: ${err.message}`, true);
  }
}

async function refresh() {
  const cfg = getConfig();
  setStatus("Loading summary...");

  try {
    const res = await fetchJsonWithAuth(buildSummaryUrl(cfg), cfg);
    const payload = await res.json();

    if (res.status === 202 || payload.status === "processing") {
      setStatus("Summary cache warming. Please retry shortly.");
      return;
    }
    if (!res.ok) {
      setStatus(`Failed to load summary (${res.status}).`, true);
      return;
    }

    if (state.authFailed) {
      state.authFailed = false;
      byId("authBanner").hidden = true;
      if (!state.poller) state.poller = setInterval(refresh, 15000);
    }

    renderSummary(payload.summary);
    renderSeries(payload.series);
    setStatus(`Updated ${new Date().toLocaleTimeString()}`);
  } catch (err) {
    if (err.message !== "Unauthorized") {
      setStatus(`Failed to load summary: ${err.message}`, true);
    }
  }
}

function attachFilterListeners() {
  const ids = [
    "interval",
    "filterStart",
    "filterEnd",
    "filterPrefix",
    "filterService",
    "filterEdge",
    "filterTier",
    "filterMethod",
    "filterRcMin",
    "filterRcMax",
    "filterCacheHit",
    "filterUpstreamError",
    "filterLatencyMin",
    "filterLatencyMax",
  ];
  for (const id of ids) {
    const el = byId(id);
    if (!el) continue;
    el.addEventListener("change", refresh);
  }
}

function bootstrap() {
  byId("apiBase").value = `${window.location.protocol}//${window.location.host}`;
  byId("refreshBtn").addEventListener("click", refresh);
  byId("clearBtn").addEventListener("click", clearAnalytics);
  byId("token").addEventListener("change", refresh);
  attachFilterListeners();
  loadFeatures();
  refresh();
  state.poller = setInterval(refresh, 15000);
}

bootstrap();
