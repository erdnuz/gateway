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
  if (!Number.isFinite(n)) return "NaN";
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  if (Math.abs(n) >= 1) return n.toFixed(2);
  return n.toFixed(4);
}

function fmtInteger(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return "NaN";
  return Math.round(n).toLocaleString();
}

function fmtMs(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return "NaN";
  if (Math.abs(n) >= 100) return `${n.toFixed(0)}ms`;
  if (Math.abs(n) >= 10) return `${n.toFixed(1)}ms`;
  return `${n.toFixed(2)}ms`;
}

function fmtPercent(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return "NaN";
  return `${(n * 100).toFixed(1)}%`;
}

function fmtBytes(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return "NaN";
  if (n <= 0) return "0B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = n;
  let unitIdx = 0;
  while (value >= 1024 && unitIdx < units.length - 1) {
    value /= 1024;
    unitIdx += 1;
  }
  const digits = unitIdx === 0 ? 0 : value >= 100 ? 0 : value >= 10 ? 1 : 2;
  return `${value.toFixed(digits)}${units[unitIdx]}`;
}

const summaryMeta = {
  requests: { label: "Requests", format: fmtInteger },
  latency_total_p90: { label: "Total Latency", format: fmtMs },
  latency_added_p90: { label: "Added Latency", format: fmtMs },
  volume_request_avg: { label: "Request Size", format: fmtBytes },
  volume_response_avg: { label: "Response Size", format: fmtBytes },
  rates_cache_hit: { label: "Cache Hit", format: fmtPercent },
  rates_upstream_err: { label: "Upstream Error", format: fmtPercent },
  rates_rate_limited: { label: "Rate Limited", format: fmtPercent },
};

function summaryDescriptor(key) {
  if (summaryMeta[key]) return summaryMeta[key];
  
  return { label: key.replaceAll("_", " "), format: fmtNumber };
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function groupFromSummaryKey(key) {
  if (key.startsWith("rates_")) return "rates";
  if (key.startsWith("prefixes_")) return "prefixes";
  if (key.startsWith("services_")) return "services";
  if (key.startsWith("edges_")) return "edges";
  if (key.startsWith("tiers_")) return "tiers";
  return "";
}

function slicePath(cx, cy, radius, startAngle, endAngle) {
  const sx = cx + radius * Math.cos(startAngle);
  const sy = cy + radius * Math.sin(startAngle);
  const ex = cx + radius * Math.cos(endAngle);
  const ey = cy + radius * Math.sin(endAngle);
  const largeArc = endAngle - startAngle > Math.PI ? 1 : 0;
  return `M ${cx} ${cy} L ${sx} ${sy} A ${radius} ${radius} 0 ${largeArc} 1 ${ex} ${ey} Z`;
}

function isFullSweep(sweep) {
  return sweep >= (Math.PI * 2) - 1e-6;
}

function renderSummaryPieCard(title, items, formatter) {
  const positiveItems = items
    .map((item, i) => ({
      ...item,
      color: palette[i % palette.length],
      value: Number(item.value),
    }))
    .filter((item) => Number.isFinite(item.value) && item.value > 0);

  const total = positiveItems.reduce((sum, item) => sum + item.value, 0);
  const pieMarkup = total > 0
    ? (() => {
      let angle = -Math.PI / 2;
      const parts = [];
      for (const item of positiveItems) {
        const sweep = (item.value / total) * Math.PI * 2;
        if (isFullSweep(sweep)) {
          parts.push(`<circle cx="80" cy="80" r="62" fill="${item.color}"></circle>`);
        } else {
          parts.push(`<path d="${slicePath(80, 80, 62, angle, angle + sweep)}" fill="${item.color}"></path>`);
        }
        angle += sweep;
      }
      return `<svg class="summary-pie" viewBox="0 0 160 160" preserveAspectRatio="xMidYMid meet">${parts.join("")}</svg>`;
    })()
    : '<div class="summary-pie-empty">No data</div>';

  const legendMarkup = (positiveItems.length ? positiveItems : items.slice(0, 6))
    .map((item, i) => {
      const descriptor = summaryDescriptor(item.key);
      const color = item.color || palette[i % palette.length];
      const value = Number(item.value);
      const rendered = Number.isFinite(value) ? formatter(value) : "NaN";
      return `<li><span class="summary-pie-dot" style="background:${color}"></span><span class="summary-pie-label">${escapeHtml(descriptor.label)}</span><span class="summary-pie-value">${escapeHtml(rendered)}</span></li>`;
    })
    .join("");

  return `
    <article class="summary-card summary-pie-card">
      <p class="summary-name">${escapeHtml(title)}</p>
      <div class="summary-pie-wrap">
        ${pieMarkup}
        <ul class="summary-pie-legend">${legendMarkup}</ul>
      </div>
    </article>
  `;
}

function fmtTrend(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return { cls: "trend-flat", text: "0.00%" };
  if (n === 0) return { cls: "trend-flat", text: "0.00%" };

  const absPct = Math.abs(n * 100);
  if (absPct < 0.01) {
    if (n > 0) return { cls: "trend-up", text: "+<0.01%" };
    return { cls: "trend-down", text: "-<0.01%" };
  }

  const pct = (n * 100).toFixed(2);
  if (n > 0) return { cls: "trend-up", text: `+${pct}%` };
  if (n < 0) return { cls: "trend-down", text: `${pct}%` };
  return { cls: "trend-flat", text: `${pct}%` };
}

function latestRow(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return null;
  return rows[rows.length - 1] || null;
}

function summaryAvailability(summary, series) {
  const out = {};
  const keys = Object.keys(summary || {});
  const latestLatency = latestRow(series?.latency);
  const latestVolume = latestRow(series?.volume);
  const latestRates = latestRow(series?.rates);
  const latestPrefixes = latestRow(series?.prefixes);
  const latestServices = latestRow(series?.services);
  const latestEdges = latestRow(series?.edges);
  const latestTiers = latestRow(series?.tiers);

  const latestSampleCount = Number(
    latestLatency?.sample_count ?? latestVolume?.sample_count ?? latestRates?.sample_count ?? 0,
  );
  const latestHasSamples = Number.isFinite(latestSampleCount) && latestSampleCount > 0;

  for (const key of keys) {
    if (
      key === "requests"
      || key.startsWith("latency_")
      || key.startsWith("volume_")
      || key.startsWith("rates_")
    ) {
      out[key] = latestHasSamples;
      continue;
    }

    if (key.startsWith("prefixes_")) {
      const dim = key.slice("prefixes_".length);
      out[key] = Number.isFinite(Number(latestPrefixes?.[dim]));
      continue;
    }
    if (key.startsWith("services_")) {
      const dim = key.slice("services_".length);
      out[key] = Number.isFinite(Number(latestServices?.[dim]));
      continue;
    }
    if (key.startsWith("edges_")) {
      const dim = key.slice("edges_".length);
      out[key] = Number.isFinite(Number(latestEdges?.[dim]));
      continue;
    }
    if (key.startsWith("tiers_")) {
      const dim = key.slice("tiers_".length);
      out[key] = Number.isFinite(Number(latestTiers?.[dim]));
      continue;
    }

    out[key] = true;
  }
  return out;
}

function renderSummary(summary, series) {
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
  const availability = summaryAvailability(summary, series);

  const scalarCards = [];
  const grouped = {
    rates: [],
    prefixes: [],
    services: [],
    edges: [],
    tiers: [],
  };

  for (const key of keys) {
    const metric = summary[key] || {};
    const group = groupFromSummaryKey(key);
    if (group) {
      grouped[group].push({ key, value: metric.last_value });
      continue;
    }

    const trend = fmtTrend(metric.trend);
    const descriptor = summaryDescriptor(key);
    const hasCurrentValue = availability[key] !== false;
    const valueText = hasCurrentValue ? descriptor.format(metric.last_value) : "NaN";
    const trendText = hasCurrentValue ? trend.text : "No samples";
    scalarCards.push(`
      <article class="summary-card">
        <p class="summary-name">${descriptor.label}</p>
        <p class="summary-value">${valueText}</p>
        <p class="summary-trend ${trend.cls}">${trendText}</p>
      </article>
    `);
  }

  const pieCards = [];
  if (grouped.rates.length>1) pieCards.push(renderSummaryPieCard("Rates", grouped.rates, fmtPercent));
  if (grouped.prefixes.length>1) pieCards.push(renderSummaryPieCard("Prefixes", grouped.prefixes, fmtInteger));
  if (grouped.services.length>1) pieCards.push(renderSummaryPieCard("Services", grouped.services, fmtInteger));
  if (grouped.edges.length>1) pieCards.push(renderSummaryPieCard("Edges", grouped.edges, fmtInteger));
  if (grouped.tiers.length>1) pieCards.push(renderSummaryPieCard("Tiers", grouped.tiers, fmtInteger));

  grid.innerHTML = scalarCards
    .concat(pieCards)
    .join("");
}

function niceStep(span, ticks) {
  if (!Number.isFinite(span) || span <= 0) return 1;
  const raw = span / Math.max(1, ticks);
  const magnitude = Math.pow(10, Math.floor(Math.log10(raw)));
  const residual = raw / magnitude;
  if (residual <= 1) return magnitude;
  if (residual <= 2) return 2 * magnitude;
  if (residual <= 5) return 5 * magnitude;
  return 10 * magnitude;
}

function fmtAxisCount(v) {
  if (!Number.isFinite(v)) return "NaN";
  return Math.round(v).toLocaleString(undefined, { notation: "compact", maximumFractionDigits: 1 });
}

function fmtAxisMs(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "NaN";
  if (Math.abs(n) >= 1000) return `${(n / 1000).toFixed(1)}s`;
  if (Math.abs(n) >= 100) return `${n.toFixed(0)}ms`;
  if (Math.abs(n) >= 10) return `${n.toFixed(1)}ms`;
  return `${n.toFixed(2)}ms`;
}

function fmtAxisBytes(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "NaN";
  return fmtBytes(n);
}

function fmtAxisRatio(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "NaN";
  return `${(n * 100).toFixed(0)}%`;
}

function parseTime(value) {
  const t = new Date(value).getTime();
  return Number.isFinite(t) ? t : null;
}

function isSameLocalDay(tsA, tsB) {
  const a = new Date(tsA);
  const b = new Date(tsB);
  return a.getFullYear() === b.getFullYear()
    && a.getMonth() === b.getMonth()
    && a.getDate() === b.getDate();
}

function fmtXAxisLabel(ts, sameDay) {
  const date = new Date(ts);
  if (sameDay) {
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  }
  return date.toLocaleString([], {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function drawChart(svgId, legendId, points, lines, formatter) {
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

  let validPointCount = 0;
  for (const p of points) {
    const t = parseTime(p.time);
    if (t === null) continue;
    let hasValue = false;
    for (const line of lines) {
      const v = Number(p[line.key]);
      if (Number.isFinite(v)) {
        hasValue = true;
        break;
      }
    }
    if (hasValue) {
      validPointCount += 1;
    }
  }
  if (validPointCount < 2) {
    svg.appendChild(makeText(450, 140, "Need at least 2 data points", "#aeb4c0", 14, "middle"));
    legend.innerHTML = "";
    return;
  }

  const xMin = Math.min(...xVals);
  const xMax = Math.max(...xVals);
  const xSpan = Math.max(1, xMax - xMin);
  const singleDayRange = isSameLocalDay(xMin, xMax);

  const yVals = [];
  for (const line of lines) {
    for (const p of points) {
      const v = Number(p[line.key]);
      if (Number.isFinite(v)) yVals.push(v);
    }
  }
  const yMinRaw = yVals.length ? Math.min(...yVals) : 0;
  const yMaxRaw = yVals.length ? Math.max(...yVals) : 1;
  const yBaselineMin = Math.min(0, yMinRaw);
  const yBaselineMax = yMaxRaw === yBaselineMin ? yBaselineMin + 1 : yMaxRaw;
  const step = niceStep(yBaselineMax - yBaselineMin, 5);
  const tickMin = Math.floor(yBaselineMin / step) * step;
  const tickMax = Math.ceil(yBaselineMax / step) * step;
  const ySpan = Math.max(step, tickMax - tickMin);

  for (let v = tickMin; v <= tickMax + step / 2; v += step) {
    const ratio = (v - tickMin) / ySpan;
    const gy = pad.top + ih - ratio * ih;
    svg.appendChild(makeLine(pad.left, gy, w - pad.right, gy, "rgba(255,255,255,0.08)", 1));
    svg.appendChild(makeText(pad.left - 8, gy + 4, formatter(v), "#aeb4c0", 11, "end"));
  }


  for (const line of lines) {
    let d = "";
    let hadPoint = false;
    let drewAny = false;
    for (const p of points) {
      const t = parseTime(p.time);
      const v = Number(p[line.key]);
      if (t === null || !Number.isFinite(v)) {
        // Break the path across missing samples so gaps stay visible.
        hadPoint = false;
        continue;
      }
      const x = pad.left + ((t - xMin) / xSpan) * iw;
      const y = pad.top + ih - ((v - tickMin) / ySpan) * ih;
      d += `${hadPoint ? "L" : "M"}${x} ${y} `;
      hadPoint = true;
      drewAny = true;
    }
    if (!drewAny) continue;

    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", d.trim());
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", line.color);
    path.setAttribute("stroke-width", "2");
    svg.appendChild(path);
  }

  svg.appendChild(makeText(pad.left, h - 14, fmtXAxisLabel(xMin, singleDayRange), "#aeb4c0", 10, "start"));
  svg.appendChild(makeText(w - pad.right, h - 14, fmtXAxisLabel(xMax, singleDayRange), "#aeb4c0", 10, "end"));

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

function clearChart(svgId, legendId) {
  const svg = byId(svgId);
  const legend = byId(legendId);
  if (svg) {
    while (svg.firstChild) svg.removeChild(svg.firstChild);
  }
  if (legend) {
    legend.innerHTML = "";
  }
}

function setSeriesCardVisible(cardId, visible) {
  const card = byId(cardId);
  if (!card) return;
  card.hidden = !visible;
}

function renderSeries(series) {
  const latency = (Array.isArray(series?.latency) ? series.latency : []).map((point) => {
    if (point.sample_count === undefined || point.sample_count === null) return point;
    if (Number(point.sample_count) > 0) return point;
    return {
      ...point,
      latency_total_p50: Number.NaN,
      latency_total_p90: Number.NaN,
      latency_total_p95: Number.NaN,
      latency_upstream_p50: Number.NaN,
      latency_upstream_p90: Number.NaN,
      latency_upstream_p95: Number.NaN,
      latency_added_p50: Number.NaN,
      latency_added_p90: Number.NaN,
      latency_added_p95: Number.NaN,
    };
  });
  drawChart(
    "latencyChart",
    "latencyLegend",
    latency,
    [
      { key: "latency_total_p90", label: "total p90", color: palette[0] },
      { key: "latency_upstream_p90", label: "upstream p90", color: palette[1] },
      { key: "latency_added_p90", label: "added p90", color: palette[2] },
    ],
    fmtAxisMs,
  );

  const volume = (Array.isArray(series?.volume) ? series.volume : []).map((point) => {
    if (point.sample_count === undefined || point.sample_count === null) return point;
    if (Number(point.sample_count) > 0) return point;
    return {
      ...point,
      request_avg: Number.NaN,
      response_avg: Number.NaN,
    };
  });
  drawChart(
    "volumeChart",
    "volumeLegend",
    volume,
    [
      { key: "request_avg", label: "request avg", color: palette[0] },
      { key: "response_avg", label: "response avg", color: palette[1] },
    ],
    fmtAxisBytes,
  );

  const rates = (Array.isArray(series?.rates) ? series.rates : []).map((point) => {
    if (point.sample_count === undefined || point.sample_count === null) return point;
    if (Number(point.sample_count) > 0) return point;
    return {
      ...point,
      cache_hit: Number.NaN,
      upstream_err: Number.NaN,
      rate_limited: Number.NaN,
    };
  });
  drawChart(
    "ratesChart",
    "ratesLegend",
    rates,
    [
      { key: "cache_hit", label: "cache hit", color: palette[1] },
      { key: "upstream_err", label: "upstream err", color: palette[3] },
      { key: "rate_limited", label: "rate limited", color: palette[2] },
    ],
    fmtAxisRatio,
  );

  const prefixes = Array.isArray(series?.prefixes) ? series.prefixes : [];
  const prefixKeys = keysFromDynamicSeries(prefixes);
  const showPrefixes = prefixKeys.length > 1;
  setSeriesCardVisible("prefixesCard", showPrefixes);
  if (showPrefixes) {
    drawChart(
      "prefixesChart",
      "prefixesLegend",
      prefixes,
      prefixKeys.map((k, i) => ({ key: k, label: k, color: palette[i % palette.length] })),
      fmtAxisCount,
    );
  } else {
    clearChart("prefixesChart", "prefixesLegend");
  }

  const services = Array.isArray(series?.services) ? series.services : [];
  const serviceKeys = keysFromDynamicSeries(services);
  const showServices = serviceKeys.length > 1;
  setSeriesCardVisible("servicesCard", showServices);
  if (showServices) {
    drawChart(
      "servicesChart",
      "servicesLegend",
      services,
      serviceKeys.map((k, i) => ({ key: k, label: k, color: palette[i % palette.length] })),
      fmtAxisCount,
    );
  } else {
    clearChart("servicesChart", "servicesLegend");
  }

  const edges = Array.isArray(series?.edges) ? series.edges : [];
  const edgeKeys = keysFromDynamicSeries(edges);
  const showEdges = edgeKeys.length > 1;
  setSeriesCardVisible("edgesCard", showEdges);
  if (showEdges) {
    drawChart(
      "edgesChart",
      "edgesLegend",
      edges,
      edgeKeys.map((k, i) => ({ key: k, label: k, color: palette[i % palette.length] })),
      fmtAxisCount,
    );
  } else {
    clearChart("edgesChart", "edgesLegend");
  }

  const tiers = Array.isArray(series?.tiers) ? series.tiers : [];
  const tierKeys = keysFromDynamicSeries(tiers);
  const showTiers = tierKeys.length > 1;
  setSeriesCardVisible("tiersCard", showTiers);
  if (showTiers) {
    drawChart(
      "tiersChart",
      "tiersLegend",
      tiers,
      tierKeys.map((k, i) => ({ key: k, label: k, color: palette[i % palette.length] })),
      fmtAxisCount,
    );
  } else {
    clearChart("tiersChart", "tiersLegend");
  }
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

    renderSummary(payload.summary, payload.series);
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
