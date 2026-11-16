/**
 * app.js — CRDT Analytics Dashboard frontend
 * Plain ES2020 vanilla JS + Chart.js 4.x (bundled locally).
 * No innerHTML with API data — all DOM text uses textContent or createElement.
 */

"use strict";

// ============================================================ Config
const POLL_MS = 1000;
const MAX_LINE_POINTS = 60;    // keep last 60 samples on the line chart
const METRICS = ["page_views", "clicks"];

// Palette for per-node colouring (up to 10 nodes)
const NODE_COLORS = [
  "#3b82f6", "#22c55e", "#f97316", "#a855f7",
  "#eab308", "#ef4444", "#06b6d4", "#ec4899",
  "#84cc16", "#f59e0b",
];

// ============================================================ State
let knownNodes = [];          // sorted list of node ids from last poll
let colorMap = {};            // node_id -> colour string
let pending = false;          // global "request in flight" lock
let lineHistory = [];         // array of history samples

// ============================================================ Chart.js instances
let barChart = null;
let lineChart = null;

// ============================================================ Helpers

/** Escape a string for safe textContent insertion (redundant but explicit). */
function esc(s) {
  return String(s == null ? "" : s);
}

/** Format a unix timestamp as HH:MM:SS. */
function fmtTime(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString("en-GB", { hour12: false });
}

/** Assign a stable colour to a node. */
function nodeColor(id) {
  if (!colorMap[id]) {
    const idx = Object.keys(colorMap).length % NODE_COLORS.length;
    colorMap[id] = NODE_COLORS[idx];
  }
  return colorMap[id];
}

/** POST JSON and return parsed body (or throw). */
async function postJSON(url, body) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(data.error || `HTTP ${resp.status}`);
  return data;
}

// ============================================================ Toast notifications
function showToast(msg, type = "info") {
  const container = document.getElementById("toast-container");
  const el = document.createElement("div");
  el.className = "toast " + (type === "error" ? "error" : type === "ok" ? "success" : "");
  el.textContent = msg;
  container.appendChild(el);
  setTimeout(() => el.remove(), 4000);
}

// ============================================================ Offline banner
function setOffline(offline) {
  document.getElementById("offline-banner").classList.toggle("hidden", !offline);
}

// ============================================================ Button helpers
function withPending(fn) {
  return async function (...args) {
    if (pending) return;
    pending = true;
    document.querySelectorAll(".btn").forEach(b => (b.disabled = true));
    try {
      await fn(...args);
    } finally {
      pending = false;
      document.querySelectorAll(".btn").forEach(b => (b.disabled = false));
    }
  };
}

// ============================================================ Render node cards
function renderNodeCards(nodes) {
  const grid = document.getElementById("node-grid");

  // Build a map of existing cards
  const existing = {};
  grid.querySelectorAll(".node-card").forEach(c => {
    existing[c.dataset.node] = c;
  });

  const seen = new Set();
  for (const node of nodes) {
    seen.add(node.id);
    let card = existing[node.id];
    if (!card) {
      card = document.createElement("div");
      card.className = "node-card";
      card.dataset.node = node.id;
      grid.appendChild(card);
    }

    // Status class
    card.className = `node-card status-${esc(node.status)}`;

    // Build card content (clear and rebuild — DOM only, no innerHTML)
    card.textContent = "";

    // Header row
    const header = document.createElement("div");
    header.className = "node-header";

    const idSpan = document.createElement("span");
    idSpan.className = "node-id";
    idSpan.textContent = node.id;
    header.appendChild(idSpan);

    const dot = document.createElement("span");
    dot.className = `node-status-dot dot-${esc(node.status)}`;
    dot.title = node.status;
    header.appendChild(dot);
    card.appendChild(header);

    // Status text
    const statusRow = document.createElement("div");
    statusRow.style.fontSize = ".78rem";
    statusRow.style.color = "var(--text-muted)";
    statusRow.textContent = node.status.toUpperCase();
    if (node.error) {
      statusRow.textContent += " — " + esc(node.error);
    }
    card.appendChild(statusRow);

    // Metric rows
    const metricsDiv = document.createElement("div");
    metricsDiv.className = "node-metrics";
    for (const metric of METRICS) {
      const val = (node.totals || {})[metric];
      const row = document.createElement("div");
      row.className = "node-metric-row";
      const nameEl = document.createElement("span");
      nameEl.className = "node-metric-name";
      nameEl.textContent = metric;
      const valEl = document.createElement("span");
      valEl.className = "node-metric-val";
      valEl.textContent = val == null ? "—" : val.toLocaleString();
      row.appendChild(nameEl);
      row.appendChild(valEl);
      metricsDiv.appendChild(row);
    }
    card.appendChild(metricsDiv);

    // Footer: hash chip + tags
    const footer = document.createElement("div");
    footer.className = "node-footer";

    if (node.state_hash) {
      const chip = document.createElement("span");
      chip.className = "hash-chip";
      chip.textContent = node.state_hash.slice(0, 8);
      // Color chip by hash so matching hashes look identical
      chip.style.background = hashToColor(node.state_hash.slice(0, 8));
      chip.title = "state_hash: " + node.state_hash;
      footer.appendChild(chip);
    }

    if (node.source === "exec") {
      const execTag = document.createElement("span");
      execTag.className = "exec-tag";
      execTag.textContent = "via exec";
      footer.appendChild(execTag);
    }

    if (node.blocked && node.blocked.length > 0) {
      const bt = document.createElement("span");
      bt.className = "blocked-tag";
      bt.textContent = "blocked";
      bt.title = "blocked: " + node.blocked.join(", ");
      footer.appendChild(bt);
    }

    card.appendChild(footer);
  }

  // Remove cards for nodes no longer present
  for (const [id, card] of Object.entries(existing)) {
    if (!seen.has(id)) card.remove();
  }
}

/** Map a short hash string to a background color deterministically. */
function hashToColor(hash) {
  let h = 0;
  for (const c of hash) h = (h * 31 + c.charCodeAt(0)) & 0xfffffff;
  const hue = h % 360;
  return `hsl(${hue}, 50%, 25%)`;
}

// ============================================================ Render node selects
function renderNodeSelects(nodes) {
  const ids = nodes.map(n => n.id);
  if (JSON.stringify(ids) === JSON.stringify(knownNodes)) return;
  knownNodes = ids;

  for (const selId of ["visit-node", "iso-node", "kill-node"]) {
    const sel = document.getElementById(selId);
    const cur = sel.value;
    sel.textContent = "";
    for (const id of ids) {
      const opt = document.createElement("option");
      opt.value = id;
      opt.textContent = id;
      sel.appendChild(opt);
    }
    if (ids.includes(cur)) sel.value = cur;
  }

  // Partition checkboxes
  for (const groupId of ["partition-group-a", "partition-group-b"]) {
    const div = document.getElementById(groupId);
    div.textContent = "";
    for (const id of ids) {
      const lbl = document.createElement("label");
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.value = id;
      cb.dataset.group = groupId;
      lbl.appendChild(cb);
      lbl.appendChild(document.createTextNode(" " + id));
      div.appendChild(lbl);
    }
  }
}

// ============================================================ Charts

function initCharts() {
  const barCtx = document.getElementById("chart-bar").getContext("2d");
  barChart = new Chart(barCtx, {
    type: "bar",
    data: { labels: [], datasets: [] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: "#e2e8f0", font: { size: 11 } } },
        tooltip: { mode: "index" },
      },
      scales: {
        x: {
          stacked: true,
          ticks: { color: "#8892a4" },
          grid: { color: "#2e3347" },
        },
        y: {
          stacked: true,
          beginAtZero: true,
          ticks: { color: "#8892a4" },
          grid: { color: "#2e3347" },
        },
      },
    },
  });

  const lineCtx = document.getElementById("chart-line").getContext("2d");
  lineChart = new Chart(lineCtx, {
    type: "line",
    data: { labels: [], datasets: [] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        legend: { labels: { color: "#e2e8f0", font: { size: 11 } } },
      },
      scales: {
        x: { ticks: { color: "#8892a4", maxTicksLimit: 10 }, grid: { color: "#2e3347" } },
        y: { beginAtZero: true, ticks: { color: "#8892a4" }, grid: { color: "#2e3347" } },
      },
    },
  });
}

function updateBarChart(nodes) {
  const metric = document.getElementById("bar-metric-sel").value;
  const labels = nodes.map(n => n.id);

  // Each dataset is one originating node_id's contribution
  const allOrigins = new Set();
  for (const node of nodes) {
    const pn = (node.per_node || {})[metric] || {};
    for (const k of Object.keys(pn)) allOrigins.add(k);
  }
  const origins = [...allOrigins].sort();

  const datasets = origins.map(origin => ({
    label: origin,
    data: nodes.map(n => ((n.per_node || {})[metric] || {})[origin] || 0),
    backgroundColor: nodeColor(origin) + "cc",
    borderColor: nodeColor(origin),
    borderWidth: 1,
  }));

  barChart.data.labels = labels;
  barChart.data.datasets = datasets;
  barChart.update("none");
}

function updateLineChart(history) {
  if (!history || history.length === 0) return;
  const recent = history.slice(-MAX_LINE_POINTS);
  const labels = recent.map(s => fmtTime(s.ts));

  // Collect all node ids across history
  const allNodes = new Set();
  for (const s of recent) {
    for (const id of Object.keys(s.per_node_totals || {})) allNodes.add(id);
  }

  // Also add a "global" line
  const datasets = [];
  for (const id of [...allNodes].sort()) {
    datasets.push({
      label: id,
      data: recent.map(s => {
        const totals = (s.per_node_totals || {})[id] || {};
        return totals["page_views"] || 0;
      }),
      borderColor: nodeColor(id),
      backgroundColor: "transparent",
      borderWidth: 2,
      pointRadius: 0,
      tension: 0.3,
    });
  }

  lineChart.data.labels = labels;
  lineChart.data.datasets = datasets;
  lineChart.update("none");
}

// ============================================================ Event log
function renderEvents(events) {
  const log = document.getElementById("event-log");
  log.textContent = "";
  for (const ev of [...events].reverse()) {
    const row = document.createElement("div");
    row.className = "event-row";
    const ts = document.createElement("span");
    ts.className = "event-ts";
    ts.textContent = fmtTime(ev.ts);
    const action = document.createElement("span");
    action.className = "event-action";
    action.textContent = esc(ev.action);
    const result = document.createElement("span");
    result.className = "event-result" + (ev.result.startsWith("ERROR") ? " err" : "");
    result.textContent = esc(ev.result);
    row.appendChild(ts);
    row.appendChild(action);
    row.appendChild(result);
    log.appendChild(row);
  }
}

// ============================================================ Poll loop
async function poll() {
  try {
    const [clusterResp, histResp, eventsResp] = await Promise.all([
      fetch("/api/cluster"),
      fetch("/api/history"),
      fetch("/api/events"),
    ]);

    if (!clusterResp.ok) throw new Error(`/api/cluster ${clusterResp.status}`);
    const cluster = await clusterResp.json();
    const history = histResp.ok ? await histResp.json() : [];
    const events = eventsResp.ok ? await eventsResp.json() : [];

    setOffline(false);

    // Header
    const badge = document.getElementById("status-badge");
    if (cluster.converged) {
      badge.textContent = "CONVERGED";
      badge.className = "badge badge-converged";
    } else {
      badge.textContent = "DIVERGED";
      badge.className = "badge badge-diverged";
    }
    const lcs = cluster.last_convergence_seconds;
    const convEl = document.getElementById("conv-time");
    convEl.textContent = lcs != null
      ? `last convergence: ${lcs.toFixed(2)}s`
      : "";

    document.getElementById("global-pv").textContent =
      (cluster.global || {}).page_views != null
        ? Number((cluster.global || {}).page_views).toLocaleString()
        : "—";
    document.getElementById("global-cl").textContent =
      (cluster.global || {}).clicks != null
        ? Number((cluster.global || {}).clicks).toLocaleString()
        : "—";

    renderNodeCards(cluster.nodes || []);
    renderNodeSelects(cluster.nodes || []);
    updateBarChart(cluster.nodes || []);
    updateLineChart(history);
    renderEvents(events);
  } catch (err) {
    setOffline(true);
  }
}

// ============================================================ Control actions

document.addEventListener("DOMContentLoaded", () => {
  initCharts();
  poll();
  setInterval(poll, POLL_MS);

  // Bar chart metric selector
  document.getElementById("bar-metric-sel").addEventListener("change", () => {
    // Will re-render on next poll; force immediate by calling updateBarChart with last data
  });

  // Visit
  document.getElementById("btn-visit").addEventListener("click", withPending(async () => {
    const node = document.getElementById("visit-node").value;
    const metric = document.getElementById("visit-metric").value;
    const n = parseInt(document.getElementById("visit-n").value, 10);
    try {
      await postJSON("/api/visit", { node, metric, n });
      showToast(`Visit sent to ${esc(node)} (+${n} ${esc(metric)})`, "ok");
    } catch (e) {
      showToast(`Visit failed: ${esc(e.message)}`, "error");
    }
  }));

  // Load
  document.getElementById("btn-load").addEventListener("click", withPending(async () => {
    const rate = parseFloat(document.getElementById("load-rate").value);
    const duration = parseFloat(document.getElementById("load-dur").value);
    const metric = document.getElementById("load-metric").value;
    try {
      await postJSON("/api/load", { rate, duration, metric });
      showToast(`Load test started: ${rate} req/s for ${duration}s`, "ok");
    } catch (e) {
      showToast(`Load failed: ${esc(e.message)}`, "error");
    }
  }));

  // Partition
  document.getElementById("btn-partition").addEventListener("click", withPending(async () => {
    const groupA = [...document.querySelectorAll("#partition-group-a input:checked")].map(c => c.value);
    const groupB = [...document.querySelectorAll("#partition-group-b input:checked")].map(c => c.value);
    if (groupA.length === 0 || groupB.length === 0) {
      showToast("Select at least one node in each group", "error");
      return;
    }
    try {
      await postJSON("/api/partition", { groups: [groupA, groupB] });
      showToast(`Partitioned: [${groupA.join(",")}] | [${groupB.join(",")}]`, "ok");
    } catch (e) {
      showToast(`Partition failed: ${esc(e.message)}`, "error");
    }
  }));

  // Heal
  document.getElementById("btn-heal").addEventListener("click", withPending(async () => {
    try {
      await postJSON("/api/heal", {});
      showToast("All nodes healed", "ok");
    } catch (e) {
      showToast(`Heal failed: ${esc(e.message)}`, "error");
    }
  }));

  // Isolate
  document.getElementById("btn-isolate").addEventListener("click", withPending(async () => {
    const node = document.getElementById("iso-node").value;
    try {
      await postJSON("/api/isolate", { node });
      showToast(`Isolated ${esc(node)}`, "ok");
    } catch (e) {
      showToast(`Isolate failed: ${esc(e.message)}`, "error");
    }
  }));

  // Rejoin
  document.getElementById("btn-rejoin").addEventListener("click", withPending(async () => {
    const node = document.getElementById("iso-node").value;
    try {
      await postJSON("/api/rejoin", { node });
      showToast(`Rejoined ${esc(node)}`, "ok");
    } catch (e) {
      showToast(`Rejoin failed: ${esc(e.message)}`, "error");
    }
  }));

  // Kill
  document.getElementById("btn-kill").addEventListener("click", withPending(async () => {
    const node = document.getElementById("kill-node").value;
    try {
      await postJSON("/api/kill", { node });
      showToast(`Killed ${esc(node)}`, "ok");
    } catch (e) {
      showToast(`Kill failed: ${esc(e.message)}`, "error");
    }
  }));

  // Revive
  document.getElementById("btn-revive").addEventListener("click", withPending(async () => {
    const node = document.getElementById("kill-node").value;
    try {
      await postJSON("/api/revive", { node });
      showToast(`Revived ${esc(node)}`, "ok");
    } catch (e) {
      showToast(`Revive failed: ${esc(e.message)}`, "error");
    }
  }));
});
