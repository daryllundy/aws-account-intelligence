from __future__ import annotations


def render_dashboard_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>AWS Account Intelligence</title>
    <style>
      :root {
        --bg: #f5efe4;
        --bg-deep: #e2d1b4;
        --surface: rgba(255, 250, 241, 0.82);
        --surface-strong: rgba(255, 248, 236, 0.96);
        --ink: #231b13;
        --muted: #6a5947;
        --accent: #0d7c66;
        --accent-strong: #084c41;
        --warm: #c75b39;
        --line: rgba(35, 27, 19, 0.12);
        --shadow: 0 18px 60px rgba(66, 41, 14, 0.14);
        --radius: 24px;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        color: var(--ink);
        font-family: "Avenir Next Condensed", "Franklin Gothic Medium", "Arial Narrow", sans-serif;
        background:
          radial-gradient(circle at top left, rgba(13, 124, 102, 0.16), transparent 35%),
          radial-gradient(circle at top right, rgba(199, 91, 57, 0.18), transparent 28%),
          linear-gradient(160deg, var(--bg) 0%, #efe5d3 45%, var(--bg-deep) 100%);
        min-height: 100vh;
      }
      h1, h2, h3, .metric-value {
        font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
        letter-spacing: -0.03em;
        margin: 0;
      }
      .shell {
        max-width: 1400px;
        margin: 0 auto;
        padding: 28px 20px 64px;
      }
      .hero {
        display: grid;
        grid-template-columns: 1.3fr 0.9fr;
        gap: 18px;
        align-items: stretch;
      }
      .panel {
        background: var(--surface);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.55);
        border-radius: var(--radius);
        box-shadow: var(--shadow);
        overflow: hidden;
      }
      .hero-main {
        padding: 28px;
        position: relative;
      }
      .hero-main::after {
        content: "";
        position: absolute;
        inset: auto -10% -35% 35%;
        height: 180px;
        background: linear-gradient(90deg, rgba(13, 124, 102, 0), rgba(13, 124, 102, 0.18), rgba(199, 91, 57, 0));
        transform: rotate(-10deg);
      }
      .eyebrow {
        text-transform: uppercase;
        letter-spacing: 0.14em;
        font-size: 12px;
        color: var(--accent-strong);
        margin-bottom: 12px;
      }
      .hero-main p, .subtle, label {
        color: var(--muted);
      }
      .hero-main p {
        max-width: 48rem;
        line-height: 1.45;
        margin: 12px 0 0;
        font-size: 16px;
      }
      .hero-side {
        padding: 24px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        gap: 16px;
        background:
          linear-gradient(150deg, rgba(13, 124, 102, 0.1), rgba(255, 250, 241, 0.8)),
          var(--surface-strong);
      }
      .scan-meta {
        display: grid;
        gap: 12px;
      }
      .scan-chip {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 8px 12px;
        border-radius: 999px;
        background: rgba(13, 124, 102, 0.12);
        color: var(--accent-strong);
        font-weight: 700;
        width: fit-content;
      }
      .metrics {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 16px;
        margin-top: 18px;
      }
      .metric {
        padding: 18px;
        border-radius: 20px;
        background: rgba(255, 250, 241, 0.74);
        border: 1px solid rgba(35, 27, 19, 0.08);
        animation: rise 480ms ease both;
      }
      .metric:nth-child(2) { animation-delay: 70ms; }
      .metric:nth-child(3) { animation-delay: 140ms; }
      .metric:nth-child(4) { animation-delay: 210ms; }
      .metric-label {
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.14em;
        color: var(--muted);
      }
      .metric-value {
        font-size: clamp(28px, 3vw, 42px);
        margin-top: 8px;
      }
      .content {
        display: grid;
        grid-template-columns: 1.2fr 0.8fr;
        gap: 18px;
        margin-top: 18px;
      }
      .inventory-panel, .graph-panel, .impact-panel {
        padding: 24px;
      }
      .section-head {
        display: flex;
        justify-content: space-between;
        gap: 12px;
        align-items: end;
        margin-bottom: 18px;
      }
      .filters {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 12px;
        margin-bottom: 18px;
      }
      input, select, button {
        width: 100%;
        border-radius: 14px;
        border: 1px solid var(--line);
        padding: 12px 14px;
        font: inherit;
        background: rgba(255,255,255,0.7);
        color: var(--ink);
      }
      button {
        cursor: pointer;
        font-weight: 700;
        background: linear-gradient(135deg, var(--accent), var(--accent-strong));
        color: #f8f5ef;
        border: none;
      }
      table {
        width: 100%;
        border-collapse: collapse;
      }
      thead th {
        text-align: left;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.14em;
        padding-bottom: 10px;
        color: var(--muted);
      }
      tbody tr {
        border-top: 1px solid rgba(35, 27, 19, 0.08);
        transition: transform 180ms ease, background 180ms ease;
      }
      tbody tr:hover {
        transform: translateX(4px);
        background: rgba(13, 124, 102, 0.05);
      }
      tbody td {
        padding: 14px 8px;
        vertical-align: top;
      }
      .resource-button {
        border: none;
        background: none;
        padding: 0;
        text-align: left;
        color: var(--ink);
        cursor: pointer;
        font-weight: 700;
      }
      .status-pill, .edge-pill, .factor-pill {
        display: inline-flex;
        padding: 6px 10px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 700;
      }
      .status-ACTIVE { background: rgba(13, 124, 102, 0.14); color: var(--accent-strong); }
      .status-IDLE { background: rgba(199, 91, 57, 0.12); color: var(--warm); }
      .status-UNKNOWN { background: rgba(35, 27, 19, 0.08); color: var(--muted); }
      .edge-pill, .factor-pill {
        background: rgba(35, 27, 19, 0.08);
        color: var(--ink);
      }
      .graph-stage {
        border-radius: 20px;
        background: linear-gradient(180deg, rgba(255,255,255,0.9), rgba(240, 231, 216, 0.8));
        border: 1px solid rgba(35, 27, 19, 0.08);
        padding: 14px;
      }
      .graph-svg {
        width: 100%;
        height: 320px;
        display: block;
      }
      .impact-card {
        border-radius: 20px;
        padding: 18px;
        background: linear-gradient(145deg, rgba(13, 124, 102, 0.08), rgba(255,255,255,0.88));
        border: 1px solid rgba(35, 27, 19, 0.08);
      }
      .impact-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 12px;
        margin-top: 14px;
      }
      .impact-list, .factor-list {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-top: 12px;
      }
      .chain-list {
        margin-top: 12px;
        display: grid;
        gap: 10px;
      }
      .chain-card {
        padding: 12px;
        border-radius: 16px;
        background: rgba(255, 255, 255, 0.68);
        border: 1px solid rgba(35, 27, 19, 0.08);
      }
      .muted-small {
        color: var(--muted);
        font-size: 13px;
      }
      .empty {
        padding: 20px;
        border-radius: 18px;
        background: rgba(255,255,255,0.7);
        border: 1px dashed rgba(35, 27, 19, 0.18);
        color: var(--muted);
      }
      @keyframes rise {
        from { opacity: 0; transform: translateY(12px); }
        to { opacity: 1; transform: translateY(0); }
      }
      @media (max-width: 1100px) {
        .hero, .content { grid-template-columns: 1fr; }
        .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      }
      @media (max-width: 720px) {
        .metrics, .filters, .impact-grid { grid-template-columns: 1fr; }
        .shell { padding: 16px 14px 40px; }
        .hero-main, .hero-side, .inventory-panel, .graph-panel, .impact-panel { padding: 18px; }
        table, thead, tbody, th, td, tr { display: block; }
        thead { display: none; }
        tbody tr { padding: 12px 0; }
        tbody td { padding: 6px 0; }
      }
    </style>
  </head>
  <body>
    <div class="shell">
      <section class="hero">
        <div class="panel hero-main">
          <div class="eyebrow">Single-Account Operations</div>
          <h1>AWS Account Intelligence</h1>
          <p>
            Inspect the latest scan snapshot, filter live inventory, review cost posture, and trace
            dependency blast radius before you shut anything down.
          </p>
          <div class="metrics">
            <div class="metric"><div class="metric-label">Resources</div><div class="metric-value" id="metric-resources">-</div></div>
            <div class="metric"><div class="metric-label">Monthly Cost</div><div class="metric-value" id="metric-cost">-</div></div>
            <div class="metric"><div class="metric-label">Warnings</div><div class="metric-value" id="metric-warnings">-</div></div>
            <div class="metric"><div class="metric-label">Edges</div><div class="metric-value" id="metric-edges">-</div></div>
          </div>
        </div>
        <div class="panel hero-side">
          <div>
            <div class="scan-chip" id="scan-status">Awaiting scan</div>
            <div class="scan-meta">
              <div>
                <div class="eyebrow">Latest Snapshot</div>
                <h2 id="scan-id">No scan loaded</h2>
              </div>
              <div class="subtle" id="scan-details">Run a scan from the CLI to populate this dashboard.</div>
            </div>
          </div>
          <button id="refresh-button" type="button">Refresh dashboard data</button>
        </div>
      </section>

      <section class="content">
        <div class="panel inventory-panel">
          <div class="section-head">
            <div>
              <div class="eyebrow">Inventory</div>
              <h2>Filterable Resource Ledger</h2>
            </div>
            <div class="subtle" id="inventory-count">0 resources</div>
          </div>
          <div class="filters">
            <input id="filter-search" type="search" placeholder="Search IDs, ARNs, tags" />
            <select id="filter-service"><option value="">All services</option></select>
            <select id="filter-region"><option value="">All regions</option></select>
            <select id="filter-status">
              <option value="">All statuses</option>
              <option value="ACTIVE">ACTIVE</option>
              <option value="IDLE">IDLE</option>
              <option value="UNKNOWN">UNKNOWN</option>
            </select>
          </div>
          <table>
            <thead>
              <tr>
                <th>Resource</th>
                <th>Service</th>
                <th>Region</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody id="inventory-body"></tbody>
          </table>
        </div>

        <div style="display:grid; gap:18px;">
          <div class="panel graph-panel">
            <div class="section-head">
              <div>
                <div class="eyebrow">Dependency Graph</div>
                <h2>Live Edge Map</h2>
              </div>
              <div class="subtle" id="graph-count">0 edges</div>
            </div>
            <div class="graph-stage">
              <svg class="graph-svg" id="graph-svg" viewBox="0 0 680 320" preserveAspectRatio="none"></svg>
            </div>
            <div class="chain-list" id="edge-list"></div>
          </div>

          <div class="panel impact-panel">
            <div class="section-head">
              <div>
                <div class="eyebrow">Impact Report</div>
                <h2>Shutdown Consequences</h2>
              </div>
              <div class="subtle" id="impact-target-label">Select a resource</div>
            </div>
            <div id="impact-root" class="empty">Choose a resource from inventory to load its impact analysis.</div>
          </div>
        </div>
      </section>
    </div>

    <script>
      const state = { scanId: null, inventory: [], graphEdges: [] };

      document.getElementById('refresh-button').addEventListener('click', () => loadDashboard());
      ['filter-search', 'filter-service', 'filter-region', 'filter-status'].forEach((id) => {
        document.getElementById(id).addEventListener('input', renderInventory);
        document.getElementById(id).addEventListener('change', renderInventory);
      });

      async function loadDashboard() {
        const latestResponse = await fetch('/scans/latest');
        if (latestResponse.status === 404) {
          renderEmptyState();
          return;
        }
        const scan = await latestResponse.json();
        state.scanId = scan.scan_run_id;
        document.getElementById('scan-status').textContent = scan.status.toUpperCase();
        document.getElementById('scan-id').textContent = scan.scan_run_id.slice(0, 8);
        document.getElementById('scan-details').textContent = `${scan.resource_count} resources across ${scan.regions.join(', ')}. Completed ${new Date(scan.completed_at).toLocaleString()}.`;
        document.getElementById('metric-resources').textContent = scan.resource_count;
        document.getElementById('metric-warnings').textContent = scan.summary.warning_count ?? 0;
        document.getElementById('metric-edges').textContent = scan.edge_count;

        const [inventoryResponse, costResponse, graphResponse] = await Promise.all([
          fetch(`/inventory?scan_run_id=${encodeURIComponent(scan.scan_run_id)}`),
          fetch(`/costs/summary?scan_run_id=${encodeURIComponent(scan.scan_run_id)}`),
          fetch(`/graph?scan_run_id=${encodeURIComponent(scan.scan_run_id)}`)
        ]);
        const inventory = await inventoryResponse.json();
        const costs = await costResponse.json();
        const graph = await graphResponse.json();

        state.inventory = inventory.services;
        state.graphEdges = Object.values(graph.adjacency).flat();

        document.getElementById('metric-cost').textContent = formatCurrency(costs.total_projected_monthly_cost_usd);
        populateFilterOptions();
        renderInventory();
        renderGraph();
      }

      function renderEmptyState() {
        document.getElementById('inventory-body').innerHTML = `<tr><td colspan="4"><div class="empty">No scans found. Run <code>aws-account-intel scan run</code> first.</div></td></tr>`;
        document.getElementById('impact-root').className = 'empty';
        document.getElementById('impact-root').textContent = 'No scan data is available yet.';
      }

      function populateFilterOptions() {
        const serviceSelect = document.getElementById('filter-service');
        const regionSelect = document.getElementById('filter-region');
        fillOptions(serviceSelect, [...new Set(state.inventory.map((item) => item.service_name))].sort());
        fillOptions(regionSelect, [...new Set(state.inventory.map((item) => item.region))].sort());
      }

      function fillOptions(select, values) {
        const current = select.value;
        select.innerHTML = `<option value="">${select.id.includes('service') ? 'All services' : 'All regions'}</option>`;
        values.forEach((value) => {
          const option = document.createElement('option');
          option.value = value;
          option.textContent = value;
          if (value === current) option.selected = true;
          select.appendChild(option);
        });
      }

      function filteredInventory() {
        const search = document.getElementById('filter-search').value.trim().toLowerCase();
        const service = document.getElementById('filter-service').value;
        const region = document.getElementById('filter-region').value;
        const status = document.getElementById('filter-status').value;
        return state.inventory.filter((item) => {
          if (service && item.service_name !== service) return false;
          if (region && item.region !== region) return false;
          if (status && item.status !== status) return false;
          if (!search) return true;
          return item.resource_id.toLowerCase().includes(search)
            || item.arn.toLowerCase().includes(search)
            || Object.values(item.tags).some((value) => String(value).toLowerCase().includes(search));
        });
      }

      function renderInventory() {
        const rows = filteredInventory();
        document.getElementById('inventory-count').textContent = `${rows.length} resources`;
        const body = document.getElementById('inventory-body');
        if (!rows.length) {
          body.innerHTML = `<tr><td colspan="4"><div class="empty">No resources match the current filters.</div></td></tr>`;
          return;
        }
        body.innerHTML = rows.map((item) => `
          <tr>
            <td>
              <button class="resource-button" data-resource="${escapeHtml(item.resource_id)}">${escapeHtml(compactId(item.resource_id))}</button>
              <div class="muted-small">${escapeHtml(item.resource_type)}</div>
            </td>
            <td>${escapeHtml(item.service_name)}</td>
            <td>${escapeHtml(item.region)}</td>
            <td><span class="status-pill status-${item.status}">${item.status}</span></td>
          </tr>
        `).join('');
        body.querySelectorAll('.resource-button').forEach((button) => {
          button.addEventListener('click', () => loadImpact(button.dataset.resource));
        });
      }

      function renderGraph() {
        const edges = state.graphEdges.slice(0, 10);
        document.getElementById('graph-count').textContent = `${state.graphEdges.length} edges`;
        const svg = document.getElementById('graph-svg');
        const edgeList = document.getElementById('edge-list');
        if (!edges.length) {
          svg.innerHTML = '';
          edgeList.innerHTML = `<div class="empty">No dependency edges were recorded for the selected scan.</div>`;
          return;
        }
        const left = [...new Set(edges.map((edge) => edge.from_resource_id))].slice(0, 6);
        const right = [...new Set(edges.map((edge) => edge.to_resource_id))].slice(0, 6);
        const leftPositions = Object.fromEntries(left.map((item, index) => [item, 40 + index * ((240) / Math.max(left.length - 1, 1))]));
        const rightPositions = Object.fromEntries(right.map((item, index) => [item, 40 + index * ((240) / Math.max(right.length - 1, 1))]));
        const lines = edges.filter((edge) => leftPositions[edge.from_resource_id] !== undefined && rightPositions[edge.to_resource_id] !== undefined).map((edge) => `
          <line x1="180" y1="${leftPositions[edge.from_resource_id]}" x2="500" y2="${rightPositions[edge.to_resource_id]}" stroke="rgba(13,124,102,0.45)" stroke-width="${Math.max(1.4, edge.confidence * 3.2)}" />
        `).join('');
        const leftLabels = left.map((item) => `
          <text x="12" y="${leftPositions[item] + 4}" fill="#231b13" font-size="12">${escapeHtml(compactId(item))}</text>
          <circle cx="176" cy="${leftPositions[item]}" r="5" fill="#c75b39"></circle>
        `).join('');
        const rightLabels = right.map((item) => `
          <text x="514" y="${rightPositions[item] + 4}" fill="#231b13" font-size="12">${escapeHtml(compactId(item))}</text>
          <circle cx="504" cy="${rightPositions[item]}" r="5" fill="#0d7c66"></circle>
        `).join('');
        svg.innerHTML = `<rect x="0" y="0" width="680" height="320" rx="18" fill="rgba(255,255,255,0.28)"></rect>${lines}${leftLabels}${rightLabels}`;
        edgeList.innerHTML = edges.map((edge) => `
          <div class="chain-card">
            <div><strong>${escapeHtml(compactId(edge.from_resource_id))}</strong> <span class="muted-small">to</span> <strong>${escapeHtml(compactId(edge.to_resource_id))}</strong></div>
            <div class="impact-list">
              <span class="edge-pill">${escapeHtml(edge.edge_type)}</span>
              <span class="edge-pill">${escapeHtml(edge.evidence_source)}</span>
              <span class="edge-pill">confidence ${Math.round(edge.confidence * 100)}%</span>
            </div>
          </div>
        `).join('');
      }

      async function loadImpact(resourceId) {
        document.getElementById('impact-target-label').textContent = compactId(resourceId);
        const response = await fetch(`/impact?scan_run_id=${encodeURIComponent(state.scanId)}&resource=${encodeURIComponent(resourceId)}`);
        if (!response.ok) {
          document.getElementById('impact-root').className = 'empty';
          document.getElementById('impact-root').textContent = 'Impact report could not be loaded.';
          return;
        }
        const report = await response.json();
        const factors = report.risk_factors.length
          ? report.risk_factors.map((factor) => `<span class="factor-pill">${escapeHtml(factor)}</span>`).join('')
          : '<span class="factor-pill">no-risk-factors</span>';
        const direct = renderNodes(report.direct_dependents);
        const transitive = renderNodes(report.transitive_dependents);
        document.getElementById('impact-root').className = 'impact-card';
        document.getElementById('impact-root').innerHTML = `
          <h3>${escapeHtml(compactId(report.target_resource_id))}</h3>
          <div class="muted-small">${escapeHtml(report.rationale)}</div>
          <div class="impact-grid">
            <div>
              <div class="eyebrow">Risk Score</div>
              <div class="metric-value">${escapeHtml(report.risk_score)}</div>
            </div>
            <div>
              <div class="eyebrow">Savings</div>
              <div class="metric-value">${formatCurrency(report.estimated_monthly_savings_usd)}</div>
            </div>
          </div>
          <div class="factor-list">${factors}</div>
          <div class="chain-list">
            <div class="chain-card"><strong>Direct dependents</strong>${direct}</div>
            <div class="chain-card"><strong>Transitive dependents</strong>${transitive}</div>
          </div>
        `;
      }

      function renderNodes(nodes) {
        if (!nodes.length) {
          return '<div class="muted-small">None</div>';
        }
        return nodes.slice(0, 6).map((node) => `
          <div style="margin-top:10px;">
            <div><strong>${escapeHtml(compactId(node.resource_id))}</strong> <span class="muted-small">(${escapeHtml(node.service_name)})</span></div>
            <div class="impact-list">
              <span class="edge-pill">depth ${node.path_depth}</span>
              ${node.edge_type ? `<span class="edge-pill">${escapeHtml(node.edge_type)}</span>` : ''}
              ${node.is_critical ? '<span class="edge-pill">critical</span>' : ''}
            </div>
          </div>
        `).join('');
      }

      function compactId(value) {
        if (value.length <= 42) return value;
        return `${value.slice(0, 18)}…${value.slice(-16)}`;
      }

      function formatCurrency(value) {
        return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(value || 0);
      }

      function escapeHtml(value) {
        return String(value)
          .replaceAll('&', '&amp;')
          .replaceAll('<', '&lt;')
          .replaceAll('>', '&gt;')
          .replaceAll('"', '&quot;')
          .replaceAll("'", '&#39;');
      }

      loadDashboard();
    </script>
  </body>
</html>"""
