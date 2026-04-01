"""Fetch raw grain data from BigQuery and generate a self-contained HTML report.

Usage:
    python generate_report.py

Output:
    mapbox_report.html  — open in any browser, no server needed.
"""
import json
from pathlib import Path
from google.cloud import bigquery

BQ_TABLE = "wmt-driver-insights.Chirag_dx.HB_mapbox_implementation_base_table"
OUT_FILE = Path(__file__).parent / "mapbox_report.html"


# ---------------------------------------------------------------------------
# 1. Fetch
# ---------------------------------------------------------------------------

def fetch_raw() -> list[dict]:
    """Return raw metric sums at the finest grain we need for client-side filtering."""
    client = bigquery.Client()
    sql = f"""
        SELECT
            CAST(wm_wk AS STRING)                              AS wm_wk,
            Test_Control,
            UPPER(COALESCE(RECOMMENDEDLATLONGSOURCE, 'UNKNOWN')) AS source,
            COALESCE(n_addresstype, 'UNKNOWN')                 AS address_type,
            COALESCE(CAST(Rollout_Percentage AS STRING), '')    AS rollout_pct,
            -- raw sums only; ratios are computed client-side
            SUM(total_orders)                                  AS total_orders,
            SUM(perfect_orders)                                AS perfect_orders,
            SUM(missing_orders)                                AS missing_orders,
            SUM(contact_num)                                   AS contact_num,
            SUM(contact_den)                                   AS contact_den,
            SUM(contact_cant_find_add_num)                     AS contact_cant_find_num,
            SUM(contact_cant_confirm_arrival_num)              AS contact_cant_confirm_num,
            SUM(FC_num)                                        AS fc_num,
            SUM(FC_den)                                        AS fc_den,
            SUM(RETURNED_PO)                                   AS returned_po,
            SUM(DISPATCHED_PO)                                 AS dispatched_po,
            SUM(RETURNED_PO_CANT_FIND_ADDRESS)                 AS returned_cant_find,
            SUM(RETURNED_PO_LOCATION_ISSUE)                    AS returned_cant_confirm
        FROM `{BQ_TABLE}`
        GROUP BY 1,2,3,4,5
        ORDER BY 1,2,3,4,5
    """
    rows = client.query(sql).result()
    data = []
    for row in rows:
        d = {}
        for k, v in dict(row).items():
            if v is None:
                d[k] = None
            elif isinstance(v, (int, float)):
                d[k] = float(v)
            else:
                d[k] = str(v)
        data.append(d)
    print(f"  Fetched {len(data):,} rows from BigQuery")
    return data


# ---------------------------------------------------------------------------
# 2. Build distinct filter values from data
# ---------------------------------------------------------------------------

def build_filter_meta(data: list[dict]) -> dict:
    wm_wks        = sorted({r["wm_wk"]       for r in data if r["wm_wk"]})
    address_types = sorted({r["address_type"] for r in data if r["address_type"]})
    rollout_pcts  = sorted({r["rollout_pct"]  for r in data if r["rollout_pct"]},
                           key=lambda v: float(v) if v.replace(".","").isdigit() else 9999)
    ctrl_sources  = sorted({r["source"] for r in data
                            if r["Test_Control"] == "Control" and r["source"] not in ("MAPBOX", "UNKNOWN")})
    return dict(
        wm_wks=wm_wks,
        address_types=address_types,
        rollout_pcts=rollout_pcts,
        ctrl_sources=ctrl_sources,
    )


# ---------------------------------------------------------------------------
# 3. Generate HTML
# ---------------------------------------------------------------------------

HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Mapbox Experiment Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  .chart-wrap{position:relative;height:200px}
  .spinner{border:3px solid #e5e7eb;border-top-color:#0053e2;border-radius:50%;width:24px;height:24px;animation:spin .7s linear infinite}
  @keyframes spin{to{transform:rotate(360deg)}}
  select[multiple]{height:90px}
</style>
</head>
<body class="bg-gray-50 text-gray-800 min-h-screen">

<!-- HEADER -->
<header class="px-6 py-4 flex items-center gap-3 shadow text-white" style="background:#0053e2">
  <div>
    <h1 class="text-lg font-bold">Mapbox Experiment Dashboard</h1>
    <p class="text-blue-200 text-xs">Test = Mapbox &nbsp;|&nbsp; Control = configurable &nbsp;|&nbsp; LMD Analytics</p>
  </div>
  <div class="ml-auto flex items-center gap-2">
    <div id="spinner" class="spinner hidden"></div>
    <span id="last_updated" class="text-blue-200 text-xs"></span>
  </div>
</header>

<!-- FILTERS -->
<section class="bg-white border-b px-6 py-4 shadow-sm">
  <div class="max-w-7xl mx-auto grid grid-cols-2 md:grid-cols-4 xl:grid-cols-6 gap-4 items-end">

    <div class="col-span-1">
      <label class="block text-xs font-semibold text-gray-500 mb-1">Week From</label>
      <select id="wk_from" class="w-full border rounded px-2 py-1.5 text-sm"></select>
    </div>
    <div class="col-span-1">
      <label class="block text-xs font-semibold text-gray-500 mb-1">Week To</label>
      <select id="wk_to" class="w-full border rounded px-2 py-1.5 text-sm"></select>
    </div>

    <div class="col-span-1">
      <label class="block text-xs font-semibold text-gray-500 mb-1">Address Type <span class="text-gray-400 font-normal">(multi)</span></label>
      <select id="addr_type" multiple class="w-full border rounded px-2 py-1 text-sm"></select>
    </div>

    <div class="col-span-1">
      <label class="block text-xs font-semibold text-gray-500 mb-1">Control Source</label>
      <select id="ctrl_src" class="w-full border rounded px-2 py-1.5 text-sm"></select>
    </div>

    <div class="col-span-1">
      <label class="block text-xs font-semibold text-gray-500 mb-1">Rollout %</label>
      <select id="rollout_pct" class="w-full border rounded px-2 py-1.5 text-sm"></select>
    </div>

    <div class="col-span-1 flex gap-2 items-end">
      <button onclick="applyFilters()" class="flex-1 py-1.5 rounded font-semibold text-sm text-white transition" style="background:#0053e2">Apply</button>
      <button onclick="resetFilters()" class="flex-1 py-1.5 rounded font-semibold text-sm bg-gray-100 text-gray-700 hover:bg-gray-200 transition">Reset</button>
    </div>

  </div>
</section>

<!-- KPI TABLE -->
<main class="max-w-7xl mx-auto px-6 py-6 space-y-8">
  <div class="bg-white rounded-lg shadow overflow-hidden">
    <table class="w-full text-sm">
      <thead>
        <tr class="text-white" style="background:#0053e2">
          <th class="text-left px-5 py-3 font-semibold w-2/5">Metric</th>
          <th class="text-right px-5 py-3 font-semibold">\u{1f9ea} Mapbox (Test)</th>
          <th class="text-right px-5 py-3 font-semibold">\u{1f39b} Control</th>
          <th class="text-right px-5 py-3 font-semibold">Δ Test vs Control</th>
        </tr>
      </thead>
      <tbody id="kpi_body"><tr><td colspan="4" class="text-center text-gray-400 py-10">Loading…</td></tr></tbody>
    </table>
  </div>

  <h2 class="text-base font-bold text-gray-700 border-b pb-2">Weekly Trends</h2>
  <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6" id="charts_grid"></div>
</main>

<footer class="text-center text-xs text-gray-400 py-6">
  Mapbox Experiment Dashboard &bull; LMD Analytics &bull;
  <code>wmt-driver-insights.Chirag_dx.HB_mapbox_implementation_base_table</code>
</footer>

<script>
// ── Embedded data (generated by generate_report.py) ──────────────────────
const RAW_DATA = __RAW_DATA__;
const META     = __META__;
const GENERATED_AT = "__GENERATED_AT__";

// ── Constants ─────────────────────────────────────────────────────────────
const BLUE  = '#0053e2';
const AMBER = '#995213';
const METRIC_KEYS = [
  'total_orders','pct_perfect_orders','pct_missing_orders',
  'pct_contacts','pct_contact_cant_find','pct_contact_cant_confirm',
  'pct_force_complete','pct_returned','pct_return_cant_find','pct_return_cant_confirm'
];
const METRIC_LABELS = {
  total_orders:             'Total Orders',
  pct_perfect_orders:       '% Perfect Orders',
  pct_missing_orders:       '% Missing Orders',
  pct_contacts:             '% Contacts',
  pct_contact_cant_find:    '% Contact — Cant Find Address',
  pct_contact_cant_confirm: '% Contact — Cant Confirm Arrival',
  pct_force_complete:       '% Force Complete',
  pct_returned:             '% Returned PO',
  pct_return_cant_find:     '% Return — Cant Find Address',
  pct_return_cant_confirm:  '% Return — Cant Confirm Arrival',
};
const charts = {};

// ── Metric computation (SAFE_DIVIDE client-side) ──────────────────────────
function sd(a, b) { return b ? a / b : null; }

function computeMetrics(rows) {
  const s = {
    total_orders:0, perfect_orders:0, missing_orders:0,
    contact_num:0, contact_den:0,
    contact_cant_find_num:0, contact_cant_confirm_num:0,
    fc_num:0, fc_den:0,
    returned_po:0, dispatched_po:0,
    returned_cant_find:0, returned_cant_confirm:0
  };
  for (const r of rows) for (const k of Object.keys(s)) s[k] += r[k] || 0;
  return {
    total_orders:             s.total_orders,
    pct_perfect_orders:       sd(s.perfect_orders, s.total_orders) * 100,
    pct_missing_orders:       sd(s.missing_orders,  s.total_orders) * 100,
    pct_contacts:             sd(s.contact_num,  s.contact_den)  * 100,
    pct_contact_cant_find:    sd(s.contact_cant_find_num,    s.contact_den) * 100,
    pct_contact_cant_confirm: sd(s.contact_cant_confirm_num, s.contact_den) * 100,
    pct_force_complete:       sd(s.fc_num, s.fc_den) * 100,
    pct_returned:             sd(s.returned_po, s.dispatched_po) * 100,
    pct_return_cant_find:     sd(s.returned_cant_find,    s.dispatched_po) * 100,
    pct_return_cant_confirm:  sd(s.returned_cant_confirm, s.dispatched_po) * 100,
  };
}

// ── Filter helpers ────────────────────────────────────────────────────────
function getSelectedMulti(id) {
  return [...document.getElementById(id).selectedOptions].map(o => o.value);
}

function filterRows() {
  const wkFrom  = document.getElementById('wk_from').value;
  const wkTo    = document.getElementById('wk_to').value;
  const addrSel = getSelectedMulti('addr_type');
  const ctrlSrc = document.getElementById('ctrl_src').value;   // single
  const rollout = document.getElementById('rollout_pct').value; // single, '' = all

  return RAW_DATA.filter(r => {
    if (r.wm_wk < wkFrom || r.wm_wk > wkTo) return false;
    if (addrSel.length && !addrSel.includes(r.address_type))  return false;
    if (rollout && r.rollout_pct !== rollout)                  return false;
    // Test: always MAPBOX source; Control: match selected ctrl_src
    if (r.Test_Control === 'Test')    return r.source === 'MAPBOX';
    if (r.Test_Control === 'Control') return r.source === ctrlSrc;
    return false;
  });
}

// ── Populate filter controls ──────────────────────────────────────────────
function rolloutLabel(v) { return isNaN(Number(v)) ? v : v + '%'; }

function populateFilters() {
  // Week selectors
  const wkFrom = document.getElementById('wk_from');
  const wkTo   = document.getElementById('wk_to');
  META.wm_wks.forEach(w => {
    wkFrom.add(new Option(w, w));
    wkTo.add(new Option(w, w));
  });
  wkFrom.value = META.wm_wks.at(0);
  wkTo.value   = META.wm_wks.at(-1);

  // Address type
  const addrEl = document.getElementById('addr_type');
  META.address_types.forEach(t => {
    const opt = new Option(t, t);
    opt.selected = true;
    addrEl.add(opt);
  });

  // Control source
  const ctrlEl = document.getElementById('ctrl_src');
  META.ctrl_sources.forEach(s => ctrlEl.add(new Option(s, s)));
  ctrlEl.value = META.ctrl_sources.includes('GOOGLE') ? 'GOOGLE' : META.ctrl_sources[0];

  // Rollout %
  const rollEl = document.getElementById('rollout_pct');
  rollEl.add(new Option('All', ''));
  META.rollout_pcts.forEach(v => rollEl.add(new Option(rolloutLabel(v), v)));
  rollEl.value = '';

  // Generated-at banner
  document.getElementById('last_updated').textContent = 'Data as of ' + GENERATED_AT;
}

// ── KPI table ─────────────────────────────────────────────────────────────
function deltaClass(key, delta) {
  if (delta === null) return '';
  const lowerBetter = key.includes('missing') || key.includes('return') ||
                      key.includes('force')   || key.includes('contact');
  if (lowerBetter) return delta > 0 ? 'text-red-500 font-semibold' : 'text-green-600 font-semibold';
  return delta > 0 ? 'text-green-600 font-semibold' : 'text-red-500 font-semibold';
}

function fmt(v, isCount) {
  if (v === null || v === undefined) return '—';
  return isCount ? Number(v).toLocaleString() : Number(v).toFixed(2) + '%';
}

function renderKPIs(test, ctrl) {
  const tbody = document.getElementById('kpi_body');
  let html = '';
  METRIC_KEYS.forEach((m, i) => {
    const isCount = m === 'total_orders';
    const tv = test[m] ?? null;
    const cv = ctrl[m] ?? null;
    const delta = tv !== null && cv !== null ? tv - cv : null;
    const sign  = delta > 0 ? '+' : '';
    const dc    = deltaClass(m, delta);
    const bg    = i % 2 === 0 ? 'bg-white' : 'bg-gray-50';
    html += `<tr class="${bg} border-t border-gray-100 hover:bg-blue-50">
      <td class="px-5 py-2.5 font-medium text-gray-700">${METRIC_LABELS[m]}</td>
      <td class="px-5 py-2.5 text-right font-mono" style="color:#0053e2">${fmt(tv, isCount)}</td>
      <td class="px-5 py-2.5 text-right font-mono" style="color:#995213">${fmt(cv, isCount)}</td>
      <td class="px-5 py-2.5 text-right font-mono ${dc}">${delta !== null
        ? sign + (isCount ? Math.round(delta).toLocaleString() : delta.toFixed(2) + '%')
        : '—'}</td>
    </tr>`;
  });
  tbody.innerHTML = html;
}

// ── Charts ────────────────────────────────────────────────────────────────
function buildCharts(weeks, testRows, ctrlRows) {
  const grid = document.getElementById('charts_grid');
  // Build chart cards if they don't exist yet
  if (!grid.children.length) {
    METRIC_KEYS.forEach(m => {
      const card = document.createElement('div');
      card.className = 'bg-white rounded-lg shadow p-4';
      card.innerHTML = `<p class="text-xs font-semibold text-gray-500 mb-2">${METRIC_LABELS[m]}</p>
        <div class="chart-wrap"><canvas id="c_${m}"></canvas></div>`;
      grid.appendChild(card);
    });
  }

  METRIC_KEYS.forEach(m => {
    const isCount   = m === 'total_orders';
    const testVals  = testRows.map(r => r ? (r[m] ?? null) : null);
    const ctrlVals  = ctrlRows.map(r => r ? (r[m] ?? null) : null);
    const formatTick = v => isCount ? Number(v).toLocaleString() : v.toFixed(2) + '%';

    if (charts[m]) {
      charts[m].data.labels         = weeks;
      charts[m].data.datasets[0].data = testVals;
      charts[m].data.datasets[1].data = ctrlVals;
      charts[m].update();
    } else {
      charts[m] = new Chart(document.getElementById(`c_${m}`), {
        type: 'line',
        data: {
          labels: weeks,
          datasets: [
            { label:'Mapbox (Test)',    data:testVals, borderColor:BLUE,  backgroundColor:BLUE +'22', tension:0.3, pointRadius:3, fill:false },
            { label:'Control',         data:ctrlVals, borderColor:AMBER, backgroundColor:AMBER+'22', tension:0.3, pointRadius:3, fill:false },
          ]
        },
        options: {
          responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ labels:{ font:{size:10} } } },
          scales:{
            x:{ ticks:{ font:{size:9}, maxRotation:45 } },
            y:{ ticks:{ font:{size:9}, callback:formatTick } }
          }
        }
      });
    }
  });
}

// ── Main render ───────────────────────────────────────────────────────────
function applyFilters() {
  document.getElementById('spinner').classList.remove('hidden');
  requestAnimationFrame(() => {
    const filtered = filterRows();
    const testRows = filtered.filter(r => r.Test_Control === 'Test');
    const ctrlRows = filtered.filter(r => r.Test_Control === 'Control');
    const test = computeMetrics(testRows);
    const ctrl = computeMetrics(ctrlRows);
    renderKPIs(test, ctrl);

    // Weekly trends
    const wkFrom = document.getElementById('wk_from').value;
    const wkTo   = document.getElementById('wk_to').value;
    const weeks  = META.wm_wks.filter(w => w >= wkFrom && w <= wkTo);
    const testWkMetrics = weeks.map(w => computeMetrics(testRows.filter(r => r.wm_wk === w)));
    const ctrlWkMetrics = weeks.map(w => computeMetrics(ctrlRows.filter(r => r.wm_wk === w)));
    buildCharts(weeks, testWkMetrics, ctrlWkMetrics);

    document.getElementById('spinner').classList.add('hidden');
  });
}

function resetFilters() {
  document.getElementById('wk_from').value = META.wm_wks.at(0);
  document.getElementById('wk_to').value   = META.wm_wks.at(-1);
  [...document.getElementById('addr_type').options].forEach(o => o.selected = true);
  const ctrlEl = document.getElementById('ctrl_src');
  ctrlEl.value = META.ctrl_sources.includes('GOOGLE') ? 'GOOGLE' : ctrlEl.options[0]?.value;
  document.getElementById('rollout_pct').value = '';
  applyFilters();
}

// ── Boot ──────────────────────────────────────────────────────────────────
populateFilters();
applyFilters();
</script>
</body>
</html>
"""


def generate_html(data: list[dict], meta: dict) -> str:
    from datetime import datetime, timezone
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        HTML_TEMPLATE
        .replace("__RAW_DATA__",    json.dumps(data,              separators=(",", ":")))
        .replace("__META__",        json.dumps(meta,              separators=(",", ":")))
        .replace("__GENERATED_AT__", generated_at)
    )


# ---------------------------------------------------------------------------
# 4. Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Fetching data from BigQuery...")
    data = fetch_raw()
    meta = build_filter_meta(data)
    print(f"  Weeks: {meta['wm_wks']}")
    print(f"  Address types: {meta['address_types']}")
    print(f"  Control sources: {meta['ctrl_sources']}")
    print(f"  Rollout %s: {meta['rollout_pcts']}")
    print("Generating HTML...")
    html = generate_html(data, meta)
    OUT_FILE.write_text(html, encoding="utf-8")
    print(f"  Saved: {OUT_FILE}")
    print(f"  Size : {OUT_FILE.stat().st_size / 1024:.1f} KB")
    print("Done! Open mapbox_report.html in any browser.")