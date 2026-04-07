"""Fetch raw grain data from BigQuery and generate a self-contained HTML report.

Usage:
    python generate_report.py

Output:
    mapbox_report.html
"""
import json
from decimal import Decimal
from pathlib import Path
from google.cloud import bigquery

BQ_TABLE  = "wmt-driver-insights.Chirag_dx.HB_mapbox_implementation_base_table"
OUT_FILE  = Path(__file__).parent / "mapbox_report.html"


# ---------------------------------------------------------------------------
# 1. Fetch raw grain data  (slot_dt + wm_wk + filters)
# ---------------------------------------------------------------------------

def _to_py(v):
    """Convert BQ row values to JSON-safe Python types."""
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (int, float)):
        return float(v)
    return str(v)


def fetch_raw(client: bigquery.Client) -> list[dict]:
    """Aggregate at wm_wk grain — keeps data small for the browser."""
    sql = f"""
        SELECT
            CAST(wm_wk   AS STRING)                             AS wm_wk,
            Test_Control,
            UPPER(COALESCE(RECOMMENDEDLATLONGSOURCE,'UNKNOWN')) AS source,
            COALESCE(n_addresstype,'UNKNOWN')                   AS address_type,
            COALESCE(CAST(Rollout_Percentage AS STRING),'')     AS rollout_pct,
            SUM(total_orders)                AS total_orders,
            SUM(perfect_orders)              AS perfect_orders,
            SUM(missing_orders)              AS missing_orders,
            SUM(contact_num)                 AS contact_num,
            SUM(contact_den)                 AS contact_den,
            SUM(contact_cant_find_add_num)   AS contact_cant_find_num,
            SUM(contact_cant_confirm_arrival_num) AS contact_cant_confirm_num,
            SUM(FC_num)                      AS fc_num,
            SUM(FC_den)                      AS fc_den,
            SUM(RETURNED_PO)                 AS returned_po,
            SUM(DISPATCHED_PO)               AS dispatched_po,
            SUM(RETURNED_PO_CANT_FIND_ADDRESS)   AS returned_cant_find,
            SUM(RETURNED_PO_LOCATION_ISSUE)      AS returned_cant_confirm
        FROM `{BQ_TABLE}`
        WHERE wm_wk IS NOT NULL
        GROUP BY 1,2,3,4,5
        ORDER BY 1,2,3,4,5
    """
    rows = client.query(sql).result()
    data = [{k: _to_py(v) for k, v in dict(r).items()} for r in rows]
    print(f"  Fetched {len(data):,} rows")
    return data


def fetch_week_dates(client: bigquery.Client) -> dict[str, dict]:
    """Return {wm_wk: {min: 'YYYY-MM-DD', max: 'YYYY-MM-DD'}} for date picker mapping."""
    sql = f"""
        SELECT
            CAST(wm_wk AS STRING)        AS wm_wk,
            CAST(MIN(slot_dt) AS STRING) AS min_date,
            CAST(MAX(slot_dt) AS STRING) AS max_date
        FROM `{BQ_TABLE}`
        WHERE wm_wk IS NOT NULL AND slot_dt IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """
    return {
        str(r.wm_wk): {"min": str(r.min_date), "max": str(r.max_date)}
        for r in client.query(sql).result()
    }


# ---------------------------------------------------------------------------
# 2. Build filter metadata
# ---------------------------------------------------------------------------

def build_meta(data: list[dict], wk_dates: dict) -> dict:
    wm_wks        = sorted({r["wm_wk"]   for r in data if r["wm_wk"]})
    address_types = sorted({r["address_type"] for r in data if r["address_type"]})
    ctrl_sources  = sorted({
        r["source"] for r in data
        if r["Test_Control"] == "Control" and r["source"] not in ("MAPBOX", "UNKNOWN")
    })
    rollout_pcts = sorted(
        {r["rollout_pct"] for r in data if r["rollout_pct"]},
        key=lambda v: float(v) if v.replace(".", "").isdigit() else 9999,
    )
    all_dates = [d for wd in wk_dates.values() for d in (wd["min"], wd["max"])]
    return dict(
        date_min=min(all_dates) if all_dates else "",
        date_max=max(all_dates) if all_dates else "",
        wm_wks=wm_wks,
        wk_dates=wk_dates,
        address_types=address_types,
        ctrl_sources=ctrl_sources,
        rollout_pcts=rollout_pcts,
    )


# ---------------------------------------------------------------------------
# 3. HTML template
# ---------------------------------------------------------------------------

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Mapbox Experiment Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
.chart-wrap{position:relative;height:200px}
.dd-panel{display:none;position:absolute;z-index:50;background:#fff;border:1px solid #d1d5db;
  border-radius:6px;box-shadow:0 4px 12px rgba(0,0,0,.12);min-width:180px;max-height:220px;
  overflow-y:auto;padding:6px 0}
.dd-panel.open{display:block}
.dd-item{display:flex;align-items:center;gap:8px;padding:5px 12px;cursor:pointer;font-size:.8rem}
.dd-item:hover{background:#eff6ff}
.dd-item input{accent-color:#0053e2}
.dd-btn{border:1px solid #d1d5db;border-radius:6px;padding:5px 10px;font-size:.8rem;
  background:#fff;cursor:pointer;white-space:nowrap;display:flex;align-items:center;gap:6px}
.dd-btn:hover{border-color:#0053e2}
.dd-wrap{position:relative;display:inline-block}
</style>
</head>
<body class="bg-gray-50 text-gray-800 min-h-screen">

<header class="px-6 py-4 shadow flex items-center gap-3 text-white" style="background:#0053e2">
  <div>
    <h1 class="text-lg font-bold">Mapbox Experiment Dashboard</h1>
    <p class="text-blue-200 text-xs">Test = Mapbox &nbsp;|&nbsp; Control = configurable &nbsp;|&nbsp; LMD Analytics</p>
  </div>
  <div class="ml-auto text-blue-200 text-xs" id="ts"></div>
</header>

<section class="bg-white border-b px-6 py-4 shadow-sm">
  <div class="max-w-7xl mx-auto flex flex-wrap gap-4 items-end">

    <div class="border border-blue-200 rounded-lg px-3 py-2 bg-blue-50">
      <span class="block text-xs font-bold text-blue-700 mb-2">PRE PERIOD</span>
      <div class="flex gap-3">
        <div>
          <label class="block text-xs text-gray-500 mb-1">From</label>
          <input type="date" id="pre_from" class="border rounded px-2 py-1.5 text-sm"/>
        </div>
        <div>
          <label class="block text-xs text-gray-500 mb-1">To</label>
          <input type="date" id="pre_to" class="border rounded px-2 py-1.5 text-sm"/>
        </div>
      </div>
    </div>
    <div class="border border-amber-200 rounded-lg px-3 py-2 bg-amber-50">
      <span class="block text-xs font-bold text-amber-700 mb-2">POST PERIOD</span>
      <div class="flex gap-3">
        <div>
          <label class="block text-xs text-gray-500 mb-1">From</label>
          <input type="date" id="post_from" class="border rounded px-2 py-1.5 text-sm"/>
        </div>
        <div>
          <label class="block text-xs text-gray-500 mb-1">To</label>
          <input type="date" id="post_to" class="border rounded px-2 py-1.5 text-sm"/>
        </div>
      </div>
    </div>

    <div>
      <label class="block text-xs font-semibold text-gray-500 mb-1">Address Type</label>
      <div class="dd-wrap">
        <button class="dd-btn" onclick="toggleDD('addr_panel')">
          <span id="addr_lbl">All</span> <span class="text-gray-400">&#9660;</span>
        </button>
        <div class="dd-panel" id="addr_panel">
          <div class="dd-item">
            <input type="checkbox" id="addr_all" onchange="toggleAll('addr')"/>
            <label for="addr_all" class="cursor-pointer font-semibold">All</label>
          </div>
          <div id="addr_opts"></div>
        </div>
      </div>
    </div>

    <div>
      <label class="block text-xs font-semibold text-gray-500 mb-1">Control Source</label>
      <div class="dd-wrap">
        <button class="dd-btn" onclick="toggleDD('ctrl_panel')">
          <span id="ctrl_lbl">GOOGLE</span> <span class="text-gray-400">&#9660;</span>
        </button>
        <div class="dd-panel" id="ctrl_panel">
          <div class="dd-item">
            <input type="checkbox" id="ctrl_all" onchange="toggleAll('ctrl')"/>
            <label for="ctrl_all" class="cursor-pointer font-semibold">All</label>
          </div>
          <div id="ctrl_opts"></div>
        </div>
      </div>
    </div>

    <div>
      <label class="block text-xs font-semibold text-gray-500 mb-1">Rollout %</label>
      <select id="rollout_sel" class="border rounded px-2 py-1.5 text-sm"></select>
    </div>

    <div class="flex gap-2">
      <button onclick="applyFilters()"
        class="px-4 py-1.5 rounded font-semibold text-sm text-white"
        style="background:#0053e2">Apply</button>
      <button onclick="resetFilters()"
        class="px-4 py-1.5 rounded font-semibold text-sm bg-gray-100 text-gray-700 hover:bg-gray-200">Reset</button>
    </div>
  </div>
</section>

<main class="max-w-7xl mx-auto px-6 py-6 space-y-8">
  <div class="bg-white rounded-lg shadow overflow-hidden">
    <table class="w-full text-sm">
      <thead>
        <tr class="text-white" style="background:#0053e2">
          <th class="text-left px-3 py-3 font-semibold" rowspan="2">Metric</th>
          <th class="text-center px-3 py-2 font-semibold border-l border-blue-400" colspan="3">Mapbox (Test)</th>
          <th class="text-center px-3 py-2 font-semibold border-l border-blue-400" colspan="3">Control</th>
        </tr>
        <tr class="text-white text-xs" style="background:#0042b8">
          <th class="text-right px-3 py-2 font-medium border-l border-blue-400">Pre</th>
          <th class="text-right px-3 py-2 font-medium">Post</th>
          <th class="text-right px-3 py-2 font-medium">Post&minus;Pre</th>
          <th class="text-right px-3 py-2 font-medium border-l border-blue-400">Pre</th>
          <th class="text-right px-3 py-2 font-medium">Post</th>
          <th class="text-right px-3 py-2 font-medium">Post&minus;Pre</th>
        </tr>
      </thead>
      <tbody id="kpi_body"></tbody>
    </table>
  </div>

  <h2 class="text-base font-bold text-gray-700 border-b pb-2">Weekly Trends</h2>
  <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6" id="charts_grid"></div>
</main>

<footer class="text-center text-xs text-gray-400 py-6">
  Mapbox Experiment Dashboard &bull; LMD Analytics &bull;
  <code>HB_mapbox_implementation_base_table</code>
</footer>

<script>
const RAW  = __RAW_DATA__;
const META = __META__;
document.getElementById('ts').textContent = 'Data as of __GENERATED_AT__';

const BLUE  = '#0053e2';
const AMBER = '#995213';
const METRICS = [
  ['total_orders',             'Total Orders',                      false],
  ['pct_perfect_orders',       '% Perfect Orders',                  true ],
  ['pct_missing_orders',       '% Missing Orders',                  true ],
  ['pct_contacts',             '% Contacts',                        true ],
  ['pct_contact_cant_find',    '% Contact - Cant Find Address',     true ],
  ['pct_contact_cant_confirm', '% Contact - Cant Confirm Arrival',  true ],
  ['pct_force_complete',       '% Force Complete',                  true ],
  ['pct_returned',             '% Returned PO',                     true ],
  ['pct_return_cant_find',     '% Return - Cant Find Address',      true ],
  ['pct_return_cant_confirm',  '% Return - Cant Confirm Arrival',   true ],
];
const charts = {};

function safePct(num, den) {
  if (!den) return null;
  return (num / den) * 100;
}

function computeMetrics(rows) {
  if (!rows || !rows.length) return null;
  let to=0,po=0,mo=0,cn=0,cd=0,cfn=0,ccn=0,fn=0,fd=0,rp=0,dp=0,rcf=0,rcc=0;
  for (const r of rows) {
    to  += r.total_orders             || 0;
    po  += r.perfect_orders           || 0;
    mo  += r.missing_orders           || 0;
    cn  += r.contact_num              || 0;
    cd  += r.contact_den              || 0;
    cfn += r.contact_cant_find_num    || 0;
    ccn += r.contact_cant_confirm_num || 0;
    fn  += r.fc_num                   || 0;
    fd  += r.fc_den                   || 0;
    rp  += r.returned_po              || 0;
    dp  += r.dispatched_po            || 0;
    rcf += r.returned_cant_find       || 0;
    rcc += r.returned_cant_confirm    || 0;
  }
  return {
    total_orders:             to,
    pct_perfect_orders:       safePct(po,  to),
    pct_missing_orders:       safePct(mo,  to),
    pct_contacts:             safePct(cn,  cd),
    pct_contact_cant_find:    safePct(cfn, cd),
    pct_contact_cant_confirm: safePct(ccn, cd),
    pct_force_complete:       safePct(fn,  fd),
    pct_returned:             safePct(rp,  dp),
    pct_return_cant_find:     safePct(rcf, dp),
    pct_return_cant_confirm:  safePct(rcc, dp),
  };
}

// ---- dropdown helpers ---------------------------------------------------
function toggleDD(id) {
  document.querySelectorAll('.dd-panel').forEach(p => {
    if (p.id !== id) p.classList.remove('open');
  });
  document.getElementById(id).classList.toggle('open');
}
document.addEventListener('click', e => {
  if (!e.target.closest('.dd-wrap'))
    document.querySelectorAll('.dd-panel').forEach(p => p.classList.remove('open'));
});

function getChecked(prefix) {
  return [...document.querySelectorAll('#' + prefix + '_opts input:checked')].map(c => c.value);
}

function toggleAll(prefix) {
  const isAll = document.getElementById(prefix + '_all').checked;
  document.querySelectorAll('#' + prefix + '_opts input').forEach(c => c.checked = isAll);
  updateLabel(prefix);
}

function updateLabel(prefix) {
  const opts = [...document.querySelectorAll('#' + prefix + '_opts input')];
  const sel  = opts.filter(o => o.checked);
  document.getElementById(prefix + '_all').checked = sel.length === opts.length;
  const lbl = sel.length === 0            ? 'None'
            : sel.length === opts.length  ? 'All'
            : sel.length <= 2             ? sel.map(o => o.value).join(', ')
            : sel.length + ' selected';
  document.getElementById(prefix + '_lbl').textContent = lbl;
}

function buildDD(prefix, values, defaultFn) {
  const cont = document.getElementById(prefix + '_opts');
  cont.innerHTML = '';
  values.forEach(function(v) {
    const div = document.createElement('div');
    div.className = 'dd-item';
    const cb = document.createElement('input');
    cb.type    = 'checkbox';
    cb.id      = prefix + '_' + v;
    cb.value   = v;
    cb.checked = !!defaultFn(v);
    cb.addEventListener('change', function() { updateLabel(prefix); });
    const lbl = document.createElement('label');
    lbl.htmlFor   = prefix + '_' + v;
    lbl.className = 'cursor-pointer';
    lbl.textContent = v;
    div.appendChild(cb);
    div.appendChild(lbl);
    cont.appendChild(div);
  });
  updateLabel(prefix);
}

// ---- date <-> week mapping ----------------------------------------------
function dateToWeeks(from, to) {
  return META.wm_wks.filter(wk => {
    const wd = META.wk_dates[wk];
    return wd && wd.min <= to && wd.max >= from;
  });
}

// ---- filter -------------------------------------------------------------
function filterRowsByRange(from, to) {
  if (!from || !to) return [];
  const addrSel = getChecked('addr');
  const ctrlSel = getChecked('ctrl');
  const rollout = document.getElementById('rollout_sel').value;
  const wkSet   = new Set(dateToWeeks(from, to));
  return RAW.filter(r => {
    if (!wkSet.has(r.wm_wk)) return false;
    if (addrSel.length && !addrSel.includes(r.address_type)) return false;
    if (rollout && r.rollout_pct !== rollout) return false;
    if (r.Test_Control === 'Test')    return r.source === 'MAPBOX';
    if (r.Test_Control === 'Control') return !ctrlSel.length || ctrlSel.includes(r.source);
    return false;
  });
}

// ---- render table -------------------------------------------------------
function fmt(v, isCount) {
  if (v == null) return '-';
  return isCount ? Number(v).toLocaleString() : Number(v).toFixed(2) + '%';
}

function deltaClass(key, d) {
  if (d == null) return '';
  const lowerBetter = key !== 'total_orders' && key !== 'pct_perfect_orders';
  return (lowerBetter ? d < 0 : d > 0) ? 'text-green-600 font-semibold' : 'text-red-500 font-semibold';
}

function renderTable(testPre, testPost, ctrlPre, ctrlPost) {
  let html = '';
  METRICS.forEach(([key, label, isP], i) => {
    const tPre   = testPre  ? testPre[key]  : null;
    const tPost  = testPost ? testPost[key] : null;
    const cPre   = ctrlPre  ? ctrlPre[key]  : null;
    const cPost  = ctrlPost ? ctrlPost[key] : null;
    const tDelta = (tPre  != null && tPost != null) ? tPost - tPre  : null;
    const cDelta = (cPre  != null && cPost != null) ? cPost - cPre  : null;
    const bg     = i % 2 ? 'bg-gray-50' : 'bg-white';
    const tDc    = deltaClass(key, tDelta);
    const cDc    = deltaClass(key, cDelta);
    const sgn    = v => (v != null && v > 0) ? '+' : '';
    html += '<tr class="' + bg + ' border-t border-gray-100 hover:bg-blue-50">'
      + '<td class="px-3 py-2 font-medium text-sm">' + label + '</td>'
      + '<td class="px-3 py-2 text-right font-mono text-sm border-l border-gray-100" style="color:' + BLUE  + '">' + fmt(tPre,  !isP) + '</td>'
      + '<td class="px-3 py-2 text-right font-mono text-sm" style="color:' + BLUE  + '">' + fmt(tPost, !isP) + '</td>'
      + '<td class="px-3 py-2 text-right font-mono text-sm ' + tDc + '">' + (tDelta != null ? sgn(tDelta) + fmt(tDelta, !isP) : '-') + '</td>'
      + '<td class="px-3 py-2 text-right font-mono text-sm border-l border-gray-100" style="color:' + AMBER + '">' + fmt(cPre,  !isP) + '</td>'
      + '<td class="px-3 py-2 text-right font-mono text-sm" style="color:' + AMBER + '">' + fmt(cPost, !isP) + '</td>'
      + '<td class="px-3 py-2 text-right font-mono text-sm ' + cDc + '">' + (cDelta != null ? sgn(cDelta) + fmt(cDelta, !isP) : '-') + '</td>'
      + '</tr>';
  });
  document.getElementById('kpi_body').innerHTML = html;
}

// ---- charts -------------------------------------------------------------
function buildCharts(weeks, tWk, cWk) {
  const grid = document.getElementById('charts_grid');
  if (!grid.children.length) {
    METRICS.forEach(([key, label]) => {
      const card = document.createElement('div');
      card.className = 'bg-white rounded-lg shadow p-4';
      card.innerHTML = '<p class="text-xs font-semibold text-gray-500 mb-2">' + label + '</p>'
        + '<div class="chart-wrap"><canvas id="c_' + key + '"></canvas></div>';
      grid.appendChild(card);
    });
  }
  METRICS.forEach(([key, , isP]) => {
    const tVals = tWk.map(m => m ? m[key] : null);
    const cVals = cWk.map(m => m ? m[key] : null);
    const tickFmt = v => v == null ? '' : (isP ? Number(v).toFixed(2) + '%' : Number(v).toLocaleString());
    if (charts[key]) {
      charts[key].data.labels            = weeks;
      charts[key].data.datasets[0].data  = tVals;
      charts[key].data.datasets[1].data  = cVals;
      charts[key].update();
    } else {
      charts[key] = new Chart(document.getElementById('c_' + key), {
        type: 'line',
        data: { labels: weeks, datasets: [
          { label:'Mapbox',  data:tVals, borderColor:BLUE,  backgroundColor:BLUE+'22',  tension:.3, pointRadius:3 },
          { label:'Control', data:cVals, borderColor:AMBER, backgroundColor:AMBER+'22', tension:.3, pointRadius:3 },
        ]},
        options: {
          responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ labels:{ font:{size:10} } } },
          scales:{
            x:{ ticks:{ font:{size:9}, maxRotation:45 } },
            y:{ ticks:{ font:{size:9}, callback: tickFmt } }
          }
        }
      });
    }
  });
}

// ---- main ---------------------------------------------------------------
function applyFilters() {
  const preFrom  = document.getElementById('pre_from').value;
  const preTo    = document.getElementById('pre_to').value;
  const postFrom = document.getElementById('post_from').value;
  const postTo   = document.getElementById('post_to').value;

  const preRows  = filterRowsByRange(preFrom, preTo);
  const postRows = filterRowsByRange(postFrom, postTo);

  const testPre  = computeMetrics(preRows.filter(r  => r.Test_Control === 'Test'));
  const testPost = computeMetrics(postRows.filter(r => r.Test_Control === 'Test'));
  const ctrlPre  = computeMetrics(preRows.filter(r  => r.Test_Control === 'Control'));
  const ctrlPost = computeMetrics(postRows.filter(r => r.Test_Control === 'Control'));

  renderTable(testPre, testPost, ctrlPre, ctrlPost);

  // Charts span the full visible range (pre start -> post end)
  const chartFrom = preFrom  || postFrom;
  const chartTo   = postTo   || preTo;
  if (chartFrom && chartTo) {
    const weeks   = dateToWeeks(chartFrom, chartTo);
    const allRows = preRows.concat(postRows);
    const tWk     = weeks.map(w => computeMetrics(allRows.filter(r => r.wm_wk === w && r.Test_Control === 'Test')));
    const cWk     = weeks.map(w => computeMetrics(allRows.filter(r => r.wm_wk === w && r.Test_Control === 'Control')));
    buildCharts(weeks, tWk, cWk);
  }
}

function resetFilters() {
  document.getElementById('pre_from').value  = '';
  document.getElementById('pre_to').value    = '';
  document.getElementById('post_from').value = '';
  document.getElementById('post_to').value   = '';
  document.querySelectorAll('#addr_opts input').forEach(c => c.checked = true);
  document.getElementById('addr_all').checked = true;
  updateLabel('addr');
  document.querySelectorAll('#ctrl_opts input').forEach(c => { c.checked = c.value === 'GOOGLE'; });
  document.getElementById('ctrl_all').checked = false;
  updateLabel('ctrl');
  document.getElementById('rollout_sel').value = '';
  applyFilters();
}

function rolloutLabel(v) { return isNaN(Number(v)) ? v : v + '%'; }

(function init() {
  buildDD('addr', META.address_types, function() { return true; });        // all selected
  buildDD('ctrl', META.ctrl_sources,  function(v) { return v === 'GOOGLE'; }); // GOOGLE only

  const rollEl = document.getElementById('rollout_sel');
  rollEl.appendChild(new Option('All', ''));
  META.rollout_pcts.forEach(v => rollEl.appendChild(new Option(rolloutLabel(v), v)));

  // Pre/Post dates start empty — user sets them manually
  applyFilters();
}());
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# 4. Generate
# ---------------------------------------------------------------------------

def generate_html(data: list[dict], meta: dict) -> str:
    from datetime import datetime, timezone
    gen = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        HTML
        .replace("__RAW_DATA__",     json.dumps(data, separators=(",", ":")))
        .replace("__META__",         json.dumps(meta, separators=(",", ":")))
        .replace("__GENERATED_AT__", gen)
    )


if __name__ == "__main__":
    print("Connecting to BigQuery...")
    client = bigquery.Client()
    print("Fetching metric data...")
    data = fetch_raw(client)
    print("Fetching week-to-date mapping...")
    wk_dates = fetch_week_dates(client)
    meta = build_meta(data, wk_dates)
    print(f"  Date range   : {meta['date_min']} to {meta['date_max']}")
    print(f"  Weeks        : {meta['wm_wks']}")
    print(f"  Address types: {meta['address_types']}")
    print(f"  Ctrl sources : {meta['ctrl_sources']}")
    print(f"  Rollout pcts : {meta['rollout_pcts']}")
    print("Generating HTML...")
    html = generate_html(data, meta)
    OUT_FILE.write_text(html, encoding="utf-8")
    print(f"  Saved: {OUT_FILE}  ({OUT_FILE.stat().st_size / 1024:.0f} KB)")
    print("Done!")
