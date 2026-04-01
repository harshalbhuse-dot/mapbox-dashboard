# Mapbox Experiment Dashboard

> **🔗 Live (while host machine is on):** http://172.19.254.119:8002
> Share this link with anyone on Eagle WiFi or Walmart VPN.

Refreshable FastAPI + HTMX dashboard comparing **Mapbox (Test)** vs **Google (Control)** delivery KPIs.

## Prerequisites
- Python 3.11+
- [`uv`](https://github.com/astral-sh/uv) package manager
- Google Cloud credentials with BigQuery read access  
  Run: `gcloud auth application-default login`

## Setup

```bash
# Clone the repo and enter the project
git clone <your-repo-url>
cd mapbox_dashboard

# Create virtualenv and install dependencies
uv venv
uv pip install -r requirements.txt
```

## Run

```bash
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Mac/Linux

# Local only
uvicorn main:app --reload --port 8002

# Network-accessible (share with team on Eagle WiFi / Walmart VPN)
uvicorn main:app --host 0.0.0.0 --reload --port 8002
```

Local:   http://localhost:8002  
Shared:  http://172.19.254.119:8002  *(your Eagle/VPN IP — update if it changes)*

> ⚠️ The dashboard is only reachable while your machine is running and the server is active.
> Colleagues must be on **Eagle WiFi** or **Walmart VPN** to access it.

> If teammates get a connection refused, check Windows Firewall:
> **Windows Security Alert → Allow access** when prompted, or run as admin:
> ```
> netsh advfirewall firewall add rule name="Mapbox Dashboard 8002" dir=in action=allow protocol=TCP localport=8002
> ```

## Filters
| Filter | Description |
|--------|-------------|
| Date range | `slot_dt` between selected dates |
| Address type | `n_addresstype` — multi-select pill toggle |
| Control source | `RECOMMENDEDLATLONGSOURCE` for Control group (excl. Mapbox). Default = GOOGLE |

## Metrics
| Metric | Formula |
|--------|---------|
| Total Orders | `SUM(total_orders)` |
| % Perfect Orders | `AVG(perfect_orders)` |
| % Missing Orders | `AVG(missing_orders)` |
| % Contacts | `SUM(contact_num) / SUM(contact_den) * 100` |
| % Contact Can't Find Address | `SUM(contact_cant_find_add_num) / SUM(contact_den) * 100` |
| % Contact Can't Confirm Arrival | `SUM(contact_cant_confirm_arrival_num) / SUM(contact_den) * 100` |
| % Force Complete | `SUM(FC_num) / SUM(FC_den) * 100` |
| % Returned PO | `SUM(RETURNED_PO) / SUM(DISPATCHED_PO) * 100` |
| % Return Can't Find Address | `SUM(RETURNED_PO_CANT_FIND_ADDRESS) / SUM(DISPATCHED_PO) * 100` |
| % Return Can't Confirm Arrival | `SUM(RETURNED_PO_LOCATION_ISSUE) / SUM(DISPATCHED_PO) * 100` |

## Source Table
`wmt-driver-insights.Chirag_dx.HB_mapbox_implementation_base_table`
