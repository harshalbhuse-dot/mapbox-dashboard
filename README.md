# Mapbox Experiment Dashboard

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

uvicorn main:app --reload --port 8000
```

Open: [http://localhost:8000](http://localhost:8000)

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
