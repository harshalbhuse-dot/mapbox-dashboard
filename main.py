"""FastAPI application entry point for the Mapbox Experiment Dashboard."""

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional
import bq_client

app = FastAPI(title="Mapbox Experiment Dashboard")
templates = Jinja2Templates(directory="templates")


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class MetricsRequest(BaseModel):
    date_from: str
    date_to: str
    address_types: list[str] = []
    control_sources: list[str] = ["GOOGLE"]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serve the dashboard shell. Filters and data load client-side on boot."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/api/filters")
async def api_filters():
    """Return available address types and non-Mapbox latlongsource options."""
    try:
        options = bq_client.get_filter_options()
        return JSONResponse(content=options)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/metrics")
async def api_metrics(body: MetricsRequest):
    """Return Test vs Control summary metrics for current filter state."""
    try:
        data = bq_client.get_summary_metrics(
            date_from=body.date_from,
            date_to=body.date_to,
            address_types=body.address_types,
            control_sources=body.control_sources,
        )
        # Ensure floats are JSON-serialisable (skip string fields like group_label)
        safe = {}
        for group, row in data.items():
            safe[group] = {}
            for k, v in row.items():
                if v is None:
                    safe[group][k] = None
                elif isinstance(v, str):
                    safe[group][k] = v
                else:
                    safe[group][k] = round(float(v), 4)
        return JSONResponse(content=safe)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/trends")
async def api_trends(body: MetricsRequest):
    """Return weekly trend data for Chart.js rendering."""
    try:
        data = bq_client.get_weekly_trends(
            date_from=body.date_from,
            date_to=body.date_to,
            address_types=body.address_types,
            control_sources=body.control_sources,
        )
        return JSONResponse(content=data)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
