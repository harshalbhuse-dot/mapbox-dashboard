"""BigQuery query logic for the Mapbox experiment dashboard."""

from google.cloud import bigquery
from typing import Optional

BQ_TABLE = "wmt-driver-insights.Chirag_dx.HB_mapbox_implementation_base_table"

_client: Optional[bigquery.Client] = None


def get_client() -> bigquery.Client:
    """Lazy singleton BQ client using Application Default Credentials."""
    global _client
    if _client is None:
        _client = bigquery.Client()
    return _client


def _run_query(sql: str, params: list) -> list[dict]:
    """Execute a parameterised BQ query and return rows as list of dicts."""
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = get_client().query(sql, job_config=job_config).result()
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Filter options
# ---------------------------------------------------------------------------

def get_filter_options() -> dict:
    """Return distinct address types and non-Mapbox control lat/long sources."""
    sql = f"""
        SELECT
          ARRAY_AGG(DISTINCT n_addresstype IGNORE NULLS ORDER BY n_addresstype) AS address_types,
          ARRAY_AGG(DISTINCT UPPER(RECOMMENDEDLATLONGSOURCE) IGNORE NULLS ORDER BY UPPER(RECOMMENDEDLATLONGSOURCE)) AS all_sources
        FROM `{BQ_TABLE}`
    """
    rows = _run_query(sql, [])
    if not rows:
        return {"address_types": [], "control_sources": []}
    row = rows[0]
    # Exclude MAPBOX from control source options
    control_sources = [
        s for s in (row["all_sources"] or []) if s and s.upper() != "MAPBOX"
    ]
    return {
        "address_types": [a for a in (row["address_types"] or []) if a],
        "control_sources": control_sources,
    }


# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------

def get_summary_metrics(
    date_from: str,
    date_to: str,
    address_types: list[str],
    control_sources: list[str],
) -> dict:
    """Return Test vs Control aggregated metrics for the selected filters."""
    addr_filter = ""
    if address_types:
        addr_filter = "AND n_addresstype IN UNNEST(@address_types)"

    sql = f"""
        SELECT
          CASE
            WHEN Test_Control = 'Test'    THEN 'Mapbox'
            WHEN Test_Control = 'Control' THEN 'Google (Control)'
            ELSE Test_Control
          END AS group_label,
          SUM(total_orders)                                                   AS total_orders,
          AVG(perfect_orders)                                                 AS pct_perfect_orders,
          AVG(missing_orders)                                                 AS pct_missing_orders,
          SAFE_DIVIDE(SUM(contact_num),            SUM(contact_den))    * 100 AS pct_contacts,
          SAFE_DIVIDE(SUM(contact_cant_find_add_num),   SUM(contact_den))    * 100 AS pct_contact_cant_find,
          SAFE_DIVIDE(SUM(contact_cant_confirm_arrival_num), SUM(contact_den)) * 100 AS pct_contact_cant_confirm,
          SAFE_DIVIDE(SUM(FC_num),                 SUM(FC_den))         * 100 AS pct_force_complete,
          SAFE_DIVIDE(SUM(RETURNED_PO),            SUM(DISPATCHED_PO))  * 100 AS pct_returned,
          SAFE_DIVIDE(SUM(RETURNED_PO_CANT_FIND_ADDRESS), SUM(DISPATCHED_PO)) * 100 AS pct_return_cant_find,
          SAFE_DIVIDE(SUM(RETURNED_PO_LOCATION_ISSUE),    SUM(DISPATCHED_PO)) * 100 AS pct_return_cant_confirm
        FROM `{BQ_TABLE}`
        WHERE slot_dt BETWEEN @date_from AND @date_to
          AND (
            (Test_Control = 'Test'    AND UPPER(RECOMMENDEDLATLONGSOURCE) = 'MAPBOX')
            OR
            (Test_Control = 'Control' AND UPPER(RECOMMENDEDLATLONGSOURCE) IN UNNEST(@control_sources))
          )
          {addr_filter}
        GROUP BY Test_Control
        ORDER BY Test_Control DESC
    """

    params = [
        bigquery.ScalarQueryParameter("date_from", "DATE", date_from),
        bigquery.ScalarQueryParameter("date_to", "DATE", date_to),
        bigquery.ArrayQueryParameter("control_sources", "STRING", control_sources),
    ]
    if address_types:
        params.append(bigquery.ArrayQueryParameter("address_types", "STRING", address_types))

    return {row["group_label"]: row for row in _run_query(sql, params)}


# ---------------------------------------------------------------------------
# Weekly trends
# ---------------------------------------------------------------------------

def get_weekly_trends(
    date_from: str,
    date_to: str,
    address_types: list[str],
    control_sources: list[str],
) -> dict:
    """Return week-by-week metrics for Test and Control (for Chart.js)."""
    addr_filter = ""
    if address_types:
        addr_filter = "AND n_addresstype IN UNNEST(@address_types)"

    sql = f"""
        SELECT
          wm_wk,
          CASE
            WHEN Test_Control = 'Test'    THEN 'Mapbox'
            WHEN Test_Control = 'Control' THEN 'Google (Control)'
            ELSE Test_Control
          END AS group_label,
          SUM(total_orders)                                                        AS total_orders,
          AVG(perfect_orders)                                                      AS pct_perfect_orders,
          AVG(missing_orders)                                                      AS pct_missing_orders,
          SAFE_DIVIDE(SUM(contact_num),            SUM(contact_den))         * 100 AS pct_contacts,
          SAFE_DIVIDE(SUM(contact_cant_find_add_num),   SUM(contact_den))    * 100 AS pct_contact_cant_find,
          SAFE_DIVIDE(SUM(contact_cant_confirm_arrival_num), SUM(contact_den)) * 100 AS pct_contact_cant_confirm,
          SAFE_DIVIDE(SUM(FC_num),                 SUM(FC_den))              * 100 AS pct_force_complete,
          SAFE_DIVIDE(SUM(RETURNED_PO),            SUM(DISPATCHED_PO))       * 100 AS pct_returned,
          SAFE_DIVIDE(SUM(RETURNED_PO_CANT_FIND_ADDRESS), SUM(DISPATCHED_PO)) * 100 AS pct_return_cant_find,
          SAFE_DIVIDE(SUM(RETURNED_PO_LOCATION_ISSUE),    SUM(DISPATCHED_PO)) * 100 AS pct_return_cant_confirm
        FROM `{BQ_TABLE}`
        WHERE slot_dt BETWEEN @date_from AND @date_to
          AND (
            (Test_Control = 'Test'    AND UPPER(RECOMMENDEDLATLONGSOURCE) = 'MAPBOX')
            OR
            (Test_Control = 'Control' AND UPPER(RECOMMENDEDLATLONGSOURCE) IN UNNEST(@control_sources))
          )
          {addr_filter}
        GROUP BY wm_wk, Test_Control
        ORDER BY wm_wk, Test_Control
    """

    params = [
        bigquery.ScalarQueryParameter("date_from", "DATE", date_from),
        bigquery.ScalarQueryParameter("date_to", "DATE", date_to),
        bigquery.ArrayQueryParameter("control_sources", "STRING", control_sources),
    ]
    if address_types:
        params.append(bigquery.ArrayQueryParameter("address_types", "STRING", address_types))

    rows = _run_query(sql, params)

    # Pivot into {metric: {label: {wm_wk: value}}} for Chart.js
    metrics = [
        "total_orders", "pct_perfect_orders", "pct_missing_orders",
        "pct_contacts", "pct_contact_cant_find", "pct_contact_cant_confirm",
        "pct_force_complete", "pct_returned", "pct_return_cant_find",
        "pct_return_cant_confirm",
    ]
    weeks: set[str] = set()
    grouped: dict[str, dict[str, dict]] = {m: {} for m in metrics}

    for row in rows:
        wk = str(row["wm_wk"])
        weeks.add(wk)
        lbl = row["group_label"]
        for m in metrics:
            grouped[m].setdefault(lbl, {})
            val = row[m]
            grouped[m][lbl][wk] = round(float(val), 4) if val is not None else None

    sorted_weeks = sorted(weeks)
    return {"weeks": sorted_weeks, "data": grouped}
