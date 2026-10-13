"""
Analytics MCP Server
====================
A Model Context Protocol server that exposes internal analytics data
endpoints to AI coding tools, enabling agentic sessions to query live
analytics results and auto-generate dashboard code from live data.

Tools:
  query_metric            — query a named KPI for a date range
  get_dashboard_data      — structured payload for a dashboard type
  generate_dashboard_code — generate Plotly/SQL/HTML code from live data
  list_available_metrics  — discover available metrics + data freshness
  run_dbt_model           — trigger a dbt model refresh

Stack: Python · asyncpg · PostgreSQL · dbt · MCP 2.x
"""

import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta

import asyncpg
from dotenv import load_dotenv
from mcp.server.mcpserver import MCPServer

load_dotenv()

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
log = logging.getLogger("analytics-mcp")

DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/analytics_mcp")

# ── Database ───────────────────────────────────────────────────

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
    return _pool


async def db_query(sql: str, *args) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]


async def db_one(sql: str, *args) -> dict | None:
    rows = await db_query(sql, *args)
    return rows[0] if rows else None


async def db_exec(sql: str) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(sql)


# ── MCP server ─────────────────────────────────────────────────

app = MCPServer("analytics-mcp-server")


# ── Tool: list_available_metrics ───────────────────────────────

@app.tool(description="List all available metrics, their descriptions, and data freshness.")
async def list_available_metrics() -> str:
    rev = await db_one("SELECT MAX(report_date) AS latest FROM analytics.daily_revenue")
    usr = await db_one("SELECT MAX(report_date) AS latest FROM analytics.user_metrics")
    prd = await db_one("SELECT MAX(report_date) AS latest FROM analytics.product_metrics")

    result = {
        "metrics": [
            {
                "name": "daily_revenue",
                "description": "Total EUR revenue per day from completed orders",
                "source_table": "analytics.daily_revenue",
                "data_freshness": str(rev["latest"] if rev else "N/A"),
            },
            {
                "name": "mrr",
                "description": "Monthly Recurring Revenue in EUR",
                "source_table": "analytics.user_metrics",
                "data_freshness": str(usr["latest"] if usr else "N/A"),
            },
            {
                "name": "new_users",
                "description": "Count of newly registered users per period",
                "source_table": "analytics.user_metrics",
                "data_freshness": str(usr["latest"] if usr else "N/A"),
            },
            {
                "name": "active_users",
                "description": "Distinct users with at least one event per period",
                "source_table": "analytics.user_metrics",
                "data_freshness": str(usr["latest"] if usr else "N/A"),
            },
            {
                "name": "churn_rate",
                "description": "Churned users as a fraction of total users",
                "source_table": "analytics.user_metrics",
                "data_freshness": str(usr["latest"] if usr else "N/A"),
            },
            {
                "name": "refund_rate",
                "description": "Refunded orders as a fraction of total orders",
                "source_table": "analytics.daily_revenue",
                "data_freshness": str(rev["latest"] if rev else "N/A"),
            },
            {
                "name": "feature_adoption",
                "description": "Per-feature unique user counts and adoption rates",
                "source_table": "analytics.product_metrics",
                "data_freshness": str(prd["latest"] if prd else "N/A"),
            },
        ]
    }
    return json.dumps(result, indent=2, default=str)


# ── Tool: query_metric ─────────────────────────────────────────

@app.tool(
    description=(
        "Query a specific analytics KPI by name and optional date range. "
        "Returns time-series data from the analytics warehouse. "
        "Available metrics: daily_revenue, mrr, new_users, active_users, "
        "churn_rate, refund_rate, feature_adoption"
    )
)
async def query_metric(
    metric: str,
    start_date: str = "",
    end_date: str = "",
    granularity: str = "daily",
) -> str:
    """
    Args:
        metric: Metric name (daily_revenue | mrr | new_users | active_users |
                churn_rate | refund_rate | feature_adoption)
        start_date: ISO date YYYY-MM-DD (default: 30 days ago)
        end_date: ISO date YYYY-MM-DD (default: today)
        granularity: daily | weekly | monthly
    """
    end_dt = date.fromisoformat(end_date) if end_date else date.today()
    start_dt = date.fromisoformat(start_date) if start_date else end_dt - timedelta(days=30)
    trunc = {"daily": "day", "weekly": "week", "monthly": "month"}.get(granularity, "day")

    metric_queries: dict[str, str] = {
        "daily_revenue": f"""
            SELECT DATE_TRUNC('{trunc}', report_date)::DATE AS period,
                   SUM(total_eur)   AS value,
                   SUM(order_count) AS order_count
            FROM   analytics.daily_revenue
            WHERE  report_date BETWEEN $1 AND $2
            GROUP  BY 1 ORDER BY 1
        """,
        "mrr": f"""
            SELECT DATE_TRUNC('{trunc}', report_date)::DATE AS period,
                   AVG(mrr_eur) AS value
            FROM   analytics.user_metrics
            WHERE  report_date BETWEEN $1 AND $2
            GROUP  BY 1 ORDER BY 1
        """,
        "new_users": f"""
            SELECT DATE_TRUNC('{trunc}', report_date)::DATE AS period,
                   SUM(new_users) AS value
            FROM   analytics.user_metrics
            WHERE  report_date BETWEEN $1 AND $2
            GROUP  BY 1 ORDER BY 1
        """,
        "active_users": f"""
            SELECT DATE_TRUNC('{trunc}', report_date)::DATE AS period,
                   AVG(active_users)::INT AS value
            FROM   analytics.user_metrics
            WHERE  report_date BETWEEN $1 AND $2
            GROUP  BY 1 ORDER BY 1
        """,
        "churn_rate": f"""
            SELECT DATE_TRUNC('{trunc}', report_date)::DATE AS period,
                   ROUND(SUM(churned_users)::NUMERIC /
                         NULLIF(SUM(new_users + active_users), 0), 4) AS value
            FROM   analytics.user_metrics
            WHERE  report_date BETWEEN $1 AND $2
            GROUP  BY 1 ORDER BY 1
        """,
        "refund_rate": f"""
            SELECT DATE_TRUNC('{trunc}', report_date)::DATE AS period,
                   ROUND(AVG(refund_rate), 4) AS value
            FROM   analytics.daily_revenue
            WHERE  report_date BETWEEN $1 AND $2
            GROUP  BY 1 ORDER BY 1
        """,
        "feature_adoption": f"""
            SELECT DATE_TRUNC('{trunc}', report_date)::DATE AS period,
                   feature_name,
                   SUM(unique_users)        AS unique_users,
                   ROUND(AVG(adoption_rate), 4) AS adoption_rate
            FROM   analytics.product_metrics
            WHERE  report_date BETWEEN $1 AND $2
            GROUP  BY 1, 2 ORDER BY 1, 2
        """,
    }

    if metric not in metric_queries:
        return json.dumps({
            "error": f"Unknown metric '{metric}'",
            "available": list(metric_queries.keys()),
        })

    rows = await db_query(metric_queries[metric], start_dt, end_dt)
    return json.dumps({
        "metric": metric,
        "granularity": granularity,
        "start_date": str(start_dt),
        "end_date": str(end_dt),
        "row_count": len(rows),
        "data": rows,
    }, indent=2, default=str)


# ── Tool: get_dashboard_data ───────────────────────────────────

@app.tool(
    description=(
        "Return a structured JSON payload ready to render a dashboard. "
        "Types: revenue_overview | user_growth | product_adoption | executive_summary"
    )
)
async def get_dashboard_data(dashboard_type: str, days: int = 30) -> str:
    """
    Args:
        dashboard_type: revenue_overview | user_growth | product_adoption | executive_summary
        days: Lookback window in days (default: 30)
    """
    since = date.today() - timedelta(days=days)

    if dashboard_type == "revenue_overview":
        timeseries = await db_query(
            "SELECT report_date, total_eur, order_count, avg_order_eur, refund_rate "
            "FROM analytics.daily_revenue WHERE report_date >= $1 ORDER BY report_date",
            since,
        )
        summary = await db_one(
            "SELECT SUM(total_eur) AS total_revenue, SUM(order_count) AS total_orders, "
            "AVG(avg_order_eur) AS avg_order, AVG(refund_rate) AS avg_refund_rate "
            "FROM analytics.daily_revenue WHERE report_date >= $1",
            since,
        )
        result = {"dashboard": dashboard_type, "period_days": days,
                  "summary": summary, "timeseries": timeseries}

    elif dashboard_type == "user_growth":
        timeseries = await db_query(
            "SELECT report_date, new_users, active_users, churned_users, mrr_eur "
            "FROM analytics.user_metrics WHERE report_date >= $1 ORDER BY report_date",
            since,
        )
        summary = await db_one(
            "SELECT SUM(new_users) AS total_new, AVG(active_users)::INT AS avg_active, "
            "SUM(churned_users) AS total_churned, MAX(mrr_eur) AS latest_mrr "
            "FROM analytics.user_metrics WHERE report_date >= $1",
            since,
        )
        result = {"dashboard": dashboard_type, "period_days": days,
                  "summary": summary, "timeseries": timeseries}

    elif dashboard_type == "product_adoption":
        features = await db_query(
            "SELECT feature_name, SUM(unique_users) AS total_users, "
            "SUM(total_uses) AS total_uses, ROUND(AVG(adoption_rate),4) AS avg_adoption "
            "FROM analytics.product_metrics WHERE report_date >= $1 "
            "GROUP BY feature_name ORDER BY avg_adoption DESC",
            since,
        )
        result = {"dashboard": dashboard_type, "period_days": days, "features": features}

    elif dashboard_type == "executive_summary":
        rev = await db_one(
            "SELECT SUM(total_eur) AS revenue, SUM(order_count) AS orders "
            "FROM analytics.daily_revenue WHERE report_date >= $1", since)
        usr = await db_one(
            "SELECT SUM(new_users) AS new_users, MAX(mrr_eur) AS mrr "
            "FROM analytics.user_metrics WHERE report_date >= $1", since)
        top = await db_one(
            "SELECT feature_name, ROUND(AVG(adoption_rate),4) AS adoption "
            "FROM analytics.product_metrics WHERE report_date >= $1 "
            "GROUP BY feature_name ORDER BY adoption DESC LIMIT 1", since)
        result = {"dashboard": dashboard_type, "period_days": days,
                  "revenue_eur": rev, "user_metrics": usr, "top_feature": top}
    else:
        result = {"error": f"Unknown dashboard type: {dashboard_type}",
                  "available": ["revenue_overview", "user_growth",
                                "product_adoption", "executive_summary"]}

    return json.dumps(result, indent=2, default=str)


# ── Tool: generate_dashboard_code ─────────────────────────────

@app.tool(
    description=(
        "Auto-generate runnable Python (Plotly), SQL, or HTML dashboard code "
        "based on live data fetched from the analytics warehouse."
    )
)
async def generate_dashboard_code(
    metric: str,
    output_format: str = "plotly_python",
    days: int = 30,
) -> str:
    """
    Args:
        metric: Metric to visualise (daily_revenue | mrr | new_users | ...)
        output_format: plotly_python | sql_query | html_chart
        days: Lookback window in days
    """
    since = str(date.today() - timedelta(days=days))
    today = str(date.today())

    col_map = {
        "daily_revenue": ("total_eur",    "analytics.daily_revenue"),
        "mrr":           ("mrr_eur",      "analytics.user_metrics"),
        "new_users":     ("new_users",    "analytics.user_metrics"),
        "active_users":  ("active_users", "analytics.user_metrics"),
        "churn_rate":    ("churned_users","analytics.user_metrics"),
        "refund_rate":   ("refund_rate",  "analytics.daily_revenue"),
    }
    col, table = col_map.get(metric, ("total_eur", "analytics.daily_revenue"))

    # Fetch live sample to embed in generated code
    since_dt = date.today() - timedelta(days=days)
    today_dt = date.today()
    sample_rows = await db_query(
        f"SELECT report_date AS period, {col} AS value "
        f"FROM {table} WHERE report_date BETWEEN $1 AND $2 "
        f"ORDER BY report_date LIMIT 5",
        since_dt, today_dt,
    )

    title = metric.replace("_", " ").title()

    if output_format == "plotly_python":
        code = f'''"""
Auto-generated by analytics-mcp-server  |  {today}
Metric : {metric}
Period : last {days} days
Sample : {json.dumps(sample_rows[:3], default=str)}
"""
import asyncio, os
import asyncpg, pandas as pd, plotly.express as px

DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/analytics_mcp")

async def fetch():
    conn = await asyncpg.connect(DB_URL)
    rows = await conn.fetch("""
        SELECT report_date AS period, {col} AS value
        FROM   {table}
        WHERE  report_date BETWEEN $1 AND $2
        ORDER  BY report_date
    """, "{since}", "{today}")
    await conn.close()
    return pd.DataFrame([dict(r) for r in rows])

def chart(df):
    fig = px.line(df, x="period", y="value",
                  title="{title} — Last {days} Days",
                  labels={{"period": "Date", "value": "{metric}"}},
                  template="plotly_white")
    fig.update_traces(line=dict(width=2.5, color="#4F46E5"))
    fig.update_layout(hovermode="x unified")
    fig.show()

if __name__ == "__main__":
    chart(asyncio.run(fetch()))
'''

    elif output_format == "sql_query":
        code = f"""-- Auto-generated by analytics-mcp-server  |  {today}
-- Metric : {metric}  |  Last {days} days

SELECT
    DATE_TRUNC('day', report_date)::DATE  AS period,
    {col}                                 AS value
FROM  {table}
WHERE report_date BETWEEN '{since}' AND '{today}'
ORDER BY period;
"""

    elif output_format == "html_chart":
        points = [
            {"x": str(r.get("period", "")), "y": float(r.get("value") or 0)}
            for r in sample_rows
        ]
        code = f"""<!-- Auto-generated by analytics-mcp-server  |  {today} -->
<!DOCTYPE html><html>
<head>
  <title>{title}</title>
  <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
</head>
<body>
  <div id="chart" style="width:100%;height:500px;"></div>
  <script>
    const pts = {json.dumps(points)};
    Plotly.newPlot('chart', [{{
      x: pts.map(p => p.x), y: pts.map(p => p.y),
      type: 'scatter', mode: 'lines+markers', name: '{metric}',
      line: {{color:'#4F46E5', width:2.5}}
    }}], {{
      title: '{title} — Last {days} Days',
      xaxis: {{title:'Date'}}, yaxis: {{title:'{metric}'}},
      template: 'plotly_white'
    }});
  </script>
</body></html>
"""
    else:
        code = f"-- Unknown format: {output_format}"

    return json.dumps({
        "metric": metric,
        "format": output_format,
        "generated_at": today,
        "live_sample_rows": sample_rows,
        "code": code,
    }, indent=2, default=str)


# ── Tool: run_dbt_model ────────────────────────────────────────

@app.tool(
    description=(
        "Trigger a dbt model to refresh analytics tables from raw data. "
        "Available models: stg_users, stg_orders, mart_revenue, mart_users, mart_product"
    )
)
async def run_dbt_model(model_name: str, full_refresh: bool = False) -> str:
    """
    Args:
        model_name: dbt model name (stg_users | stg_orders | mart_revenue |
                    mart_users | mart_product)
        full_refresh: Run full refresh — default is incremental
    """
    dbt_sql: dict[str, str] = {
        "mart_revenue": """
            INSERT INTO analytics.daily_revenue
                (report_date, total_eur, order_count, avg_order_eur, refund_rate)
            SELECT
                created_at::DATE,
                SUM(amount_eur) FILTER (WHERE status='completed'),
                COUNT(*) FILTER (WHERE status='completed'),
                AVG(amount_eur) FILTER (WHERE status='completed'),
                ROUND(COUNT(*) FILTER (WHERE status='refunded')::NUMERIC /
                      NULLIF(COUNT(*),0), 4)
            FROM raw.orders
            GROUP BY created_at::DATE
            ON CONFLICT (report_date) DO UPDATE
               SET total_eur=EXCLUDED.total_eur, order_count=EXCLUDED.order_count,
                   avg_order_eur=EXCLUDED.avg_order_eur, refund_rate=EXCLUDED.refund_rate
        """,
        "mart_users": """
            INSERT INTO analytics.user_metrics
                (report_date, new_users, active_users, churned_users, mrr_eur)
            SELECT
                d::DATE,
                COUNT(u.user_id) FILTER (WHERE u.created_at::DATE = d::DATE),
                COUNT(DISTINCT e.user_id),
                COUNT(u.user_id) FILTER (WHERE u.churned_at::DATE = d::DATE),
                SUM(CASE u.plan WHEN 'starter' THEN 29 WHEN 'pro' THEN 99
                    WHEN 'enterprise' THEN 499 ELSE 0 END)
                    FILTER (WHERE u.churned_at IS NULL)
            FROM generate_series(CURRENT_DATE-89, CURRENT_DATE,'1 day') d
            LEFT JOIN raw.users  u ON TRUE
            LEFT JOIN raw.events e ON e.occurred_at::DATE = d::DATE
            GROUP BY d
            ON CONFLICT (report_date) DO UPDATE
               SET new_users=EXCLUDED.new_users, active_users=EXCLUDED.active_users,
                   churned_users=EXCLUDED.churned_users, mrr_eur=EXCLUDED.mrr_eur
        """,
        "mart_product": """
            INSERT INTO analytics.product_metrics
                (report_date, feature_name, unique_users, total_uses, adoption_rate)
            SELECT
                recorded_date, feature_name,
                COUNT(DISTINCT user_id), SUM(usage_count),
                ROUND(COUNT(DISTINCT user_id)::NUMERIC /
                      NULLIF((SELECT COUNT(*) FROM raw.users),0), 4)
            FROM raw.feature_usage
            GROUP BY recorded_date, feature_name
            ON CONFLICT (report_date, feature_name) DO UPDATE
               SET unique_users=EXCLUDED.unique_users, total_uses=EXCLUDED.total_uses,
                   adoption_rate=EXCLUDED.adoption_rate
        """,
    }

    staging_models = {"stg_users", "stg_orders", "stg_events"}
    if model_name in staging_models:
        return json.dumps({
            "status": "skipped",
            "model": model_name,
            "reason": "Staging model is a view — no refresh needed",
        })

    if model_name not in dbt_sql:
        return json.dumps({
            "error": f"Unknown model '{model_name}'",
            "available_models": list(dbt_sql.keys()) + list(staging_models),
        })

    result = await db_exec(dbt_sql[model_name])
    return json.dumps({
        "status": "success",
        "model": model_name,
        "full_refresh": full_refresh,
        "rows_affected": result,
        "executed_at": str(datetime.utcnow()),
        "note": f"Equivalent dbt command: dbt run --select {model_name}",
    }, indent=2, default=str)


# ── Entrypoint ─────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Starting Analytics MCP Server (stdio)…")
    app.run(transport="stdio")
