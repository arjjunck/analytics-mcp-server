# Analytics MCP Server

> A **Model Context Protocol (MCP) server** built with Python and FastAPI that exposes internal analytics data endpoints to AI coding tools, enabling agentic sessions to query live analytics results and auto-generate dashboard code from live data.

[![Python](https://img.shields.io/badge/Python-3.13-blue)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue)](https://postgresql.org)
[![MCP](https://img.shields.io/badge/Protocol-MCP-purple)](https://modelcontextprotocol.io)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      AI Coding Tools                        │
│              (Claude Code, Cursor, Windsurf)                │
└──────────────────────────┬──────────────────────────────────┘
                           │  MCP stdio transport
┌──────────────────────────▼──────────────────────────────────┐
│                  analytics-mcp-server                        │
│                 server.py  (Python/asyncio)                  │
│                                                              │
│  Tools:  query_metric │ get_dashboard_data                   │
│          generate_dashboard_code │ run_dbt_model             │
└──────────────────────────┬──────────────────────────────────┘
                           │  asyncpg
┌──────────────────────────▼──────────────────────────────────┐
│                      PostgreSQL                              │
│  raw schema         │  analytics schema (dbt marts)          │
│  ├── users          │  ├── daily_revenue                     │
│  ├── orders         │  ├── user_metrics                      │
│  ├── events         │  └── product_metrics                   │
│  └── feature_usage  │                                        │
└─────────────────────┴───────────────────────────────────────┘
                           │  Airflow DAG (nightly 02:00 UTC)
                 dbt incremental models refresh analytics layer
```

**Stack mirrors Irish MNC data teams** (Stripe Dublin, HubSpot, Workday):
`Python` · `PostgreSQL` · `dbt` · `Apache Airflow` · `MCP` · `asyncpg`

---

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL running locally (Postgres.app on macOS)
- Claude Code installed (`npm install -g @anthropic-ai/claude-code`)

### 1. Clone and set up

```bash
git clone https://github.com/yourusername/analytics-mcp-server
cd analytics-mcp-server
bash scripts/setup.sh
```

The setup script:
- Creates a Python virtual environment
- Installs dependencies
- Creates the `analytics_mcp` PostgreSQL database
- Applies the schema and seeds 90 days of realistic data
- Registers the MCP server with Claude Code

### 2. Verify it works

```bash
source .venv/bin/activate
python tests/test_server.py
```

### 3. Use with Claude Code

Restart Claude Code — the server registers automatically. Then in any session:

```
> query the daily_revenue metric for the last 30 days
> show me an executive summary dashboard
> generate plotly python code for the mrr metric
> run the mart_revenue dbt model
```

---

## MCP Tools

### `query_metric`
Query a named KPI for a date range with configurable granularity.

```json
{
  "metric": "daily_revenue",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "granularity": "weekly"
}
```

**Available metrics:** `daily_revenue` · `mrr` · `new_users` · `active_users` · `churn_rate` · `refund_rate` · `feature_adoption`

---

### `get_dashboard_data`
Return a structured JSON payload for a dashboard type.

```json
{ "dashboard_type": "executive_summary", "days": 30 }
```

**Types:** `revenue_overview` · `user_growth` · `product_adoption` · `executive_summary`

---

### `generate_dashboard_code`
Auto-generate runnable code from live data.

```json
{
  "metric": "daily_revenue",
  "output_format": "plotly_python",
  "days": 30
}
```

**Formats:** `plotly_python` · `sql_query` · `html_chart`

---

### `run_dbt_model`
Trigger a dbt model refresh against the analytics warehouse.

```json
{ "model_name": "mart_revenue", "full_refresh": false }
```

**Models:** `stg_users` · `stg_orders` · `mart_revenue` · `mart_users` · `mart_product`

---

## Project Structure

```
analytics-mcp-server/
├── server.py                   # MCP server — all tool handlers
├── requirements.txt
├── .env.example
├── sql/
│   ├── 01_schema.sql           # PostgreSQL schema (raw + analytics)
│   └── 02_seed.sql             # 90 days of realistic seed data
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_users.sql
│       │   └── stg_orders.sql
│       └── marts/
│           ├── mart_revenue.sql    # incremental
│           └── mart_users.sql      # incremental
├── airflow/
│   └── dags/
│       └── analytics_pipeline.py  # Nightly Airflow DAG
├── scripts/
│   └── setup.sh                   # One-command setup
└── tests/
    └── test_server.py
```

---

## Manual MCP Registration

If the setup script didn't auto-register, add this to `~/.claude.json`:

```json
{
  "mcpServers": {
    "analytics-mcp": {
      "command": "/path/to/analytics-mcp-server/.venv/bin/python",
      "args": ["/path/to/analytics-mcp-server/server.py"],
      "env": {
        "DATABASE_URL": "postgresql://localhost:5432/analytics_mcp"
      }
    }
  }
}
```

---

## Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Transport | stdio | Standard for local MCP; used by Claude Code, Cursor |
| DB driver | asyncpg | Fastest async PostgreSQL driver; used in production at scale |
| Transform layer | dbt | Industry standard (42% of data engineering job postings in 2025) |
| Orchestration | Airflow | Most common scheduler at MNCs (48% of DE job postings) |
| Schema split | raw / analytics | Mirrors medallion architecture used at Stripe, HubSpot |

---

## Author

Arjun — [arjjunck@gmail.com](mailto:arjjunck@gmail.com)
