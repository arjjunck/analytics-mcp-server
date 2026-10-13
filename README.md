# Analytics MCP Server

> A **Model Context Protocol (MCP) server** built with Python that exposes internal analytics data endpoints to AI coding tools, enabling agentic sessions to query live analytics results and auto-generate dashboard code from live data.

[![Python](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-4169E1?logo=postgresql&logoColor=white)](https://postgresql.org)
[![dbt](https://img.shields.io/badge/dbt-incremental-FF694B?logo=dbt&logoColor=white)](https://getdbt.com)
[![MCP](https://img.shields.io/badge/Protocol-MCP_2.x-8B5CF6)](https://modelcontextprotocol.io)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AI Coding Tools                          │
│         Cursor · Windsurf · Zed · any MCP-compatible tool       │
└───────────────────────────┬─────────────────────────────────────┘
                            │  Model Context Protocol (stdio)
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                   analytics-mcp-server                          │
│                      server.py                                  │
│                                                                 │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │  query_metric   │  │ get_dashboard_   │  │  run_dbt_     │  │
│  │                 │  │ data             │  │  model        │  │
│  └─────────────────┘  └──────────────────┘  └───────────────┘  │
│  ┌─────────────────┐  ┌──────────────────┐                     │
│  │ generate_       │  │ list_available_  │                     │
│  │ dashboard_code  │  │ metrics          │                     │
│  └─────────────────┘  └──────────────────┘                     │
└───────────────────────────┬─────────────────────────────────────┘
                            │  asyncpg
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                        PostgreSQL                               │
│                                                                 │
│   raw schema                    analytics schema (dbt marts)    │
│   ├── users                     ├── daily_revenue               │
│   ├── orders          ──dbt──►  ├── user_metrics                │
│   ├── events                    └── product_metrics             │
│   └── feature_usage                                             │
└─────────────────────────────────────────────────────────────────┘
                            │
              Airflow DAG — nightly 02:00 UTC
         stg models → mart models → tests → validation
```

**Stack mirrors data teams at Irish MNCs** (Stripe, HubSpot, Workday Dublin):
`Python 3.13` · `PostgreSQL 18` · `dbt` · `Apache Airflow` · `MCP 2.x` · `asyncpg`

---

## What it does

An AI tool connected to this server can:

- **Query any KPI** — *"show me weekly revenue for the last 90 days"*
- **Pull dashboard data** — *"give me the executive summary for this month"*
- **Generate runnable code** — *"write me a Plotly chart for MRR"* → returns working Python
- **Trigger dbt refreshes** — *"run the mart_revenue model"* → executes incremental SQL
- **Inspect data freshness** — *"what metrics are available and when were they last updated?"*

---

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL running locally
- A MCP-compatible AI tool (Cursor, Windsurf, Zed, etc.)

### Setup

```bash
git clone https://github.com/arjjunck/analytics-mcp-server
cd analytics-mcp-server
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
createdb analytics_mcp
psql analytics_mcp -f sql/01_schema.sql
psql analytics_mcp -f sql/02_seed.sql   # seeds 90 days of realistic data
```

### Run the tests

```bash
python tests/test_server.py
# 15/15 passed ✓
```

### Register with your AI tool

Add to your MCP client config (e.g. `~/.cursor/mcp.json`):

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

## MCP Tools

| Tool | Description |
|---|---|
| `query_metric` | Query a KPI by name with date range + granularity |
| `get_dashboard_data` | Structured JSON payload for 4 dashboard types |
| `generate_dashboard_code` | Auto-generate Plotly / SQL / HTML code from live data |
| `run_dbt_model` | Trigger incremental dbt model refresh |
| `list_available_metrics` | Discover metrics + data freshness |

### Available metrics
`daily_revenue` · `mrr` · `new_users` · `active_users` · `churn_rate` · `refund_rate` · `feature_adoption`

### Dashboard types
`revenue_overview` · `user_growth` · `product_adoption` · `executive_summary`

---

## Data Pipeline

```
raw.orders          ──► stg_orders (view)  ──► mart_revenue  (incremental)
raw.users           ──► stg_users  (view)  ──► mart_users    (incremental)
raw.feature_usage   ────────────────────── ► mart_product   (incremental)
                                                    │
                               Airflow DAG orchestrates nightly at 02:00 UTC
```

The dbt models use **incremental strategy** — only new rows are processed on each run, mirroring production patterns at scale.

---

## Project Structure

```
analytics-mcp-server/
├── server.py                      # MCP server — all 5 tool handlers
├── requirements.txt
├── .env.example
├── sql/
│   ├── 01_schema.sql              # raw + analytics schemas
│   └── 02_seed.sql                # 90 days of realistic SaaS data
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_users.sql
│       │   └── stg_orders.sql
│       └── marts/
│           ├── mart_revenue.sql   # incremental
│           └── mart_users.sql     # incremental
├── airflow/
│   └── dags/
│       └── analytics_pipeline.py  # nightly Airflow DAG
├── scripts/
│   └── setup.sh
└── tests/
    └── test_server.py             # 15/15 smoke tests
```

---

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Transport | stdio | MCP standard for local tools; zero network overhead |
| DB driver | asyncpg | Fastest async PostgreSQL driver; production-grade at scale |
| Transform layer | dbt incremental | Industry standard — 42% of DE job postings in 2025 |
| Orchestration | Airflow DAG | Most common scheduler at MNCs — 48% of DE roles require it |
| Schema split | raw / analytics | Medallion architecture pattern used at Stripe, HubSpot |
| Protocol | MCP 2.x | Latest spec; compatible with Cursor, Windsurf, Zed and others |

---

## Author

**Arjun** · [arjjunck@gmail.com](mailto:arjjunck@gmail.com) · [github.com/arjjunck](https://github.com/arjjunck)
