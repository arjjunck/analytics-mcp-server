"""
Analytics Pipeline DAG
======================
Orchestrates the nightly analytics refresh:
  1. Check DB connectivity
  2. Run dbt staging models (stg_users, stg_orders, stg_events)
  3. Run dbt mart models (mart_revenue, mart_users, mart_product)
  4. Validate row counts
  5. Notify on completion

This mirrors a real Airflow DAG used at companies like
Stripe, HubSpot, and Intercom in their Dublin data teams.

Schedule: nightly at 02:00 UTC (03:00 Irish time)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ── Default args (standard production config) ─────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

# ── DAG definition ─────────────────────────────────────────────

with DAG(
    dag_id="analytics_pipeline_nightly",
    default_args=default_args,
    description="Nightly refresh of analytics warehouse via dbt",
    schedule_interval="0 2 * * *",  # 02:00 UTC daily
    catchup=False,
    tags=["analytics", "dbt", "nightly"],
    doc_md="""
## Analytics Pipeline

Refreshes all analytics tables from raw operational data.

### Architecture
```
PostgreSQL (raw) → dbt staging → dbt marts → analytics schema → MCP server
```

### Models
| Model         | Type        | Refresh    |
|---------------|-------------|------------|
| stg_users     | view        | on demand  |
| stg_orders    | view        | on demand  |
| mart_revenue  | incremental | nightly    |
| mart_users    | incremental | nightly    |
| mart_product  | incremental | nightly    |
""",
) as dag:

    # ── Task 1: health check ───────────────────────────────────
    check_db = BashOperator(
        task_id="check_db_connectivity",
        bash_command=(
            'psql "$DATABASE_URL" -c "SELECT 1" > /dev/null && echo "DB OK"'
        ),
    )

    # ── Task 2: dbt staging layer ──────────────────────────────
    run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select staging --profiles-dir . --target prod"
        ),
    )

    # ── Task 3: dbt mart layer ─────────────────────────────────
    run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select marts --profiles-dir . --target prod"
        ),
    )

    # ── Task 4: dbt tests ──────────────────────────────────────
    run_tests = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --profiles-dir . --target prod"
        ),
    )

    # ── Task 5: row count validation ──────────────────────────
    def validate_row_counts(**context):
        """
        Ensures analytics tables were populated correctly.
        Raises if any table has zero rows — catches silent failures.
        """
        import os
        import psycopg2

        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        cur = conn.cursor()

        checks = {
            "analytics.daily_revenue": "SELECT COUNT(*) FROM analytics.daily_revenue",
            "analytics.user_metrics":  "SELECT COUNT(*) FROM analytics.user_metrics",
            "analytics.product_metrics": "SELECT COUNT(*) FROM analytics.product_metrics",
        }

        failures = []
        for table, sql in checks.items():
            cur.execute(sql)
            count = cur.fetchone()[0]
            print(f"  {table}: {count} rows")
            if count == 0:
                failures.append(f"{table} has 0 rows!")

        conn.close()

        if failures:
            raise ValueError("Row count validation FAILED:\n" + "\n".join(failures))

        print("✓ All row count checks passed")
        return {"status": "passed", "checked_at": str(datetime.utcnow())}

    validate = PythonOperator(
        task_id="validate_row_counts",
        python_callable=validate_row_counts,
        provide_context=True,
    )

    # ── Task 6: generate freshness report ─────────────────────
    def generate_freshness_report(**context):
        """Logs data freshness — surfaced in Airflow UI and logs."""
        import os
        import psycopg2

        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        cur = conn.cursor()
        cur.execute("""
            SELECT 'daily_revenue'   AS tbl, MAX(report_date) FROM analytics.daily_revenue
            UNION ALL
            SELECT 'user_metrics',          MAX(report_date) FROM analytics.user_metrics
            UNION ALL
            SELECT 'product_metrics',       MAX(report_date) FROM analytics.product_metrics
        """)
        rows = cur.fetchall()
        conn.close()

        report = {tbl: str(dt) for tbl, dt in rows}
        print("Data freshness report:", report)
        context["task_instance"].xcom_push(key="freshness", value=report)
        return report

    freshness = PythonOperator(
        task_id="freshness_report",
        python_callable=generate_freshness_report,
        provide_context=True,
    )

    # ── DAG wiring (dependency graph) ─────────────────────────
    check_db >> run_staging >> run_marts >> run_tests >> validate >> freshness
