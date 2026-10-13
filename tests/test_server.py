"""
Smoke tests for the analytics MCP server.
Run: python tests/test_server.py
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from server import (
    query_metric,
    get_dashboard_data,
    generate_dashboard_code,
    list_available_metrics,
    run_dbt_model,
    get_pool,
)


async def run_tests():
    print("\n🧪 Analytics MCP Server — Smoke Tests\n" + "─" * 42)
    passed = failed = 0

    async def test(name: str, coro):
        nonlocal passed, failed
        try:
            raw = await coro
            result = json.loads(raw)
            assert "error" not in result, f"Got error: {result['error']}"
            print(f"  ✓ {name}")
            passed += 1
            return result
        except Exception as exc:
            print(f"  ✗ {name}: {exc}")
            failed += 1
            return None

    await test("list_available_metrics", list_available_metrics())

    r = await test("query_metric: daily_revenue daily",
                   query_metric("daily_revenue", granularity="daily"))
    if r:
        print(f"     → {r['row_count']} rows")

    await test("query_metric: mrr weekly",
               query_metric("mrr", granularity="weekly"))

    await test("query_metric: feature_adoption",
               query_metric("feature_adoption"))

    # Unknown metric should return error key (not raise)
    raw = await query_metric("nonexistent_metric")
    r = json.loads(raw)
    if "error" in r:
        print("  ✓ query_metric: unknown metric handled gracefully")
        passed += 1
    else:
        print("  ✗ query_metric: expected error for unknown metric")
        failed += 1

    for dtype in ["revenue_overview", "user_growth", "product_adoption", "executive_summary"]:
        await test(f"get_dashboard_data: {dtype}",
                   get_dashboard_data(dtype, days=30))

    for fmt in ["plotly_python", "sql_query", "html_chart"]:
        r = await test(f"generate_dashboard_code: {fmt}",
                       generate_dashboard_code("daily_revenue", output_format=fmt, days=30))
        if r:
            print(f"     → {len(r['code'])} chars generated")

    for model in ["mart_revenue", "mart_users", "mart_product"]:
        await test(f"run_dbt_model: {model}",
                   run_dbt_model(model, full_refresh=False))

    print(f"\n{'─'*42}")
    print(f"  {passed}/{passed+failed} passed", "✓" if failed == 0 else "✗")
    if failed:
        print(f"  {failed} failed — check DB connection and seed data")
    print()

    pool = await get_pool()
    await pool.close()
    return failed == 0


if __name__ == "__main__":
    ok = asyncio.run(run_tests())
    sys.exit(0 if ok else 1)
