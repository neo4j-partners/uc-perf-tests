"""Spark explain plans for remote_query() calls (informational)."""

import contextlib
import io

from perf_scripts.tests import PerfTest, TestContext, register


@register("explain", groups=["neo4j", "uc"])
class TestExplain(PerfTest):
    """Captures Spark explain output for remote_query() calls."""

    def run(self, ctx: TestContext) -> None:
        conn = ctx.cfg["uc_connection_name"]

        # Simple remote_query explain
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            ctx.spark.sql(f"""
                SELECT * FROM remote_query('{conn}',
                    query => 'SELECT COUNT(*) AS cnt FROM MaintenanceEvent')
            """).explain("extended")
        ctx.report_data["explain_simple"] = f.getvalue()
        ctx.vr.record("Explain: simple remote_query", True, "captured")

        # Compound CROSS JOIN explain
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            ctx.spark.sql(f"""
                SELECT maint.cnt AS total_maintenance_events, crit.cnt AS critical_events
                FROM
                    remote_query('{conn}',
                        query => 'SELECT COUNT(*) AS cnt FROM MaintenanceEvent') AS maint
                CROSS JOIN
                    remote_query('{conn}',
                        query => 'SELECT COUNT(*) AS cnt FROM Flight') AS crit
            """).explain("extended")
        ctx.report_data["explain_compound"] = f.getvalue()
        ctx.vr.record("Explain: compound CROSS JOIN", True, "captured")

    def print_report(self, ctx: TestContext) -> None:
        simple = ctx.report_data.get("explain_simple", "")
        compound = ctx.report_data.get("explain_compound", "")
        if not simple:
            return
        print(f"\n{'=' * 70}")
        print("SPARK EXPLAIN PLANS")
        print(f"{'=' * 70}")
        print("\n  --- Simple remote_query() ---")
        for line in simple.strip().splitlines():
            print(f"  {line}")
        if compound:
            print("\n  --- Compound CROSS JOIN ---")
            for line in compound.strip().splitlines():
                print(f"  {line}")
