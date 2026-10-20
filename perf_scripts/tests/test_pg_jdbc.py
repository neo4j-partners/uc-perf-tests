"""PostgreSQL via custom JDBC driver (TYPE JDBC, through SafeSpark)."""

from perf_scripts.connections import create_pg_jdbc_connection, pg_remote_query
from perf_scripts.queries import PG_RQ_NUM_RUNS, PG_RQ_QUERIES
from perf_scripts.reporting import avg_subsequent, print_warmup_report, timed
from perf_scripts.tests import PerfTest, TestContext, register

PG_JDBC_CONN = "pg_perf_jdbc"


@register("pg_jdbc", groups=["pg"])
class TestPgJdbc(PerfTest):
    """PostgreSQL via custom JDBC — measures SafeSpark overhead with a non-Neo4j driver."""

    def run(self, ctx: TestContext) -> None:
        if not ctx.pg_cfg or not ctx.pg_cfg.get("jdbc_jar_path"):
            print("  [SKIP] PostgreSQL JDBC not configured (no PG JAR)")
            return

        ctx.spark.sql(f"DROP CONNECTION IF EXISTS {PG_JDBC_CONN}")
        create_pg_jdbc_connection(ctx.spark, ctx.pg_cfg, PG_JDBC_CONN)
        ctx.vr.record("PG JDBC: connection created", True)

        for label, sql in PG_RQ_QUERIES.items():
            run_times = []
            for _ in range(PG_RQ_NUM_RUNS):
                with timed() as t:
                    pg_remote_query(ctx.spark, PG_JDBC_CONN, ctx.pg_cfg, sql).collect()
                run_times.append(t.elapsed)
            ctx.report_data.setdefault("pg_jdbc_safespark_runs", {})[label] = run_times
            # Run 1 keeps feeding the cross-driver comparison table.
            ctx.report_data.setdefault("pg_jdbc_safespark_times", {})[label] = run_times[0]
            ctx.vr.record(f"PG JDBC SafeSpark: {label}", True, f"{run_times[0]:.3f}s")
            ctx.vr.record(
                f"PG JDBC SafeSpark warmup: {label}",
                True,
                f"run 1 {run_times[0]:.3f}s, avg runs 2-{PG_RQ_NUM_RUNS} {avg_subsequent(run_times):.3f}s",
            )

        # Cleanup
        ctx.spark.sql(f"DROP CONNECTION IF EXISTS {PG_JDBC_CONN}")

    def print_report(self, ctx: TestContext) -> None:
        print_warmup_report(
            "PG JDBC WARM-UP EFFECT (repeated remote_query() through SafeSpark)",
            ctx.report_data.get("pg_jdbc_safespark_runs", {}),
        )
