"""PostgreSQL via direct Spark JDBC (no UC, no SafeSpark)."""

from perf_scripts.connections import read_pg_direct_jdbc
from perf_scripts.queries import PG_RQ_NUM_RUNS, PG_RQ_QUERIES
from perf_scripts.reporting import avg_subsequent, print_warmup_report, timed
from perf_scripts.tests import PerfTest, TestContext, register


@register("pg_direct_jdbc", groups=["pg_direct"])
class TestPgDirectJdbc(PerfTest):
    """Same PostgreSQL queries as pg_jdbc, read with the Spark JDBC reader.

    The PostgreSQL driver ships with the runtime, so this isolates Spark JDBC cost
    from the UC/SafeSpark layer.
    """

    def run(self, ctx: TestContext) -> None:
        if not ctx.pg_cfg:
            print("  [SKIP] PostgreSQL not configured")
            return

        for label, sql in PG_RQ_QUERIES.items():
            name = label.removeprefix("remote_query ")
            run_times = []
            for _ in range(PG_RQ_NUM_RUNS):
                with timed() as t:
                    read_pg_direct_jdbc(ctx.spark, ctx.pg_cfg, sql).collect()
                run_times.append(t.elapsed)
            ctx.report_data.setdefault("pg_direct_jdbc_runs", {})[name] = run_times
            ctx.vr.record(
                f"PG direct JDBC: {name}",
                True,
                f"run 1 {run_times[0]:.3f}s, avg runs 2-{PG_RQ_NUM_RUNS} {avg_subsequent(run_times):.3f}s",
            )

    def print_report(self, ctx: TestContext) -> None:
        print_warmup_report(
            "PG DIRECT JDBC WARM-UP EFFECT (Spark JDBC reader, no UC, no SafeSpark)",
            ctx.report_data.get("pg_direct_jdbc_runs", {}),
        )
