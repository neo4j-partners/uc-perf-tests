"""No-op JDBC driver through SafeSpark: the fixed cost with nothing to load."""

from perf_scripts.connections import create_trivial_jdbc_connection
from perf_scripts.reporting import avg_subsequent, print_warmup_report, timed
from perf_scripts.tests import PerfTest, TestContext, register

TRIVIAL_CONN = "trivial_jdbc_perf_test"
TRIVIAL_QUERY = "SELECT 1 AS val"
NUM_RUNS = 5


@register("trivial_jdbc", groups=["trivial"])
class TestTrivialJdbc(PerfTest):
    """remote_query() against a 9 KB driver that returns one row and does no I/O.

    Any time beyond a plain Spark action is SafeSpark cost, since the driver itself
    does no work.
    """

    def run(self, ctx: TestContext) -> None:
        jar_path = ctx.cfg["trivial_jdbc_jar_path"]
        if not jar_path:
            print("  [SKIP] Trivial JDBC stub not configured (no TRIVIAL_JDBC_JAR_PATH)")
            return

        with timed() as t:
            ctx.spark.range(1).collect()
        ctx.report_data["trivial_spark_range_time"] = t.elapsed
        ctx.vr.record("Trivial JDBC: spark.range(1) baseline", True, f"{t.elapsed:.3f}s")

        create_trivial_jdbc_connection(ctx.spark, jar_path, TRIVIAL_CONN)
        ctx.vr.record("Trivial JDBC: connection created", True)
        try:
            run_times = []
            for _ in range(NUM_RUNS):
                with timed() as t:
                    rows = ctx.spark.sql(f"""
                        SELECT * FROM remote_query('{TRIVIAL_CONN}',
                            query => '{TRIVIAL_QUERY}')
                    """).collect()
                run_times.append(t.elapsed)
            ctx.report_data.setdefault("trivial_jdbc_runs", {})["remote_query SELECT 1"] = run_times
            ctx.vr.record("Trivial JDBC: returned val = 1", rows[0]["val"] == 1)
            ctx.vr.record(
                "Trivial JDBC SafeSpark: remote_query SELECT 1",
                True,
                f"run 1 {run_times[0]:.3f}s, avg runs 2-{NUM_RUNS} {avg_subsequent(run_times):.3f}s",
            )
        finally:
            ctx.spark.sql(f"DROP CONNECTION IF EXISTS {TRIVIAL_CONN}")

    def print_report(self, ctx: TestContext) -> None:
        print_warmup_report(
            "TRIVIAL JDBC STUB (remote_query() through SafeSpark, no-op driver)",
            ctx.report_data.get("trivial_jdbc_runs", {}),
        )
