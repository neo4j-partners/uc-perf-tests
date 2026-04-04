"""Warm-up effect: repeated query timing to detect connection/JIT warm-up."""

from perf_scripts.queries import BENCHMARK_QUERIES
from perf_scripts.reporting import timed
from perf_scripts.tests import PerfTest, TestContext, register

NUM_RUNS = 5


@register("warmup", groups=["neo4j", "uc"])
class TestWarmup(PerfTest):
    """Runs the same remote_query() repeatedly to detect warm-up effects."""

    def run(self, ctx: TestContext) -> None:
        conn = ctx.cfg["uc_connection_name"]
        qdef = BENCHMARK_QUERIES["MaintenanceEvent count"]
        test_sql = f"""
            SELECT * FROM remote_query('{conn}',
                query => '{qdef.sql_escaped}')
        """

        run_times = []
        for _ in range(NUM_RUNS):
            with timed() as t:
                ctx.spark.sql(test_sql).collect()
            run_times.append(t.elapsed)

        ctx.report_data["warmup_runs"] = run_times

        avg_subsequent = sum(run_times[1:]) / len(run_times[1:]) if len(run_times) > 1 else 0
        penalty = run_times[0] - avg_subsequent

        ctx.vr.record("Warmup: first run (cold)", True, f"{run_times[0]:.3f}s")
        ctx.vr.record(f"Warmup: avg runs 2-{NUM_RUNS}", True, f"{avg_subsequent:.3f}s")
        ctx.vr.record("Warmup: penalty", True, f"{penalty:+.3f}s")

    def print_report(self, ctx: TestContext) -> None:
        runs = ctx.report_data.get("warmup_runs", [])
        if not runs:
            return
        print(f"\n{'=' * 70}")
        print("WARM-UP EFFECT (repeated remote_query())")
        print(f"{'=' * 70}")
        for i, t in enumerate(runs):
            marker = " <-- cold start" if i == 0 else ""
            print(f"  Run {i + 1}: {t:.3f}s{marker}")
        avg_warm = sum(runs[1:]) / len(runs[1:]) if len(runs) > 1 else 0
        penalty = runs[0] - avg_warm
        print(f"\n  First run:        {runs[0]:.3f}s")
        print(f"  Avg subsequent:   {avg_warm:.3f}s")
        print(f"  Min:              {min(runs):.3f}s")
        print(f"  Max:              {max(runs):.3f}s")
        print(f"  Warm-up penalty:  {penalty:+.3f}s")
