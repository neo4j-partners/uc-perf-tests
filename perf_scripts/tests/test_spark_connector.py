"""Spark Connector: schema inference vs data pull timing."""

from perf_scripts.connections import read_spark_connector
from perf_scripts.queries import CONNECTOR_LABELS
from perf_scripts.reporting import timed
from perf_scripts.tests import PerfTest, TestContext, register


@register("spark_connector", groups=["neo4j"])
class TestSparkConnector(PerfTest):
    """Times .load() (schema inference) vs .count() (data pull) separately."""

    def run(self, ctx: TestContext) -> None:
        for label in CONNECTOR_LABELS:
            with timed() as t_load:
                df = read_spark_connector(ctx.spark, ctx.cfg, labels=label)
            with timed() as t_count:
                count = df.count()

            ctx.report_data.setdefault("connector_times", {})[label] = {
                "load": t_load.elapsed,
                "count": t_count.elapsed,
                "total": t_load.elapsed + t_count.elapsed,
                "rows": count,
            }
            ctx.vr.record(
                f"Connector: {label}", count > 0,
                f"load={t_load.elapsed:.3f}s count={t_count.elapsed:.3f}s ({count:,} rows)",
            )

    def print_report(self, ctx: TestContext) -> None:
        conn_times = ctx.report_data.get("connector_times", {})
        if not conn_times:
            return
        print(f"\n{'=' * 70}")
        print("SPARK CONNECTOR: SCHEMA INFERENCE vs DATA PULL")
        print(f"{'=' * 70}")
        print(f"  {'Label':25s} {'Load (schema)':>14s} {'Count (data)':>14s} {'Total':>10s} {'Rows':>8s}")
        print(f"  {'-' * 25} {'-' * 14} {'-' * 14} {'-' * 10} {'-' * 8}")
        for label, d in conn_times.items():
            print(
                f"  {label:25s} {d['load']:13.3f}s {d['count']:13.3f}s "
                f"{d['total']:9.3f}s {d['rows']:>7,}"
            )
