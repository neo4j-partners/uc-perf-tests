"""Direct JDBC via Spark (no UC, no SafeSpark)."""

from perf_scripts.connections import read_direct_jdbc
from perf_scripts.queries import BENCHMARK_QUERIES
from perf_scripts.reporting import timed
from perf_scripts.tests import PerfTest, TestContext, register


@register("direct_jdbc", groups=["neo4j", "baseline"])
class TestDirectJdbc(PerfTest):
    """Same queries via Spark DataFrame JDBC reader — isolates Spark overhead
    from the UC/SafeSpark layer.
    """

    def run(self, ctx: TestContext) -> None:
        for label, qdef in BENCHMARK_QUERIES.items():
            with timed() as t:
                df = read_direct_jdbc(ctx.spark, ctx.cfg, qdef.sql, qdef.schema)
                result = df.collect()
            cnt = result[0]["cnt"]
            ctx.report_data.setdefault("direct_jdbc_times", {})[label] = t.elapsed
            ctx.vr.record(f"Direct JDBC: {label}", cnt >= 0, f"{cnt:,} in {t.elapsed:.3f}s")
