"""Individual remote_query() calls through UC JDBC."""

from perf_scripts.queries import BENCHMARK_QUERIES
from perf_scripts.reporting import timed
from perf_scripts.tests import PerfTest, TestContext, register


@register("remote_query", groups=["neo4j", "uc"])
class TestRemoteQuery(PerfTest):
    """Measures per-query overhead through the UC/SafeSpark pipeline."""

    def run(self, ctx: TestContext) -> None:
        conn = ctx.cfg["uc_connection_name"]

        for label, qdef in BENCHMARK_QUERIES.items():
            with timed() as t:
                result = ctx.spark.sql(f"""
                    SELECT * FROM remote_query('{conn}',
                        query => '{qdef.sql_escaped}')
                """).collect()
            cnt = result[0][qdef.result_column]
            ctx.report_data.setdefault("rq_times", {})[label] = t.elapsed
            ctx.vr.record(f"remote_query: {label}", cnt == qdef.expected_count, f"{cnt:,} (expected {qdef.expected_count:,}) in {t.elapsed:.3f}s")
