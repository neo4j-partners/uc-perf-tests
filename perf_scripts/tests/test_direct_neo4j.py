"""Baseline: queries Neo4j directly via Python driver, no Spark."""

from perf_scripts.queries import BENCHMARK_QUERIES
from perf_scripts.reporting import timed
from perf_scripts.tests import PerfTest, TestContext, register


@register("direct_neo4j", groups=["neo4j", "baseline"])
class TestDirectNeo4j(PerfTest):
    """Python driver baseline — measures raw Neo4j query latency without Spark."""

    def run(self, ctx: TestContext) -> None:
        driver = ctx.neo4j_driver()
        try:
            with driver.session(database=ctx.cfg["neo4j_database"]) as session:
                for label, qdef in BENCHMARK_QUERIES.items():
                    with timed() as t:
                        result = session.run(qdef.cypher).single()
                    cnt = result["cnt"]
                    ctx.report_data.setdefault("direct_times", {})[label] = t.elapsed
                    ctx.vr.record(f"Direct Neo4j: {label}", cnt >= 0, f"{cnt:,} in {t.elapsed:.3f}s")
        finally:
            driver.close()
