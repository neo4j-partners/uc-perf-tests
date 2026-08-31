"""Focused, read-only smoke tests for an existing UC JDBC connection."""

from perf_scripts.connections import read_neo4j_jdbc, remote_query
from perf_scripts.reporting import timed
from perf_scripts.tests import PerfTest, TestContext, register


SOURCE_QUERY = "SELECT COUNT(*) AS cnt FROM MaintenanceEvent"
EXPECTED_COUNT = 300


@register("uc_smoke", groups=["uc_smoke"])
class TestUcSmoke(PerfTest):
    """Separate UC JDBC DataFrame and ``remote_query()`` connectivity checks.

    The source query contains no string literals, so failures here are independent
    of nested SQL literal escaping. The test only uses the configured connection;
    it never creates, replaces, or drops it.
    """

    def run(self, ctx: TestContext) -> None:
        self._test_dataframe_api(ctx)
        self._test_remote_query(ctx)

    def _test_dataframe_api(self, ctx: TestContext) -> None:
        try:
            with timed() as timer:
                dataframe = read_neo4j_jdbc(
                    ctx.spark,
                    ctx.cfg,
                    "cnt LONG",
                    query=SOURCE_QUERY,
                )
                count = dataframe.collect()[0]["cnt"]
            ctx.vr.record(
                "UC smoke: JDBC DataFrame API",
                count == EXPECTED_COUNT,
                f"{count:,} in {timer.elapsed:.3f}s",
            )
        except Exception as exc:
            ctx.vr.record(
                "UC smoke: JDBC DataFrame API",
                False,
                f"{type(exc).__name__}: {exc}"[:240],
            )

    def _test_remote_query(self, ctx: TestContext) -> None:
        try:
            with timed() as timer:
                dataframe = remote_query(ctx.spark, ctx.cfg, SOURCE_QUERY)
                count = dataframe.collect()[0]["cnt"]
            ctx.vr.record(
                "UC smoke: remote_query()",
                count == EXPECTED_COUNT,
                f"{count:,} in {timer.elapsed:.3f}s",
            )
        except Exception as exc:
            ctx.vr.record(
                "UC smoke: remote_query()",
                False,
                f"{type(exc).__name__}: {exc}"[:240],
            )
