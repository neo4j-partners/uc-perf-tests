"""Basic connectivity validation across all connection paths.

Validates each connection path with a simple SELECT 1 query.
"""

from perf_scripts.connections import (
    create_uc_connection,
    read_direct_jdbc,
    read_neo4j_jdbc,
    read_spark_connector,
    remote_query,
)
from perf_scripts.reporting import timed
from perf_scripts.tests import PerfTest, TestContext, register


@register("connectivity", groups=["neo4j", "baseline"])
class TestConnectivity(PerfTest):
    """Validates each connection path works: Python driver, Spark Connector,
    direct JDBC, UC connection, UC JDBC, and remote_query().
    """

    def run(self, ctx: TestContext) -> None:
        self._test_python_driver(ctx)
        self._test_spark_connector(ctx)
        self._test_direct_jdbc(ctx)
        self._test_uc_connection(ctx)
        self._test_uc_jdbc(ctx)
        self._test_uc_remote_query(ctx)

    def _test_python_driver(self, ctx: TestContext) -> None:
        try:
            with timed() as t:
                driver = ctx.neo4j_driver()
                driver.verify_connectivity()
            with driver.session(database=ctx.cfg["neo4j_database"]) as session:
                val = session.run("RETURN 1 AS test").single()["test"]
            driver.close()
            ctx.vr.record("Python driver connectivity", val == 1, f"{t.ms:.0f}ms")
        except Exception as e:
            ctx.vr.record("Python driver connectivity", False, str(e)[:120])

    def _test_spark_connector(self, ctx: TestContext) -> None:
        try:
            with timed() as t:
                df = read_spark_connector(
                    ctx.spark, ctx.cfg,
                    query="RETURN 'ok' AS message, 1 AS value",
                )
                rows = df.collect()
            ctx.vr.record(
                "Spark Connector",
                len(rows) == 1 and rows[0]["value"] == 1,
                f"{t.ms:.0f}ms",
            )
        except Exception as e:
            ctx.vr.record("Spark Connector", False, str(e)[:120])

    def _test_direct_jdbc(self, ctx: TestContext) -> None:
        try:
            with timed() as t:
                df = read_direct_jdbc(ctx.spark, ctx.cfg, "SELECT 1 AS value", "value INT")
                val = df.collect()[0]["value"]
            ctx.vr.record("Direct JDBC SQL translation", val == 1, f"{t.ms:.0f}ms")
        except Exception as e:
            ctx.vr.record("Direct JDBC SQL translation", False, str(e)[:120])

    def _test_uc_connection(self, ctx: TestContext) -> None:
        try:
            with timed() as t:
                create_uc_connection(ctx.spark, ctx.cfg)
            ctx.vr.record("UC connection created", True, f"{ctx.cfg['uc_connection_name']}, {t.ms:.0f}ms")
        except Exception as e:
            ctx.vr.record("UC connection created", False, str(e)[:120])

        try:
            df = ctx.spark.sql(f"DESCRIBE CONNECTION {ctx.cfg['uc_connection_name']}")
            ctx.vr.record("UC connection described", df.count() > 0)
        except Exception as e:
            ctx.vr.record("UC connection described", False, str(e)[:120])

    def _test_uc_jdbc(self, ctx: TestContext) -> None:
        try:
            with timed() as t:
                df = read_neo4j_jdbc(ctx.spark, ctx.cfg, "test INT", query="SELECT 1 AS test")
                val = df.collect()[0]["test"]
            ctx.vr.record("UC JDBC basic query", val == 1, f"{t.ms:.0f}ms")
        except Exception as e:
            ctx.vr.record("UC JDBC basic query", False, str(e)[:120])

    def _test_uc_remote_query(self, ctx: TestContext) -> None:
        try:
            with timed() as t:
                df = remote_query(ctx.spark, ctx.cfg, "SELECT 1 AS test")
                val = df.collect()[0]["test"]
            ctx.vr.record("UC remote_query()", val == 1, f"{t.ms:.0f}ms")
        except Exception as e:
            ctx.vr.record("UC remote_query()", False, str(e)[:120])
