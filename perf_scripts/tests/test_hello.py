"""Smoke test to verify remote execution on Databricks."""

import os
import sys

from perf_scripts.tests import PerfTest, TestContext, register


@register("hello", groups=[])
class TestHello(PerfTest):
    """Minimal smoke test — verifies Python, Spark, and Neo4j driver availability."""

    def run(self, ctx: TestContext) -> None:
        print(f"  Python version: {sys.version}")
        print(f"  Spark version: {ctx.spark.version}")
        print(f"  DATABRICKS_RUNTIME_VERSION: {os.environ.get('DATABRICKS_RUNTIME_VERSION', 'not set')}")
        try:
            import neo4j
            print(f"  Neo4j Python driver: {neo4j.__version__}")
        except ImportError:
            print("  Neo4j Python driver: NOT found")
        ctx.vr.record("Remote execution", True, "smoke test")
