"""Test infrastructure: PerfTest base class, registry, and TestContext."""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from perf_scripts.connections import get_neo4j_driver
from perf_scripts.reporting import ValidationResults

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Test registry
# ---------------------------------------------------------------------------
REGISTRY: dict[str, PerfTest] = {}


def register(name: str, groups: list[str]):
    """Class decorator that registers a PerfTest subclass.

    Usage::

        @register("direct_neo4j", groups=["neo4j", "baseline"])
        class TestDirectNeo4j(PerfTest):
            ...
    """
    def decorator(cls):
        instance = cls(name=name, groups=groups)
        REGISTRY[name] = instance
        return cls
    return decorator


# ---------------------------------------------------------------------------
# TestContext
# ---------------------------------------------------------------------------
@dataclass
class TestContext:
    """Shared context passed to every test's run() method."""
    spark: SparkSession
    cfg: dict
    pg_cfg: dict | None
    vr: ValidationResults
    report_data: dict = field(default_factory=dict)

    def neo4j_driver(self):
        """Create a fresh Neo4j driver from config. Caller must close it."""
        return get_neo4j_driver(self.cfg)


# ---------------------------------------------------------------------------
# PerfTest base class
# ---------------------------------------------------------------------------
class PerfTest:
    """Base class for all performance tests."""

    def __init__(self, name: str, groups: list[str]):
        self.name = name
        self.groups = groups

    def run(self, ctx: TestContext) -> None:
        """Execute the test. Must be overridden."""
        raise NotImplementedError

    def print_report(self, ctx: TestContext) -> None:
        """Print this test's section of the consolidated report. Optional."""

    def safe_run(self, ctx: TestContext) -> None:
        """Run the test with exception handling."""
        print(f"\n{'=' * 60}")
        print(f"  {self.name}")
        print(f"{'=' * 60}")
        try:
            self.run(ctx)
        except Exception as e:
            ctx.vr.record(f"{self.name} EXECUTION", False, f"Error: {e}")
            traceback.print_exc()
