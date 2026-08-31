"""Unit tests for the focused UC JDBC smoke diagnostic."""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from perf_scripts.tests.test_uc_smoke import (
    EXPECTED_COUNT,
    TestUcSmoke as UcSmokePerfTest,
)


class _ResultDataFrame:
    def collect(self):
        return [{"cnt": EXPECTED_COUNT}]


class UcSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.validation = Mock()
        self.context = SimpleNamespace(
            spark=Mock(),
            cfg={"uc_connection_name": "existing_connection"},
            vr=self.validation,
        )
        self.test = UcSmokePerfTest(name="uc_smoke", groups=["uc_smoke"])

    @patch("perf_scripts.tests.test_uc_smoke.remote_query")
    @patch("perf_scripts.tests.test_uc_smoke.read_neo4j_jdbc")
    def test_runs_both_read_only_uc_paths(self, read_jdbc, run_remote) -> None:
        read_jdbc.return_value = _ResultDataFrame()
        run_remote.return_value = _ResultDataFrame()

        self.test.run(self.context)

        read_jdbc.assert_called_once_with(
            self.context.spark,
            self.context.cfg,
            "cnt LONG",
            query="SELECT COUNT(*) AS cnt FROM MaintenanceEvent",
        )
        run_remote.assert_called_once_with(
            self.context.spark,
            self.context.cfg,
            "SELECT COUNT(*) AS cnt FROM MaintenanceEvent",
        )
        self.assertEqual(self.validation.record.call_count, 2)
        self.assertTrue(all(call.args[1] for call in self.validation.record.call_args_list))

    @patch("perf_scripts.tests.test_uc_smoke.remote_query")
    @patch("perf_scripts.tests.test_uc_smoke.read_neo4j_jdbc")
    def test_dataframe_failure_does_not_skip_remote_query(
        self,
        read_jdbc,
        run_remote,
    ) -> None:
        read_jdbc.side_effect = RuntimeError("sandbox channel closed")
        run_remote.return_value = _ResultDataFrame()

        self.test.run(self.context)

        run_remote.assert_called_once()
        first, second = self.validation.record.call_args_list
        self.assertFalse(first.args[1])
        self.assertTrue(second.args[1])


if __name__ == "__main__":
    unittest.main()
