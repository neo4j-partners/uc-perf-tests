"""Mocked tests for the repeated PG remote_query warm-up measurement."""

from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import Mock, patch

from perf_scripts.queries import PG_RQ_NUM_RUNS, PG_RQ_QUERIES
from perf_scripts.reporting import avg_subsequent, print_warmup_report
from perf_scripts.tests.test_pg_builtin import TestPgBuiltin as PgBuiltinPerfTest
from perf_scripts.tests.test_pg_jdbc import TestPgJdbc as PgJdbcPerfTest


class PgWarmupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.validation = Mock()
        self.context = SimpleNamespace(
            spark=Mock(),
            pg_cfg={"database": "db", "jdbc_jar_path": "/jar"},
            vr=self.validation,
            report_data={},
        )

    def _run(
        self, test, module: str, create_fn: str, label_key: str, runs_key: str
    ) -> None:
        with (
            patch(f"{module}.pg_remote_query") as remote,
            patch(f"{module}.{create_fn}"),
            redirect_stdout(io.StringIO()),
        ):
            test.run(self.context)
        expected_calls = PG_RQ_NUM_RUNS * len(PG_RQ_QUERIES)
        self.assertEqual(remote.call_count, expected_calls)
        runs = self.context.report_data[runs_key]
        self.assertEqual(list(runs), list(PG_RQ_QUERIES))
        for label, times in runs.items():
            self.assertEqual(len(times), PG_RQ_NUM_RUNS)
            self.assertEqual(self.context.report_data[label_key][label], times[0])

    def test_pg_jdbc_repeats_each_remote_query(self) -> None:
        test = PgJdbcPerfTest(name="pg_jdbc", groups=["pg"])
        self._run(
            test,
            "perf_scripts.tests.test_pg_jdbc",
            "create_pg_jdbc_connection",
            "pg_jdbc_safespark_times",
            "pg_jdbc_safespark_runs",
        )
        self.assertTrue(all(call.args[1] for call in self.validation.record.call_args_list))

    def test_pg_builtin_repeats_each_remote_query(self) -> None:
        test = PgBuiltinPerfTest(name="pg_builtin", groups=["pg"])
        self._run(
            test,
            "perf_scripts.tests.test_pg_builtin",
            "create_pg_builtin_connection",
            "pg_builtin_times",
            "pg_builtin_runs",
        )

    def test_avg_subsequent_skips_first_run(self) -> None:
        self.assertEqual(avg_subsequent([10.0, 2.0, 4.0]), 3.0)
        self.assertEqual(avg_subsequent([10.0]), 0)

    def test_report_shows_runs_and_first_vs_subsequent(self) -> None:
        out = io.StringIO()
        with redirect_stdout(out):
            print_warmup_report("TITLE", {"q": [10.0, 2.0, 4.0]})
        text = out.getvalue()
        self.assertIn("Run 3: 4.000s", text)
        self.assertIn("First run:        10.000s", text)
        self.assertIn("Avg subsequent:   3.000s", text)
        self.assertIn("Max:              10.000s", text)
        self.assertIn("Warm-up penalty:  +7.000s", text)

    def test_report_is_silent_without_runs(self) -> None:
        out = io.StringIO()
        with redirect_stdout(out):
            print_warmup_report("TITLE", {})
        self.assertEqual(out.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
