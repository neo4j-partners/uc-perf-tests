"""Mocked tests for the SQL Warehouse Neo4j remote_query benchmark."""

from __future__ import annotations

import json
import subprocess
import unittest
from unittest.mock import Mock, patch

from perf_scripts.queries import BENCHMARK_QUERIES
from perf_scripts.sql_warehouse import (
    STATEMENTS_PATH,
    DatabricksCliApi,
    StatementExecutionError,
    execute_benchmark_query,
)


class DatabricksCliApiTests(unittest.TestCase):
    @patch("perf_scripts.sql_warehouse.subprocess.run")
    def test_post_uses_cli_profile_auth_and_json_body(self, run: Mock) -> None:
        run.return_value = subprocess.CompletedProcess([], 0, '{"statement_id":"abc"}', "")
        body = {"warehouse_id": "warehouse-1", "statement": "SELECT 1"}

        response = DatabricksCliApi("TEST").request("POST", STATEMENTS_PATH, body)

        self.assertEqual(response, {"statement_id": "abc"})
        command = run.call_args.args[0]
        self.assertEqual(command[:4], ["databricks", "api", "post", STATEMENTS_PATH])
        self.assertEqual(command[command.index("--profile") + 1], "TEST")
        self.assertEqual(json.loads(command[command.index("--json") + 1]), body)
        run.assert_called_once_with(
            command,
            check=True,
            capture_output=True,
            text=True,
        )


class ExecuteBenchmarkQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.query = BENCHMARK_QUERIES["Critical events"]

    def test_posts_then_polls_and_validates_count(self) -> None:
        api = Mock()
        api.request.side_effect = [
            {"statement_id": "statement-1", "status": {"state": "PENDING"}},
            {
                "statement_id": "statement-1",
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["42"]]},
            },
        ]
        clock = Mock(side_effect=[10.0, 10.1, 11.5])
        sleeper = Mock()

        result = execute_benchmark_query(
            api,
            "warehouse-1",
            "catalog.schema.neo4j_connection",
            self.query,
            poll_interval=0.25,
            clock=clock,
            sleeper=sleeper,
        )

        self.assertEqual(result.count, 42)
        self.assertEqual(result.statement_id, "statement-1")
        self.assertAlmostEqual(result.elapsed_seconds, 1.5)
        post_call, get_call = api.request.call_args_list
        self.assertEqual(post_call.args[:2], ("POST", STATEMENTS_PATH))
        body = post_call.args[2]
        self.assertEqual(body["warehouse_id"], "warehouse-1")
        self.assertIn("remote_query('catalog.schema.neo4j_connection'", body["statement"])
        self.assertIn(self.query.sql_escaped, body["statement"])
        self.assertEqual(body["wait_timeout"], "0s")
        self.assertEqual(get_call.args, ("GET", f"{STATEMENTS_PATH}/statement-1"))
        sleeper.assert_called_once_with(0.25)

    def test_failed_statement_raises_without_returning_results(self) -> None:
        api = Mock()
        api.request.return_value = {
            "statement_id": "statement-2",
            "status": {
                "state": "FAILED",
                "error": {"message": "remote_query failed"},
            },
        }

        with self.assertRaisesRegex(StatementExecutionError, "remote_query failed"):
            execute_benchmark_query(
                api,
                "warehouse-1",
                "neo4j_connection",
                self.query,
                clock=Mock(side_effect=[1.0, 2.0]),
            )

    def test_non_integer_count_is_rejected(self) -> None:
        api = Mock()
        api.request.return_value = {
            "statement_id": "statement-3",
            "status": {"state": "SUCCEEDED"},
            "result": {"data_array": [["not-a-count"]]},
        }

        with self.assertRaisesRegex(StatementExecutionError, "non-integer count"):
            execute_benchmark_query(
                api,
                "warehouse-1",
                "neo4j_connection",
                self.query,
                clock=Mock(side_effect=[1.0, 2.0]),
            )


if __name__ == "__main__":
    unittest.main()
