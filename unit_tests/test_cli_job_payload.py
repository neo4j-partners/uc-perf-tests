"""Unit tests for the Databricks Jobs payload built by ``perf_scripts.cli``."""

from __future__ import annotations

import argparse
import json
import unittest
from unittest.mock import patch

from perf_scripts import cli


class SubmitJobPayloadTests(unittest.TestCase):
    """Verify compute-specific fields without submitting a Databricks job."""

    @staticmethod
    def _args(*, serverless: bool) -> argparse.Namespace:
        return argparse.Namespace(
            tests="direct_jdbc,remote_query",
            group="",
            exclude="",
            no_wait=True,
            serverless=serverless,
        )

    @staticmethod
    def _env() -> dict[str, str]:
        return {
            "DATABRICKS_PROFILE": "unit-test-profile",
            "DATABRICKS_CLUSTER_ID": "0123-456789-test123",
            "WORKSPACE_DIR": "/Workspace/Users/test/perf",
            "SERVERLESS_ENV_VERSION": "5",
            "JDBC_JAR_PATH": "/Volumes/test/dependencies/neo4j-jdbc.jar",
        }

    def _submitted_payload(self, *, serverless: bool) -> tuple[dict, object]:
        """Run submission with side effects mocked and return its JSON payload."""
        with (
            patch.object(cli, "_ensure_cluster") as ensure_cluster,
            patch.object(cli, "_build_params", return_value=[]) as build_params,
            patch.object(cli.subprocess, "run") as run,
        ):
            cli.cmd_submit(self._args(serverless=serverless), self._env())

        build_params.assert_called_once_with(self._env())
        run.assert_called_once()
        command = run.call_args.args[0]
        self.assertEqual(command[:3], ["databricks", "jobs", "submit"])
        self.assertIn("--no-wait", command)
        payload = json.loads(command[command.index("--json") + 1])
        return payload, ensure_cluster

    def test_classic_uses_existing_cluster_without_serverless_environment(self) -> None:
        payload, ensure_cluster = self._submitted_payload(serverless=False)

        self.assertNotIn("environments", payload)
        self.assertEqual(len(payload["tasks"]), 1)
        task = payload["tasks"][0]
        self.assertEqual(task["existing_cluster_id"], self._env()["DATABRICKS_CLUSTER_ID"])
        self.assertNotIn("environment_key", task)
        ensure_cluster.assert_called_once_with(
            self._env()["DATABRICKS_PROFILE"],
            self._env()["DATABRICKS_CLUSTER_ID"],
        )

    def test_serverless_attaches_jdbc_jar_and_neo4j_python_dependency(self) -> None:
        payload, ensure_cluster = self._submitted_payload(serverless=True)

        ensure_cluster.assert_not_called()
        self.assertEqual(len(payload["tasks"]), 1)
        task = payload["tasks"][0]
        self.assertEqual(task["environment_key"], "default")
        self.assertNotIn("existing_cluster_id", task)

        environments = payload["environments"]
        self.assertEqual(len(environments), 1)
        environment = environments[0]
        self.assertEqual(environment["environment_key"], task["environment_key"])
        spec = environment["spec"]
        self.assertEqual(spec["environment_version"], self._env()["SERVERLESS_ENV_VERSION"])
        self.assertIn(self._env()["JDBC_JAR_PATH"], spec["java_dependencies"])
        self.assertTrue(
            any(dependency.lower().startswith("neo4j") for dependency in spec["dependencies"]),
            f"Expected a Neo4j Python dependency, got {spec['dependencies']!r}",
        )


if __name__ == "__main__":
    unittest.main()
