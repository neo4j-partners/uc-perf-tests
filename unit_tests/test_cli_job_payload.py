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
    def _args(
        *,
        serverless: bool,
        tests: str = "direct_jdbc,remote_query",
        group: str = "",
        exclude: str = "",
    ) -> argparse.Namespace:
        return argparse.Namespace(
            tests=tests,
            group=group,
            exclude=exclude,
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

    def _submitted_payload(
        self,
        *,
        serverless: bool,
        tests: str = "direct_jdbc,remote_query",
        group: str = "",
    ) -> tuple[dict, object]:
        """Run submission with side effects mocked and return its JSON payload."""
        with (
            patch.object(cli, "_ensure_cluster") as ensure_cluster,
            patch.object(cli, "_build_params", return_value=[]) as build_params,
            patch.object(cli.subprocess, "run") as run,
        ):
            cli.cmd_submit(
                self._args(serverless=serverless, tests=tests, group=group),
                self._env(),
            )

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

    def test_serverless_uses_only_neo4j_python_dependency(self) -> None:
        payload, ensure_cluster = self._submitted_payload(
            serverless=True,
            tests="direct_neo4j,remote_query,warmup",
        )

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
        self.assertNotIn("java_dependencies", spec)
        self.assertTrue(
            any(dependency.lower().startswith("neo4j") for dependency in spec["dependencies"]),
            f"Expected a Neo4j Python dependency, got {spec['dependencies']!r}",
        )

    def test_serverless_rejects_explicit_jvm_dependent_tests(self) -> None:
        for test_name in cli.SERVERLESS_UNSUPPORTED_TEST_GROUPS:
            with self.subTest(test_name=test_name):
                with (
                    patch.object(cli, "_build_params") as build_params,
                    patch.object(cli.subprocess, "run") as run,
                ):
                    with self.assertRaisesRegex(
                        cli.ServerlessSelectionError,
                        test_name,
                    ):
                        cli.cmd_submit(
                            self._args(serverless=True, tests=test_name),
                            self._env(),
                        )

                build_params.assert_not_called()
                run.assert_not_called()

    def test_serverless_rejects_groups_containing_jvm_tests(self) -> None:
        for group in ("neo4j", "baseline", "uc"):
            with self.subTest(group=group):
                with self.assertRaises(cli.ServerlessSelectionError):
                    cli.cmd_submit(
                        self._args(serverless=True, tests="", group=group),
                        self._env(),
                    )

    def test_serverless_run_rejects_unsupported_tests_before_upload(self) -> None:
        args = self._args(serverless=True, tests="direct_jdbc")
        args.skip_upload = False

        with (
            patch.object(cli, "cmd_upload") as upload,
            patch.object(cli, "cmd_submit") as submit,
        ):
            with self.assertRaises(cli.ServerlessSelectionError):
                cli.cmd_run(args, self._env())

        upload.assert_not_called()
        submit.assert_not_called()

    def test_serverless_pg_group_remains_supported(self) -> None:
        payload, ensure_cluster = self._submitted_payload(
            serverless=True,
            tests="",
            group="pg",
        )

        ensure_cluster.assert_not_called()
        self.assertEqual(payload["tasks"][0]["environment_key"], "default")
        self.assertNotIn("java_dependencies", payload["environments"][0]["spec"])

    def test_serverless_uc_smoke_remains_supported(self) -> None:
        payload, ensure_cluster = self._submitted_payload(
            serverless=True,
            tests="uc_smoke",
        )

        ensure_cluster.assert_not_called()
        self.assertEqual(payload["tasks"][0]["environment_key"], "default")
        self.assertNotIn("java_dependencies", payload["environments"][0]["spec"])


if __name__ == "__main__":
    unittest.main()
