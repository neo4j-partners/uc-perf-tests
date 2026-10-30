"""CLI for uc-perf-tests. Replaces upload.sh, submit.sh, and validate.sh.

Usage::

    python -m perf_scripts.cli upload --all
    python -m perf_scripts.cli upload test_hello.py
    python -m perf_scripts.cli submit --group neo4j
    python -m perf_scripts.cli submit --tests warmup,delta
    python -m perf_scripts.cli run                       # upload + submit all
    python -m perf_scripts.cli run --skip-upload --group neo4j
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from perf_scripts.config import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SERVERLESS_UNSUPPORTED_TEST_GROUPS = {
    "connectivity": frozenset({"neo4j", "baseline"}),
    "direct_jdbc": frozenset({"neo4j", "baseline"}),
    "spark_connector": frozenset({"neo4j"}),
    "federated": frozenset({"neo4j", "uc"}),
}


class ServerlessSelectionError(ValueError):
    """Raised when serverless compute cannot run the selected tests."""


def _validate_serverless_selection(args: argparse.Namespace) -> None:
    """Reject tests that require JVM dependencies on serverless Python tasks."""
    if not getattr(args, "serverless", False):
        return

    tests_arg = getattr(args, "tests", "")
    group_arg = getattr(args, "group", "")
    exclude_arg = getattr(args, "exclude", "")
    excluded_groups = {
        group.strip() for group in exclude_arg.split(",") if group.strip()
    }

    if tests_arg:
        selected = {name.strip() for name in tests_arg.split(",") if name.strip()}
        unsupported = selected & SERVERLESS_UNSUPPORTED_TEST_GROUPS.keys()
    elif group_arg:
        unsupported = {
            name
            for name, groups in SERVERLESS_UNSUPPORTED_TEST_GROUPS.items()
            if group_arg in groups
        }
    else:
        unsupported = set(SERVERLESS_UNSUPPORTED_TEST_GROUPS)

    unsupported = {
        name
        for name in unsupported
        if not (SERVERLESS_UNSUPPORTED_TEST_GROUPS[name] & excluded_groups)
    }
    if not unsupported:
        return

    names = ", ".join(sorted(unsupported))
    raise ServerlessSelectionError(
        "Serverless Python tasks cannot run JVM-dependent tests: "
        f"{names}. Use classic compute for those tests, or select "
        "serverless-supported tests such as direct_neo4j, remote_query, "
        "and warmup. The pg group is also supported on serverless."
    )


def load_env() -> dict[str, str]:
    """Load .env from project root."""
    env = load_dotenv(PROJECT_ROOT / ".env")
    if os.environ.get("PERF_CLASSIC_CLUSTER_ID"):
        env["DATABRICKS_CLUSTER_ID"] = os.environ["PERF_CLASSIC_CLUSTER_ID"]
    return env


# ---------------------------------------------------------------------------
# Databricks helpers
# ---------------------------------------------------------------------------

def _run_databricks(cmd_args: list[str], profile: str) -> subprocess.CompletedProcess:
    """Run a databricks CLI command."""
    return subprocess.run(
        ["databricks", "--profile", profile] + cmd_args,
        check=True, capture_output=True, text=True,
    )


def _upload_file(local_path: Path, remote_path: str, profile: str) -> None:
    """Upload a single file to the workspace."""
    print(f"  {local_path.name} -> {remote_path}")
    _run_databricks([
        "workspace", "import",
        "--file", str(local_path),
        "--format", "AUTO",
        "--language", "PYTHON",
        "--overwrite",
        remote_path,
    ], profile)


def _ensure_cluster(profile: str, cluster_id: str) -> None:
    """Ensure the cluster is running. Delegates to cluster_utils.sh."""
    script = PROJECT_ROOT / "cluster_utils.sh"
    subprocess.run(
        ["bash", "-c", f'source "{script}" && ensure_cluster_running "{profile}" "{cluster_id}"'],
        check=True,
    )


def _load_pg_credentials() -> dict | None:
    """Load PG credentials from deploy-lakebase/perftest-connection.json.

    Returns a dict with host, port, database, username, password keys,
    or None if the file doesn't exist.
    """
    pg_json = PROJECT_ROOT / "deploy-lakebase" / "perftest-connection.json"
    if not pg_json.exists():
        return None
    with open(pg_json) as f:
        return json.load(f)["connection"]


def _build_params(env: dict) -> list[str]:
    """Build CLI parameters from .env values and Lakebase credentials."""
    params = []
    mapping = [
        ("NEO4J_URI", "--neo4j-uri"),
        ("NEO4J_USERNAME", "--neo4j-username"),
        ("NEO4J_PASSWORD", "--neo4j-password"),
        ("NEO4J_DATABASE", "--neo4j-database"),
        ("UC_CONNECTION_NAME", "--uc-connection-name"),
        ("JDBC_JAR_PATH", "--jdbc-jar-path"),
        ("TRIVIAL_JDBC_JAR_PATH", "--trivial-jdbc-jar-path"),
        ("LAKEHOUSE_CATALOG", "--lakehouse-catalog"),
        ("LAKEHOUSE_SCHEMA", "--lakehouse-schema"),
    ]
    for env_key, arg_name in mapping:
        val = env.get(env_key, "")
        if val:
            params.extend([arg_name, val])

    # PG credentials from deploy-lakebase connection JSON
    pg = _load_pg_credentials()
    if pg:
        params.extend(["--pg-host", pg["host"]])
        params.extend(["--pg-port", str(pg.get("port", 5432))])
        params.extend(["--pg-database", pg.get("database", "postgres")])
        params.extend(["--pg-username", pg["username"]])
        params.extend(["--pg-password", pg["password"]])

    # PG JDBC JAR path comes from .env (it's a Databricks Volume path)
    pg_jar = env.get("PG_JDBC_JAR_PATH", "")
    if pg_jar:
        params.extend(["--pg-jdbc-jar-path", pg_jar])

    return params


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------

def cmd_upload(args, env: dict) -> None:
    """Upload Python files to the Databricks workspace."""
    profile = env["DATABRICKS_PROFILE"]
    remote_dir = env["WORKSPACE_DIR"]

    _run_databricks(["workspace", "mkdirs", remote_dir], profile)
    _run_databricks(["workspace", "mkdirs", f"{remote_dir}/perf_scripts"], profile)
    _run_databricks(["workspace", "mkdirs", f"{remote_dir}/perf_scripts/tests"], profile)

    perf_dir = PROJECT_ROOT / "perf_scripts"

    if args.all:
        print(f"Uploading all perf_scripts/ to {remote_dir}/perf_scripts (profile: {profile})")
        print("---")
        for py_file in sorted(perf_dir.rglob("*.py")):
            if py_file.name.startswith("__pycache__"):
                continue
            rel = py_file.relative_to(PROJECT_ROOT)
            remote_path = f"{remote_dir}/{rel}"
            _upload_file(py_file, remote_path, profile)
    elif args.file:
        local_path = perf_dir / args.file
        if not local_path.exists():
            # Try in tests/
            local_path = perf_dir / "tests" / args.file
        if not local_path.exists():
            print(f"Error: {args.file} not found in perf_scripts/ or perf_scripts/tests/")
            sys.exit(1)
        rel = local_path.relative_to(PROJECT_ROOT)
        print(f"Uploading {rel} to {remote_dir}/{rel} (profile: {profile})")
        print("---")
        _upload_file(local_path, f"{remote_dir}/{rel}", profile)
    else:
        # Default: upload runner.py
        print(f"Uploading runner.py to {remote_dir}/perf_scripts (profile: {profile})")
        print("---")
        _upload_file(perf_dir / "runner.py", f"{remote_dir}/perf_scripts/runner.py", profile)

    print("\nUpload complete.")


def cmd_submit(args, env: dict) -> None:
    """Submit runner.py as a Databricks job."""
    _validate_serverless_selection(args)
    profile = env["DATABRICKS_PROFILE"]
    remote_dir = env["WORKSPACE_DIR"]
    serverless = getattr(args, "serverless", False)
    cluster_id = "" if serverless else env["DATABRICKS_CLUSTER_ID"]

    if not serverless:
        _ensure_cluster(profile, cluster_id)

    params = _build_params(env)

    if args.tests:
        params.extend(["--tests", args.tests])
    if args.group:
        params.extend(["--group", args.group])
    if args.exclude:
        params.extend(["--exclude", args.exclude])

    remote_path = f"{remote_dir}/perf_scripts/runner.py"
    compute = "serverless" if serverless else "classic"
    run_name = f"uc_perf_tests ({compute}): {args.tests or args.group or 'all'}"

    task = {
        "task_key": "run_script",
        "spark_python_task": {
            "python_file": remote_path,
            "parameters": params,
        },
    }
    job_json = {"run_name": run_name, "tasks": [task]}

    if serverless:
        env_version = env.get("SERVERLESS_ENV_VERSION", "5")
        environment_spec = {
            "environment_version": env_version,
            "dependencies": ["neo4j>=6,<7"],
        }

        task["environment_key"] = "default"
        job_json["environments"] = [{
            "environment_key": "default",
            "spec": environment_spec,
        }]
    else:
        task["existing_cluster_id"] = cluster_id

    print(f"Submitting job (profile: {profile})")
    print(f"  Script:   {remote_path}")
    if serverless:
        print(f"  Compute:  serverless (environment_version {env.get('SERVERLESS_ENV_VERSION', '5')})")
    else:
        print(f"  Cluster:  {cluster_id}")
    print(f"  Run name: {run_name}")
    if args.tests:
        print(f"  Tests:    {args.tests}")
    if args.group:
        print(f"  Group:    {args.group}")
    if args.exclude:
        print(f"  Exclude:  {args.exclude}")
    print("---")

    cmd = ["databricks", "jobs", "submit", "--profile", profile, "--json", json.dumps(job_json)]
    if hasattr(args, "no_wait") and args.no_wait:
        cmd.append("--no-wait")

    subprocess.run(cmd, check=True)
    print("\nJob submission complete.")


def cmd_run(args, env: dict) -> None:
    """Upload + submit (replaces validate.sh)."""
    _validate_serverless_selection(args)
    if not args.skip_upload:
        upload_args = argparse.Namespace(all=True, file=None)
        cmd_upload(upload_args, env)
        print("")

    cmd_submit(args, env)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(prog="perf_scripts.cli", description="UC Performance Test CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    # upload
    p_upload = sub.add_parser("upload", help="Upload scripts to Databricks workspace")
    p_upload.add_argument("file", nargs="?", help="Specific file to upload")
    p_upload.add_argument("--all", action="store_true", help="Upload all files")

    # submit
    p_submit = sub.add_parser("submit", help="Submit test runner as a Databricks job")
    p_submit.add_argument("--tests", default="", help="Comma-separated test names")
    p_submit.add_argument("--group", default="", help="Run tests in this group")
    p_submit.add_argument("--exclude", default="", help="Exclude tests in these groups")
    p_submit.add_argument("--no-wait", action="store_true", help="Don't wait for job completion")
    p_submit.add_argument("--serverless", action="store_true",
                          help="Run on serverless compute (env version from SERVERLESS_ENV_VERSION, default 5 ≈ DBR 18)")

    # run (upload + submit)
    p_run = sub.add_parser("run", help="Upload and submit (full pipeline)")
    p_run.add_argument("--skip-upload", action="store_true")
    p_run.add_argument("--tests", default="")
    p_run.add_argument("--group", default="")
    p_run.add_argument("--exclude", default="")
    p_run.add_argument("--no-wait", action="store_true")
    p_run.add_argument("--serverless", action="store_true",
                       help="Run on serverless compute (env version from SERVERLESS_ENV_VERSION, default 5 ≈ DBR 18)")

    args = parser.parse_args()
    env = load_env()

    try:
        if args.command == "upload":
            cmd_upload(args, env)
        elif args.command == "submit":
            cmd_submit(args, env)
        elif args.command == "run":
            cmd_run(args, env)
    except ServerlessSelectionError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
