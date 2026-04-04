#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["python-dotenv"]
# ///
"""Deploy, refresh credentials for, or destroy a Databricks Lakebase instance."""

import argparse
import json
import os
import shutil
import subprocess
import sys


def run_cli(profile, *args):
    """Run a databricks CLI command and return parsed JSON output."""
    cmd = ["databricks", "--profile", profile, *args, "--output", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        print(f"CLI error: {stderr}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def require_env(name):
    """Return an env var or exit with an error."""
    value = os.environ.get(name)
    if not value:
        print(f"Error: {name} not set in .env or environment", file=sys.stderr)
        sys.exit(1)
    return value


def get_endpoint_path(profile, project_id):
    """Discover the default branch and endpoint, return (endpoint_path, branch_name, endpoint_name, host)."""
    branches = run_cli(profile, "postgres", "list-branches", f"projects/{project_id}")
    default_branch = next(
        (b for b in branches if b.get("status", {}).get("default")),
        branches[0],
    )
    branch_path = default_branch["name"]
    branch_name = branch_path.rsplit("/", 1)[-1]

    endpoints = run_cli(profile, "postgres", "list-endpoints", branch_path)
    ep = endpoints[0]
    endpoint_path = ep["name"]
    endpoint_name = endpoint_path.rsplit("/", 1)[-1]
    host = ep.get("status", {}).get("hosts", {}).get("host", "<pending>")

    return endpoint_path, branch_name, endpoint_name, host


def output_connectivity(project_id, branch_name, endpoint_name, host, username, token, expire_time, *, save=False):
    """Print connection details as JSON and optionally save to file."""
    connectivity = {
        "project": project_id,
        "branch": branch_name,
        "endpoint": endpoint_name,
        "connection": {
            "host": host,
            "port": 5432,
            "database": "databricks_postgres",
            "username": username,
            "password": token,
            "token_expires": expire_time,
            "jdbc_url": f"jdbc:postgresql://{host}:5432/databricks_postgres",
            "psql": f"PGPASSWORD='{token}' psql 'host={host} port=5432 dbname=databricks_postgres user={username}'",
        },
    }
    output = json.dumps(connectivity, indent=2)
    print("\n" + output)
    if save:
        out_path = os.path.join(os.path.dirname(__file__), f"{project_id}-connection.json")
        with open(out_path, "w") as f:
            f.write(output + "\n")
        print(f"\n  Saved to {out_path}")


def cmd_deploy(profile, project_id):
    """Create a new Lakebase project and output connection details."""
    print(f"Creating project '{project_id}' (profile: {profile})...")
    run_cli(
        profile,
        "postgres", "create-project", project_id,
        "--json", json.dumps({"spec": {"display_name": project_id}}),
    )
    print("  Project created.")

    endpoint_path, branch_name, endpoint_name, host = get_endpoint_path(profile, project_id)
    print(f"  Default branch: {branch_name}")
    print(f"  Default endpoint: {endpoint_name}")

    print("Generating database credential...")
    cred = run_cli(profile, "postgres", "generate-database-credential", endpoint_path)
    token = cred.get("token", "<error>")
    expire_time = cred.get("expire_time", "")
    print("  Credential generated.")

    me = run_cli(profile, "current-user", "me")
    username = me.get("userName", "<unknown>")

    output_connectivity(project_id, branch_name, endpoint_name, host, username, token, expire_time, save=True)


def cmd_refresh(profile, project_id):
    """Generate a fresh OAuth credential for an existing project."""
    print(f"Refreshing credentials for '{project_id}' (profile: {profile})...")

    endpoint_path, branch_name, endpoint_name, host = get_endpoint_path(profile, project_id)

    cred = run_cli(profile, "postgres", "generate-database-credential", endpoint_path)
    token = cred.get("token", "<error>")
    expire_time = cred.get("expire_time", "")
    print("  Credential generated.")

    me = run_cli(profile, "current-user", "me")
    username = me.get("userName", "<unknown>")

    output_connectivity(project_id, branch_name, endpoint_name, host, username, token, expire_time, save=True)


def cmd_destroy(profile, project_id):
    """Delete a Lakebase project and all its resources."""
    print(f"Deleting project '{project_id}' (profile: {profile})...")
    cmd = ["databricks", "--profile", profile, "postgres", "delete-project",
           f"projects/{project_id}", "--output", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        if "not found" in (result.stderr + result.stdout):
            print(f"  Project '{project_id}' does not exist. Nothing to delete.")
            return
        print(f"CLI error: {result.stderr.strip() or result.stdout.strip()}", file=sys.stderr)
        sys.exit(1)
    print(f"  Project '{project_id}' deleted.")


def main():
    from dotenv import load_dotenv

    load_dotenv()

    if not shutil.which("databricks"):
        print("Error: 'databricks' CLI not found. Install it first.", file=sys.stderr)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Manage a Databricks Lakebase instance")
    parser.add_argument(
        "command",
        choices=["deploy", "refresh", "destroy"],
        help="deploy: create project and output credentials, "
             "refresh: generate new OAuth credentials, "
             "destroy: delete the project",
    )
    args = parser.parse_args()

    project_id = require_env("LAKEBASE_PROJECT_NAME")
    profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")

    if args.command == "deploy":
        cmd_deploy(profile, project_id)
    elif args.command == "refresh":
        cmd_refresh(profile, project_id)
    elif args.command == "destroy":
        cmd_destroy(profile, project_id)


if __name__ == "__main__":
    main()
