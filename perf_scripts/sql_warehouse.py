"""Benchmark Neo4j ``remote_query()`` calls on a Databricks SQL Warehouse.

This module intentionally runs outside Spark Jobs. It submits statements through
the SQL Statement Execution API using the Databricks CLI profile for
authentication.

Run it from the repository root::

    uv run python -m perf_scripts.sql_warehouse
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from perf_scripts.config import load_dotenv
from perf_scripts.queries import BENCHMARK_QUERIES, QueryDef


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATEMENTS_PATH = "/api/2.0/sql/statements"
TERMINAL_STATES = frozenset({"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"})


class StatementExecutionError(RuntimeError):
    """Raised when a SQL Warehouse statement fails or returns invalid data."""


@dataclass(frozen=True)
class WarehouseQueryResult:
    """Validated result and end-to-end latency for one benchmark statement."""

    name: str
    count: int
    elapsed_seconds: float
    statement_id: str


class DatabricksCliApi:
    """Small JSON API client authenticated by a Databricks CLI profile."""

    def __init__(self, profile: str) -> None:
        self.profile = profile

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, object] | None = None,
    ) -> dict:
        """Call a workspace API with ``databricks api`` and decode its JSON."""
        command = [
            "databricks",
            "api",
            method.lower(),
            path,
            "--profile",
            self.profile,
            "--output",
            "json",
        ]
        if body is not None:
            command.extend(["--json", json.dumps(body)])

        try:
            completed = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or "").strip()
            suffix = f": {detail}" if detail else ""
            raise StatementExecutionError(
                f"Databricks CLI {method.upper()} {path} failed{suffix}"
            ) from exc

        try:
            return json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise StatementExecutionError(
                f"Databricks CLI {method.upper()} {path} returned invalid JSON"
            ) from exc


def _remote_query_statement(connection_name: str, query: QueryDef) -> str:
    """Build a SQL Warehouse statement for a single benchmark query."""
    escaped_connection = connection_name.replace("'", "''")
    return (
        f"SELECT * FROM remote_query('{escaped_connection}', "
        f"query => '{query.sql_escaped}')"
    )


def _statement_error(response: dict) -> str:
    status = response.get("status") or {}
    error = status.get("error") or {}
    return error.get("message") or status.get("error_message") or "unknown error"


def _validate_count(response: dict, query: QueryDef) -> int:
    """Extract and validate the one-row integer count against the expected count."""
    rows = (response.get("result") or {}).get("data_array")
    if not isinstance(rows, list) or len(rows) != 1:
        raise StatementExecutionError(
            f"{query.name} returned {0 if not rows else len(rows)} rows; expected one"
        )
    row = rows[0]
    if not isinstance(row, list) or len(row) != 1:
        raise StatementExecutionError(
            f"{query.name} did not return exactly one count column"
        )

    value = row[0]
    try:
        count = int(value)
    except (TypeError, ValueError) as exc:
        raise StatementExecutionError(
            f"{query.name} returned a non-integer count"
        ) from exc
    if isinstance(value, float) and not value.is_integer():
        raise StatementExecutionError(
            f"{query.name} returned a non-integer count"
        )
    if count != query.expected_count:
        raise StatementExecutionError(
            f"{query.name} returned {count}; expected {query.expected_count}"
        )
    return count


def execute_benchmark_query(
    api: DatabricksCliApi,
    warehouse_id: str,
    connection_name: str,
    query: QueryDef,
    *,
    poll_interval: float = 1.0,
    timeout: float = 900.0,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> WarehouseQueryResult:
    """Submit, poll, validate, and time one SQL Warehouse query."""
    now = clock or time.perf_counter
    sleep = sleeper or time.sleep
    started = now()
    response = api.request(
        "POST",
        STATEMENTS_PATH,
        {
            "warehouse_id": warehouse_id,
            "statement": _remote_query_statement(connection_name, query),
            "wait_timeout": "0s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
    )
    statement_id = response.get("statement_id")
    if not statement_id:
        raise StatementExecutionError("Statement submission returned no statement_id")

    while (response.get("status") or {}).get("state") not in TERMINAL_STATES:
        if now() - started >= timeout:
            raise StatementExecutionError(
                f"Statement {statement_id} timed out after {timeout:.1f}s"
            )
        sleep(poll_interval)
        response = api.request("GET", f"{STATEMENTS_PATH}/{statement_id}")

    state = (response.get("status") or {}).get("state")
    elapsed = now() - started
    if state != "SUCCEEDED":
        raise StatementExecutionError(
            f"Statement {statement_id} ended in {state}: {_statement_error(response)}"
        )

    return WarehouseQueryResult(
        name=query.name,
        count=_validate_count(response, query),
        elapsed_seconds=elapsed,
        statement_id=statement_id,
    )


def run_benchmarks(
    api: DatabricksCliApi,
    warehouse_id: str,
    connection_name: str,
    *,
    poll_interval: float = 1.0,
    timeout: float = 900.0,
) -> list[WarehouseQueryResult]:
    """Run all canonical Neo4j count benchmarks sequentially."""
    results = []
    for query in BENCHMARK_QUERIES.values():
        result = execute_benchmark_query(
            api,
            warehouse_id,
            connection_name,
            query,
            poll_interval=poll_interval,
            timeout=timeout,
        )
        results.append(result)
        print(
            f"  [PASS] SQL Warehouse remote_query: {result.name} — "
            f"{result.count:,} in {result.elapsed_seconds:.3f}s"
        )
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark Neo4j remote_query() on a Databricks SQL Warehouse"
    )
    parser.add_argument("--env-file", default=str(PROJECT_ROOT / ".env"))
    parser.add_argument("--profile", help="Databricks CLI profile")
    parser.add_argument("--warehouse-id", help="SQL Warehouse ID")
    parser.add_argument("--connection-name", help="Unity Catalog connection name")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=900.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    env = load_dotenv(args.env_file)
    profile = args.profile or env.get("DATABRICKS_PROFILE") or "DEFAULT"
    warehouse_id = args.warehouse_id or env.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    connection_name = args.connection_name or env.get("UC_CONNECTION_NAME", "")

    if not warehouse_id:
        raise SystemExit(
            "DATABRICKS_SQL_WAREHOUSE_ID is required in .env or --warehouse-id"
        )
    if not connection_name:
        raise SystemExit("UC_CONNECTION_NAME is required in .env or --connection-name")
    if args.poll_interval < 0:
        raise SystemExit("--poll-interval must be non-negative")
    if args.timeout <= 0:
        raise SystemExit("--timeout must be positive")

    print("Neo4j remote_query() SQL Warehouse benchmark")
    print(f"  Profile:    {profile}")
    print(f"  Warehouse:  {warehouse_id}")
    print(f"  Connection: {connection_name}")
    results = run_benchmarks(
        DatabricksCliApi(profile),
        warehouse_id,
        connection_name,
        poll_interval=args.poll_interval,
        timeout=args.timeout,
    )
    total = sum(result.elapsed_seconds for result in results)
    print(f"Completed {len(results)} queries in {total:.3f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
