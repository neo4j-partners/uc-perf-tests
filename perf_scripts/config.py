"""Configuration: argument parsing, env loading, and config derivation."""

from __future__ import annotations

import argparse
from pathlib import Path


def add_common_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Add the standard Neo4j + lakehouse arguments that submit injects."""
    parser.add_argument("--neo4j-host", required=True, help="Neo4j host (without protocol)")
    parser.add_argument("--neo4j-username", default="neo4j", help="Neo4j username")
    parser.add_argument("--neo4j-password", required=True, help="Neo4j password")
    parser.add_argument("--neo4j-database", default="neo4j", help="Neo4j database name")
    parser.add_argument("--uc-connection-name", required=True, help="UC JDBC connection name")
    parser.add_argument("--jdbc-jar-path", required=True, help="Path to connector JAR in UC Volume")
    parser.add_argument("--lakehouse-catalog", required=True, help="Lakehouse catalog name")
    parser.add_argument("--lakehouse-schema", default="lakehouse", help="Lakehouse schema name")
    return parser


def add_pg_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Add PostgreSQL comparison arguments.

    Credentials (host, username, password) come from deploy-lakebase/perftest-connection.json
    via cli.py. The JDBC JAR path comes from .env.
    """
    parser.add_argument("--pg-host", default="", help="PostgreSQL host")
    parser.add_argument("--pg-port", default="5432", help="PostgreSQL port")
    parser.add_argument("--pg-database", default="postgres", help="PostgreSQL database")
    parser.add_argument("--pg-username", default="", help="PostgreSQL username")
    parser.add_argument("--pg-password", default="", help="PostgreSQL password")
    parser.add_argument("--pg-jdbc-jar-path", default="", help="Path to PostgreSQL JDBC JAR")
    return parser


def derive_config(args) -> dict:
    """Compute derived values from parsed args. Returns a dict with all config."""
    return {
        "neo4j_host": args.neo4j_host,
        "neo4j_username": args.neo4j_username,
        "neo4j_password": args.neo4j_password,
        "neo4j_database": args.neo4j_database,
        "neo4j_bolt_uri": f"neo4j+s://{args.neo4j_host}",
        "neo4j_jdbc_url": f"jdbc:neo4j+s://{args.neo4j_host}:7687/{args.neo4j_database}",
        "neo4j_jdbc_url_sql": f"jdbc:neo4j+s://{args.neo4j_host}:7687/{args.neo4j_database}?enableSQLTranslation=true",
        "uc_connection_name": args.uc_connection_name,
        "jdbc_jar_path": args.jdbc_jar_path,
        "java_dependencies": f'["{args.jdbc_jar_path}"]',
        "lakehouse_catalog": args.lakehouse_catalog,
        "lakehouse_schema": args.lakehouse_schema,
        "lakehouse_fqn": f"`{args.lakehouse_catalog}`.`{args.lakehouse_schema}`",
    }


def derive_pg_config(args) -> dict | None:
    """Returns pg_cfg dict or None if PG is not configured."""
    if not (args.pg_host and args.pg_username and args.pg_password):
        return None
    return {
        "host": args.pg_host,
        "port": args.pg_port,
        "database": args.pg_database,
        "username": args.pg_username,
        "password": args.pg_password,
        "jdbc_url": f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_database}",
        "jdbc_jar_path": args.pg_jdbc_jar_path,
        "java_dependencies": f'["{args.pg_jdbc_jar_path}"]' if args.pg_jdbc_jar_path else "",
    }


def load_dotenv(path: str | Path) -> dict[str, str]:
    """Parse a .env file into a dict. Does NOT modify os.environ."""
    env = {}
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f".env file not found: {path}")
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            env[key] = value
    return env
