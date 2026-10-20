"""Centralized query definitions for benchmark tests."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QueryDef:
    """A benchmark query in all its variants.

    The SQL form is the canonical definition. Cypher is hand-written.
    Escaped variants are derived automatically.
    """
    name: str
    cypher: str
    sql: str
    schema: str
    expected_count: int
    result_column: str = "cnt"

    @property
    def sql_escaped(self) -> str:
        """SQL escaped for a Spark SQL ``query => '...'`` string literal.

        Spark does not treat ``''`` as an escaped quote: adjacent literals are
        concatenated and the quotes are dropped. Backslash escapes are required.
        """
        return self.sql.replace("\\", "\\\\").replace("'", "\\'")


BENCHMARK_QUERIES: dict[str, QueryDef] = {
    "MaintenanceEvent count": QueryDef(
        name="MaintenanceEvent count",
        cypher="MATCH (m:MaintenanceEvent) RETURN COUNT(m) AS cnt",
        sql="SELECT COUNT(*) AS cnt FROM MaintenanceEvent",
        schema="cnt LONG",
        expected_count=300,
    ),
    "Critical events": QueryDef(
        name="Critical events",
        cypher="MATCH (m:MaintenanceEvent {severity: 'CRITICAL'}) RETURN COUNT(m) AS cnt",
        sql="SELECT COUNT(*) AS cnt FROM MaintenanceEvent WHERE severity = 'CRITICAL'",
        schema="cnt LONG",
        expected_count=101,
    ),
    "Flight count": QueryDef(
        name="Flight count",
        cypher="MATCH (f:Flight) RETURN COUNT(f) AS cnt",
        sql="SELECT COUNT(*) AS cnt FROM Flight",
        schema="cnt LONG",
        expected_count=800,
    ),
    "Flight->Airport traversal": QueryDef(
        name="Flight->Airport traversal",
        cypher="MATCH (f:Flight)-[:DEPARTS_FROM]->(a:Airport) RETURN COUNT(*) AS cnt",
        sql="SELECT COUNT(*) AS cnt FROM Flight f NATURAL JOIN DEPARTS_FROM r NATURAL JOIN Airport a",
        schema="cnt LONG",
        expected_count=800,
    ),
    "Aircraft count": QueryDef(
        name="Aircraft count",
        cypher="MATCH (a:Aircraft) RETURN COUNT(a) AS cnt",
        sql="SELECT COUNT(*) AS cnt FROM Aircraft",
        schema="cnt LONG",
        expected_count=20,
    ),
}

# Subset used in test_compound (excludes Aircraft count).
COMPOUND_QUERY_KEYS = [
    "MaintenanceEvent count",
    "Critical events",
    "Flight count",
    "Flight->Airport traversal",
]

# Spark Connector labels tested in test_spark_connector.
CONNECTOR_LABELS = ["MaintenanceEvent", "Flight", "Aircraft"]

# PostgreSQL remote_query test queries (used in both test_pg_builtin and test_pg_jdbc).
PG_RQ_QUERIES = {
    "remote_query SELECT 1": "SELECT 1 AS val",
    "remote_query COUNT(*)": "SELECT COUNT(*) AS cnt FROM information_schema.tables",
}

# Repeats of each PG remote_query to show whether the first call is just a warm-up.
PG_RQ_NUM_RUNS = 5
