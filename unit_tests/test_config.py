"""Tests for derived benchmark configuration."""

from types import SimpleNamespace

from perf_scripts.config import NEO4J_JDBC_TIMEOUT_MS, derive_config


def _args() -> SimpleNamespace:
    return SimpleNamespace(
        neo4j_uri="neo4j+s://example.databases.neo4j.io",
        neo4j_username="neo4j",
        neo4j_password="secret",
        neo4j_database="neo4j",
        uc_connection_name="catalog.schema.neo4j_connection",
        jdbc_jar_path="/Volumes/catalog/schema/jars/neo4j-jdbc.jar",
        lakehouse_catalog="catalog",
        lakehouse_schema="schema",
    )


def test_derived_neo4j_jdbc_urls_use_30_second_timeout() -> None:
    config = derive_config(_args())

    assert NEO4J_JDBC_TIMEOUT_MS == 30_000
    assert config["neo4j_jdbc_url"] == (
        "jdbc:neo4j+s://example.databases.neo4j.io:7687/neo4j?timeout=30000"
    )
    assert config["neo4j_jdbc_url_sql"] == (
        "jdbc:neo4j+s://example.databases.neo4j.io:7687/neo4j"
        "?timeout=30000&enableSQLTranslation=true"
    )


def test_sql_translation_url_has_one_query_separator() -> None:
    sql_url = derive_config(_args())["neo4j_jdbc_url_sql"]

    assert sql_url.count("?") == 1
    assert "&enableSQLTranslation=true" in sql_url
