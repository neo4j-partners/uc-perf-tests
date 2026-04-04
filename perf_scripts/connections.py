"""Connection helpers for Neo4j, JDBC, Spark Connector, and PostgreSQL."""

from __future__ import annotations


def get_neo4j_driver(cfg: dict):
    """Create and return a Neo4j driver from config dict."""
    from neo4j import GraphDatabase
    return GraphDatabase.driver(
        cfg["neo4j_bolt_uri"],
        auth=(cfg["neo4j_username"], cfg["neo4j_password"]),
    )


def read_direct_jdbc(spark, cfg: dict, query: str, custom_schema: str):
    """Read from Neo4j via direct JDBC (no UC, no SafeSpark)."""
    return (
        spark.read.format("jdbc")
        .option("url", cfg["neo4j_jdbc_url_sql"])
        .option("driver", "org.neo4j.jdbc.Neo4jDriver")
        .option("user", cfg["neo4j_username"])
        .option("password", cfg["neo4j_password"])
        .option("query", query)
        .option("customSchema", custom_schema)
        .load()
    )



def read_neo4j_jdbc(spark, cfg: dict, custom_schema: str, *, query: str = None, dbtable: str = None):
    """Read from Neo4j through the UC JDBC connection (DataFrame API)."""
    reader = (
        spark.read.format("jdbc")
        .option("databricks.connection", cfg["uc_connection_name"])
        .option("customSchema", custom_schema)
    )
    if query:
        reader = reader.option("query", query)
    else:
        reader = reader.option("dbtable", dbtable)
    return reader.load()


def remote_query(spark, cfg: dict, query: str):
    """Execute a query via remote_query() SQL function."""
    return spark.sql(f"""
        SELECT * FROM remote_query('{cfg["uc_connection_name"]}',
            query => '{query}')
    """)


def read_spark_connector(spark, cfg: dict, *, query: str = None, labels: str = None):
    """Read via Neo4j Spark Connector."""
    reader = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", cfg["neo4j_bolt_uri"])
        .option("authentication.type", "basic")
        .option("authentication.basic.username", cfg["neo4j_username"])
        .option("authentication.basic.password", cfg["neo4j_password"])
    )
    if query:
        reader = reader.option("query", query)
    if labels:
        reader = reader.option("labels", labels)
    return reader.load()


def create_uc_connection(spark, cfg: dict, name: str = None) -> str:
    """Create (or recreate) a UC JDBC connection. Returns the connection name."""
    conn_name = name or cfg["uc_connection_name"]
    spark.sql(f"DROP CONNECTION IF EXISTS {conn_name}")
    spark.sql(f"""
        CREATE CONNECTION {conn_name} TYPE JDBC
        ENVIRONMENT (
          java_dependencies '{cfg["java_dependencies"]}'
        )
        OPTIONS (
          url '{cfg["neo4j_jdbc_url_sql"]}',
          user '{cfg["neo4j_username"]}',
          password '{cfg["neo4j_password"]}',
          driver 'org.neo4j.jdbc.Neo4jDriver',
          externalOptionsAllowList 'dbtable,query,partitionColumn,lowerBound,upperBound,numPartitions,fetchSize,customSchema'
        )
    """)
    return conn_name


def create_pg_builtin_connection(spark, pg_cfg: dict, conn_name: str) -> str:
    """Create PostgreSQL TYPE postgresql connection (no SafeSpark)."""
    spark.sql(f"DROP CONNECTION IF EXISTS {conn_name}")
    spark.sql(f"""
        CREATE CONNECTION {conn_name} TYPE postgresql
        OPTIONS (
            host '{pg_cfg["host"]}',
            port '{pg_cfg["port"]}',
            user '{pg_cfg["username"]}',
            password '{pg_cfg["password"]}'
        )
    """)
    return conn_name


def create_pg_jdbc_connection(spark, pg_cfg: dict, conn_name: str) -> str:
    """Create PostgreSQL TYPE JDBC connection (through SafeSpark)."""
    spark.sql(f"DROP CONNECTION IF EXISTS {conn_name}")
    spark.sql(f"""
        CREATE CONNECTION {conn_name} TYPE JDBC
        ENVIRONMENT (
            java_dependencies '{pg_cfg["java_dependencies"]}'
        )
        OPTIONS (
            url '{pg_cfg["jdbc_url"]}',
            user '{pg_cfg["username"]}',
            password '{pg_cfg["password"]}',
            driver 'org.postgresql.Driver'
        )
    """)
    return conn_name


def pg_remote_query(spark, conn_name: str, pg_cfg: dict, query: str, *, use_database: bool = False):
    """Execute remote_query() against a PostgreSQL connection."""
    if use_database:
        return spark.sql(f"""
            SELECT * FROM remote_query('{conn_name}',
                database => '{pg_cfg["database"]}',
                query => '{query}')
        """)
    return spark.sql(f"""
        SELECT * FROM remote_query('{conn_name}', query => '{query}')
    """)
