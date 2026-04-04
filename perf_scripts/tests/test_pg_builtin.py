"""PostgreSQL via built-in connector (TYPE postgresql, no SafeSpark)."""

from perf_scripts.connections import create_pg_builtin_connection, pg_remote_query
from perf_scripts.queries import PG_RQ_QUERIES
from perf_scripts.reporting import timed
from perf_scripts.tests import PerfTest, TestContext, register

PG_BUILTIN_CONN = "pg_perf_builtin"
PG_CATALOG = "pg_perf_catalog"

# Foreign catalog queries — need catalog qualification at runtime.
PG_CATALOG_QUERIES = {
    "SELECT 1": "SELECT 1 AS val",
    "COUNT(*) pg_catalog_info": f"SELECT COUNT(*) AS cnt FROM {PG_CATALOG}.information_schema.tables",
}


@register("pg_builtin", groups=["pg"])
class TestPgBuiltin(PerfTest):
    """PostgreSQL via built-in connector — baseline without SafeSpark."""

    def run(self, ctx: TestContext) -> None:
        if not ctx.pg_cfg:
            print("  [SKIP] PostgreSQL not configured")
            return

        # Clean up from prior runs
        ctx.spark.sql(f"DROP CATALOG IF EXISTS {PG_CATALOG}")
        ctx.spark.sql(f"DROP CONNECTION IF EXISTS {PG_BUILTIN_CONN}")

        create_pg_builtin_connection(ctx.spark, ctx.pg_cfg, PG_BUILTIN_CONN)
        ctx.vr.record("PG builtin: connection created", True)

        # Create foreign catalog
        ctx.spark.sql(f"""
            CREATE FOREIGN CATALOG {PG_CATALOG}
            USING CONNECTION {PG_BUILTIN_CONN}
            OPTIONS (database '{ctx.pg_cfg["database"]}')
        """)
        ctx.vr.record("PG builtin: foreign catalog created", True)

        # Timed queries through the foreign catalog
        for label, sql in PG_CATALOG_QUERIES.items():
            with timed() as t:
                ctx.spark.sql(sql).collect()
            ctx.report_data.setdefault("pg_builtin_times", {})[label] = t.elapsed
            ctx.vr.record(f"PG builtin: {label}", True, f"{t.elapsed:.3f}s")

        # Timed remote_query through the builtin connection
        for label, sql in PG_RQ_QUERIES.items():
            with timed() as t:
                pg_remote_query(
                    ctx.spark, PG_BUILTIN_CONN, ctx.pg_cfg, sql,
                    use_database=True,
                ).collect()
            ctx.report_data.setdefault("pg_builtin_times", {})[label] = t.elapsed
            ctx.vr.record(f"PG builtin: {label}", True, f"{t.elapsed:.3f}s")

        # Cleanup
        ctx.spark.sql(f"DROP CATALOG IF EXISTS {PG_CATALOG}")
