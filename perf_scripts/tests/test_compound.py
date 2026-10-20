"""Compound CROSS JOIN vs individual remote_query() calls."""

from perf_scripts.queries import BENCHMARK_QUERIES, COMPOUND_QUERY_KEYS
from perf_scripts.reporting import timed
from perf_scripts.tests import PerfTest, TestContext, register

ALIASES = ["maint", "crit", "flights", "deps"]


@register("compound", groups=["neo4j", "uc"])
class TestCompound(PerfTest):
    """Compares a CROSS JOIN of 4 remote_query() calls vs running each individually."""

    def run(self, ctx: TestContext) -> None:
        conn = ctx.cfg["uc_connection_name"]

        # Individual calls
        individual_total = 0.0
        for key in COMPOUND_QUERY_KEYS:
            qdef = BENCHMARK_QUERIES[key]
            with timed() as t:
                ctx.spark.sql(
                    f"SELECT * FROM remote_query('{conn}', query => '{qdef.sql_escaped}')"
                ).collect()
            individual_total += t.elapsed

        # Compound CROSS JOIN — built dynamically
        parts = []
        for i, key in enumerate(COMPOUND_QUERY_KEYS):
            qdef = BENCHMARK_QUERIES[key]
            parts.append(
                f"remote_query('{conn}', query => '{qdef.sql_escaped}') AS {ALIASES[i]}"
            )
        from_clause = "\n            CROSS JOIN\n                ".join(parts)
        compound_sql = f"""
            SELECT {', '.join(f'{a}.cnt' for a in ALIASES)}
            FROM {from_clause}
        """

        with timed() as t_compound:
            compound_row = ctx.spark.sql(compound_sql).collect()[0]

        ctx.report_data["compound_vs_individual"] = {
            "individual_total": individual_total,
            "compound_time": t_compound.elapsed,
            "diff": t_compound.elapsed - individual_total,
        }

        ratio = t_compound.elapsed / individual_total if individual_total > 0 else 0
        counts = list(compound_row)
        expected = [BENCHMARK_QUERIES[key].expected_count for key in COMPOUND_QUERY_KEYS]
        ctx.vr.record("Compound counts", counts == expected, f"{counts} (expected {expected})")
        ctx.vr.record("Individual total", True, f"{individual_total:.3f}s")
        ctx.vr.record("Compound CROSS JOIN", True, f"{t_compound.elapsed:.3f}s ({ratio:.1f}x)")

    def print_report(self, ctx: TestContext) -> None:
        cvi = ctx.report_data.get("compound_vs_individual", {})
        if not cvi:
            return
        print(f"\n{'=' * 70}")
        print("COMPOUND CROSS JOIN vs INDIVIDUAL remote_query()")
        print(f"{'=' * 70}")
        print(f"  Individual total:  {cvi['individual_total']:.3f}s")
        print(f"  Compound total:    {cvi['compound_time']:.3f}s")
        print(f"  Difference:        {cvi['diff']:+.3f}s", end="")
        if cvi["individual_total"] > 0:
            print(f"  ({cvi['compound_time'] / cvi['individual_total']:.1f}x)")
        else:
            print()
