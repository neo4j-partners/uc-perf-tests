"""On-cluster entry point. Discovers, filters, and runs performance tests.

Usage (on Databricks via submit)::

    python runner.py                          # run all tests
    python runner.py --tests warmup,delta
    python runner.py --group neo4j
    python runner.py --group pg
    python runner.py --exclude pg
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure the parent directory is on sys.path so ``from perf_scripts...`` works
# when Databricks runs this file directly via spark_python_task.
# Note: Databricks exec()'s the script without setting __file__, so we fall
# back to inspect to get the filename from the code object.
try:
    _this_file = Path(__file__).resolve()
except NameError:
    import inspect
    _this_file = Path(inspect.getfile(inspect.currentframe())).resolve()
_parent = str(_this_file.parent.parent)
if _parent not in sys.path:
    sys.path.insert(0, _parent)

from perf_scripts.config import add_common_args, add_pg_args, derive_config, derive_pg_config
from perf_scripts.reporting import ValidationResults
from perf_scripts.tests import REGISTRY, TestContext

# Import all test modules so @register decorators fire.
import perf_scripts.tests.test_hello
import perf_scripts.tests.test_connectivity
import perf_scripts.tests.test_direct_neo4j
import perf_scripts.tests.test_direct_jdbc
import perf_scripts.tests.test_remote_query
import perf_scripts.tests.test_delta
import perf_scripts.tests.test_spark_connector
import perf_scripts.tests.test_compound
import perf_scripts.tests.test_explain
import perf_scripts.tests.test_federated
import perf_scripts.tests.test_warmup
import perf_scripts.tests.test_pg_builtin
import perf_scripts.tests.test_pg_jdbc


def parse_args():
    parser = argparse.ArgumentParser(description="UC Performance Test Runner")
    add_common_args(parser)
    add_pg_args(parser)
    parser.add_argument("--tests", default="", help="Comma-separated test names to run")
    parser.add_argument("--group", default="", help="Run only tests in this group")
    parser.add_argument("--exclude", default="", help="Comma-separated groups to exclude")
    return parser.parse_known_args()


def select_tests(tests_arg: str, group_arg: str, exclude_arg: str) -> list:
    """Filter REGISTRY based on --tests, --group, --exclude."""
    exclude_groups = {g.strip() for g in exclude_arg.split(",") if g.strip()} if exclude_arg else set()

    if tests_arg:
        names = [n.strip() for n in tests_arg.split(",")]
        selected = [REGISTRY[n] for n in names if n in REGISTRY]
    elif group_arg:
        selected = [t for t in REGISTRY.values() if group_arg in t.groups]
    else:
        selected = list(REGISTRY.values())

    if exclude_groups:
        selected = [t for t in selected if not (set(t.groups) & exclude_groups)]

    return selected


def print_pipeline_comparison(report_data: dict) -> None:
    """Print the cross-test pipeline comparison table."""
    direct = report_data.get("direct_times", {})
    djdbc = report_data.get("direct_jdbc_times", {})
    rq = report_data.get("rq_times", {})
    if not direct or not rq:
        return

    has_jdbc = bool(djdbc)
    print(f"\n{'=' * 90}")
    print("QUERY PIPELINE COMPARISON: Direct Python -> Direct JDBC (Spark) -> remote_query (UC)")
    print(f"{'=' * 90}")
    if has_jdbc:
        print(f"  {'Query':30s} {'Python':>8s} {'JDBC+Sp':>8s} {'UC r_q':>8s} {'JDBC oh':>8s} {'UC oh':>8s}")
        print(f"  {'-' * 30} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8}")
    else:
        print(f"  {'Query':30s} {'Python':>8s} {'UC r_q':>8s} {'Overhead':>8s} {'Ratio':>6s}")
        print(f"  {'-' * 30} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 6}")

    total_direct = total_jdbc = total_rq = 0.0
    for label in direct:
        d = direct[label]
        j = djdbc.get(label, 0)
        r = rq.get(label, 0)
        total_direct += d
        total_jdbc += j
        total_rq += r
        if has_jdbc and r > 0:
            jdbc_oh = j - d if j > 0 else 0
            uc_oh = r - j if j > 0 else r - d
            print(f"  {label:30s} {d:7.3f}s {j:7.3f}s {r:7.3f}s {jdbc_oh:+7.3f}s {uc_oh:+7.3f}s")
        elif r > 0:
            overhead = r - d
            ratio = r / d if d > 0 else 0
            print(f"  {label:30s} {d:7.3f}s {r:7.3f}s {overhead:+7.3f}s {ratio:5.1f}x")

    if has_jdbc:
        jdbc_total_oh = total_jdbc - total_direct
        uc_total_oh = total_rq - total_jdbc
        print(f"  {'-' * 30} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 8}")
        print(f"  {'TOTAL':30s} {total_direct:7.3f}s {total_jdbc:7.3f}s {total_rq:7.3f}s {jdbc_total_oh:+7.3f}s {uc_total_oh:+7.3f}s")
        print(f"\n  Breakdown of overhead:")
        print(f"    Spark JDBC layer (no UC):   {jdbc_total_oh:+.3f}s")
        print(f"    UC/SafeSpark layer:          {uc_total_oh:+.3f}s")
        print(f"    Total overhead vs direct:    {total_rq - total_direct:+.3f}s")
    else:
        print(f"  {'-' * 30} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 6}")
        avg_ratio = total_rq / total_direct if total_direct > 0 else 0
        print(f"  {'TOTAL':30s} {total_direct:7.3f}s {total_rq:7.3f}s {total_rq - total_direct:+7.3f}s {avg_ratio:5.1f}x")


def print_pg_comparison(report_data: dict) -> None:
    """Print the cross-test PostgreSQL SafeSpark comparison table."""
    pg_builtin = report_data.get("pg_builtin_times", {})
    pg_jdbc = report_data.get("pg_jdbc_safespark_times", {})
    neo4j_rq = report_data.get("rq_times", {})
    if not pg_builtin and not pg_jdbc:
        return

    print(f"\n{'=' * 90}")
    print("POSTGRESQL SAFESPARK COMPARISON")
    print(f"{'=' * 90}")

    if pg_builtin:
        print(f"\n  --- PostgreSQL Built-in Connector (TYPE postgresql, no SafeSpark) ---")
        for label, t in pg_builtin.items():
            print(f"  {label:40s} {t:7.3f}s")
    if pg_jdbc:
        print(f"\n  --- PostgreSQL Custom JDBC (TYPE JDBC, through SafeSpark) ---")
        for label, t in pg_jdbc.items():
            print(f"  {label:40s} {t:7.3f}s")

    if pg_builtin and pg_jdbc:
        rq_labels = [l for l in pg_builtin if l.startswith("remote_query")]
        has_neo4j = bool(neo4j_rq)
        print(f"\n  --- Cross-Driver SafeSpark Comparison (remote_query) ---")
        header = f"  {'Query':40s} {'PG builtin':>10s} {'PG JDBC':>10s}"
        divider = f"  {'-' * 40} {'-' * 10} {'-' * 10}"
        if has_neo4j:
            header += f" {'Neo4j UC':>10s}"
            divider += f" {'-' * 10}"
        print(header)
        print(divider)
        for label in rq_labels:
            b = pg_builtin.get(label, 0)
            j = pg_jdbc.get(label, 0)
            line = f"  {label:40s} {b:9.3f}s {j:9.3f}s"
            if has_neo4j:
                n = list(neo4j_rq.values())[0]
                line += f" {n:9.3f}s"
            print(line)

        pg_jdbc_avg = sum(pg_jdbc.values()) / len(pg_jdbc)
        pg_builtin_rq_avg = sum(pg_builtin[l] for l in rq_labels) / len(rq_labels) if rq_labels else 0
        print(f"\n  Interpretation:")
        if has_neo4j:
            neo4j_avg = sum(neo4j_rq.values()) / len(neo4j_rq)
            if pg_jdbc_avg > 5 and neo4j_avg > 5:
                print(f"    PG JDBC SafeSpark avg:  {pg_jdbc_avg:.1f}s")
                print(f"    Neo4j UC avg:           {neo4j_avg:.1f}s")
                print(f"    -> Both slow: SafeSpark sandbox is the bottleneck, not the driver")
            else:
                print(f"    PG JDBC SafeSpark avg:  {pg_jdbc_avg:.1f}s")
                print(f"    Neo4j UC avg:           {neo4j_avg:.1f}s")
        else:
            print(f"    PG JDBC SafeSpark avg:  {pg_jdbc_avg:.1f}s")
        if pg_builtin_rq_avg < 3 and pg_jdbc_avg > 5:
            print(f"    PG builtin r_q avg:     {pg_builtin_rq_avg:.1f}s")
            print(f"    -> Built-in connector bypasses SafeSpark (or uses a faster path)")


def main():
    args, _ = parse_args()
    cfg = derive_config(args)
    pg_cfg = derive_pg_config(args)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    vr = ValidationResults()
    report_data = {}

    ctx = TestContext(
        spark=spark,
        cfg=cfg,
        pg_cfg=pg_cfg,
        vr=vr,
        report_data=report_data,
    )

    print("=" * 60)
    print("uc-perf-tests: Performance Test Runner")
    print("=" * 60)
    print(f"  Neo4j Host:    {cfg['neo4j_host']}")
    print(f"  Bolt URI:      {cfg['neo4j_bolt_uri']}")
    print(f"  UC Connection: {cfg['uc_connection_name']}")
    print(f"  Lakehouse:     {cfg['lakehouse_fqn']}")
    if pg_cfg:
        print(f"  PostgreSQL:    {pg_cfg['host']}:{pg_cfg['port']}/{pg_cfg['database']}")
    else:
        print(f"  PostgreSQL:    (not configured)")
    print("")

    spark.sql(f"USE CATALOG `{cfg['lakehouse_catalog']}`")
    spark.sql(f"USE SCHEMA `{cfg['lakehouse_schema']}`")

    selected = select_tests(args.tests, args.group, args.exclude)
    print(f"Running {len(selected)} test(s): {', '.join(t.name for t in selected)}")
    print("")

    for test in selected:
        test.safe_run(ctx)

    # Consolidated report
    print("\n")
    print("#" * 70)
    print("#  PERFORMANCE DIAGNOSTICS REPORT")
    print("#" * 70)

    print_pipeline_comparison(report_data)

    for test in selected:
        test.print_report(ctx)

    print_pg_comparison(report_data)

    print(f"\n{'=' * 70}")
    print("INTERPRETATION GUIDE")
    print(f"{'=' * 70}")
    print("  Direct ~= remote_query()  -> overhead is minimal, look at Delta/JOINs")
    print("  remote_query() >> Direct   -> overhead in Spark/SafeSpark/JDBC pipeline")
    print("  Compound >> Individual sum -> Spark adds per-subquery overhead in CROSS JOIN")
    print("  Run 1 >> Runs 2-5          -> one-time sandbox/connection init cost")
    print("  All runs equally slow      -> per-query overhead (translation? connection?)")
    print("  .load() >> .count()        -> schema inference dominates, not data transfer")
    print("  .count() >> .load()        -> data volume dominates, not schema overhead")
    print("  PG JDBC ~= Neo4j UC        -> SafeSpark itself is the bottleneck")
    print("  PG JDBC << Neo4j UC        -> Neo4j JAR size/complexity adds overhead in sandbox")

    all_passed = vr.summary()
    if not all_passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
