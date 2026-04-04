"""Federated query breakdown — step-by-step decomposition."""

from perf_scripts.connections import read_spark_connector
from perf_scripts.reporting import timed
from perf_scripts.tests import PerfTest, TestContext, register


@register("federated", groups=["neo4j", "uc"])
class TestFederated(PerfTest):
    """Breaks the federated sensor+maintenance correlation into timed steps."""

    def run(self, ctx: TestContext) -> None:
        timings = {}

        # Step 1: Load maintenance events from Neo4j via Spark Connector
        with timed() as t:
            neo4j_maint = read_spark_connector(ctx.spark, ctx.cfg, labels="MaintenanceEvent")
        timings["1. Spark Connector .load()"] = t.elapsed

        # Step 2: Create temp view + materialize count
        with timed() as t:
            neo4j_maint.createOrReplaceTempView("neo4j_maintenance_perf")
            maint_count = neo4j_maint.count()
        timings["2. Temp view + count"] = t.elapsed

        # Step 3: Delta-only sensor health aggregation
        with timed() as t:
            ctx.spark.sql("""
                SELECT
                    sys.aircraft_id,
                    ROUND(AVG(CASE WHEN sen.type = 'EGT' THEN r.value END), 1) AS avg_egt,
                    ROUND(AVG(CASE WHEN sen.type = 'Vibration' THEN r.value END), 4) AS avg_vibration
                FROM sensor_readings r
                JOIN sensors sen ON r.sensor_id = sen.`:ID(Sensor)`
                JOIN systems sys ON sen.system_id = sys.`:ID(System)`
                GROUP BY sys.aircraft_id
            """).createOrReplaceTempView("sensor_health_perf")
            ctx.spark.sql("SELECT COUNT(*) FROM sensor_health_perf").collect()
        timings["3. Delta sensor aggregation"] = t.elapsed

        # Step 4: Neo4j maintenance aggregation (from temp view)
        with timed() as t:
            ctx.spark.sql("""
                SELECT
                    aircraft_id,
                    COUNT(*) AS total_events,
                    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical
                FROM neo4j_maintenance_perf
                GROUP BY aircraft_id
            """).createOrReplaceTempView("maint_summary_perf")
            ctx.spark.sql("SELECT COUNT(*) FROM maint_summary_perf").collect()
        timings["4. Maintenance aggregation"] = t.elapsed

        # Step 5: Final JOIN
        with timed() as t:
            result = ctx.spark.sql("""
                SELECT
                    a.tail_number, a.model,
                    COALESCE(m.total_events, 0) AS maint_events,
                    COALESCE(m.critical, 0) AS critical,
                    s.avg_egt, s.avg_vibration
                FROM (SELECT `:ID(Aircraft)` AS aircraft_id, tail_number, model FROM aircraft) a
                LEFT JOIN maint_summary_perf m ON a.aircraft_id = m.aircraft_id
                LEFT JOIN sensor_health_perf s ON a.aircraft_id = s.aircraft_id
                ORDER BY maint_events DESC
            """).collect()
        timings["5. Final JOIN + collect"] = t.elapsed

        ctx.report_data["federated_breakdown"] = {
            "timings": timings,
            "maint_count": maint_count,
            "result_rows": len(result),
        }

        total = sum(timings.values())
        for step, elapsed in timings.items():
            pct = (elapsed / total) * 100 if total > 0 else 0
            ctx.vr.record(f"Federated {step}", True, f"{elapsed:.3f}s ({pct:.0f}%)")

    def print_report(self, ctx: TestContext) -> None:
        fb = ctx.report_data.get("federated_breakdown", {})
        if not fb:
            return
        timings = fb["timings"]
        total = sum(timings.values())
        print(f"\n{'=' * 70}")
        print("FEDERATED QUERY BREAKDOWN (Sensor + Maintenance Correlation)")
        print(f"{'=' * 70}")
        print(f"  {'Step':35s} {'Time':>8s} {'% Total':>8s}  Bar")
        print(f"  {'-' * 35} {'-' * 8} {'-' * 8}  {'-' * 20}")
        for step, elapsed in timings.items():
            pct = (elapsed / total) * 100 if total > 0 else 0
            bar = "#" * int(pct / 2)
            print(f"  {step:35s} {elapsed:7.3f}s {pct:7.1f}%  {bar}")
        print(f"  {'-' * 35} {'-' * 8}")
        print(f"  {'TOTAL':35s} {total:7.3f}s")
        print(f"  Maintenance events: {fb['maint_count']:,}  |  Result rows: {fb['result_rows']}")
