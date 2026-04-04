"""Delta lakehouse query timing — Spark-side baseline."""

from perf_scripts.reporting import timed
from perf_scripts.tests import PerfTest, TestContext, register

DELTA_QUERIES = {
    "sensor_readings count": ("SELECT COUNT(*) AS cnt FROM sensor_readings", "cnt"),
    "aircraft count": ("SELECT COUNT(*) AS cnt FROM aircraft", "cnt"),
    "sensors count": ("SELECT COUNT(*) AS cnt FROM sensors", "cnt"),
    "systems count": ("SELECT COUNT(*) AS cnt FROM systems", "cnt"),
    "sensor avg (full scan)": (
        """SELECT
            ROUND(AVG(CASE WHEN sen.type = 'EGT' THEN r.value END), 1) AS avg_egt,
            ROUND(AVG(CASE WHEN sen.type = 'Vibration' THEN r.value END), 4) AS avg_vibration,
            COUNT(*) AS total_readings
        FROM sensor_readings r
        JOIN sensors sen ON r.sensor_id = sen.`:ID(Sensor)`""",
        None,
    ),
    "sensor health per aircraft": (
        """SELECT
            sys.aircraft_id,
            ROUND(AVG(CASE WHEN sen.type = 'EGT' THEN r.value END), 1) AS avg_egt,
            ROUND(AVG(CASE WHEN sen.type = 'Vibration' THEN r.value END), 4) AS avg_vibration
        FROM sensor_readings r
        JOIN sensors sen ON r.sensor_id = sen.`:ID(Sensor)`
        JOIN systems sys ON sen.system_id = sys.`:ID(System)`
        GROUP BY sys.aircraft_id""",
        None,
    ),
}


@register("delta", groups=["baseline"])
class TestDelta(PerfTest):
    """Baseline for Spark-side cost querying local Delta tables."""

    def run(self, ctx: TestContext) -> None:
        for label, (sql, cnt_col) in DELTA_QUERIES.items():
            with timed() as t:
                result = ctx.spark.sql(sql).collect()
            ctx.report_data.setdefault("delta_times", {})[label] = t.elapsed
            if cnt_col and len(result) == 1:
                detail = f"{result[0][cnt_col]:,} in {t.elapsed:.3f}s"
            else:
                detail = f"{len(result)} rows in {t.elapsed:.3f}s"
            ctx.vr.record(f"Delta: {label}", True, detail)

    def print_report(self, ctx: TestContext) -> None:
        delta = ctx.report_data.get("delta_times", {})
        if not delta:
            return
        print(f"\n{'=' * 70}")
        print("DELTA LAKEHOUSE BASELINES")
        print(f"{'=' * 70}")
        for label, t in delta.items():
            print(f"  {label:30s} {t:7.3f}s")
        print(f"  {'-' * 30} {'-' * 8}")
        print(f"  {'TOTAL':30s} {sum(delta.values()):7.3f}s")
