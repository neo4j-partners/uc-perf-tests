"""Reporting: timing utilities and PASS/FAIL result collection."""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass
class TimingResult:
    """Mutable container so the context manager can expose elapsed time."""
    elapsed: float = 0.0

    @property
    def ms(self) -> float:
        return self.elapsed * 1000

    def __format__(self, spec: str) -> str:
        return format(self.elapsed, spec)


@contextmanager
def timed():
    """Context manager that measures wall-clock time.

    Usage::

        with timed() as t:
            result = spark.sql(...).collect()
        print(f"Took {t.elapsed:.3f}s")
    """
    result = TimingResult()
    start = time.perf_counter()
    try:
        yield result
    finally:
        result.elapsed = time.perf_counter() - start


def avg_subsequent(runs: list[float]) -> float:
    """Average of every run after the first, or 0 when there is only one run."""
    return sum(runs[1:]) / len(runs[1:]) if len(runs) > 1 else 0


def print_warmup_report(title: str, runs_by_label: dict[str, list[float]]) -> None:
    """Print run-by-run times and first vs subsequent stats for each query."""
    if not runs_by_label:
        return
    print(f"\n{'=' * 70}")
    print(title)
    print(f"{'=' * 70}")
    for label, runs in runs_by_label.items():
        avg_warm = avg_subsequent(runs)
        print(f"\n  {label}")
        for i, t in enumerate(runs):
            marker = " <-- first run" if i == 0 else ""
            print(f"    Run {i + 1}: {t:.3f}s{marker}")
        print(f"    First run:        {runs[0]:.3f}s")
        print(f"    Avg subsequent:   {avg_warm:.3f}s")
        print(f"    Min:              {min(runs):.3f}s")
        print(f"    Max:              {max(runs):.3f}s")
        print(f"    Warm-up penalty:  {runs[0] - avg_warm:+.3f}s")


class ValidationResults:
    """Collects PASS/FAIL results and prints a summary."""

    def __init__(self):
        self.results: list[tuple[str, bool, str]] = []

    def record(self, name: str, passed: bool, detail: str = "") -> None:
        status = "PASS" if passed else "FAIL"
        self.results.append((name, passed, detail))
        msg = f"  [{status}] {name}"
        if detail:
            msg += f" — {detail}"
        print(msg)

    def record_timed(self, name: str, passed: bool, t: TimingResult, detail: str = "") -> None:
        """Convenience: appends timing info to the detail string."""
        timing_str = f"{t.elapsed:.3f}s"
        full_detail = f"{detail}, {timing_str}" if detail else timing_str
        self.record(name, passed, full_detail)

    def summary(self) -> bool:
        passed = sum(1 for _, p, _ in self.results if p)
        total = len(self.results)
        print("")
        print("=" * 60)
        print(f"RESULTS: {passed}/{total} passed")
        print("=" * 60)
        for name, p, detail in self.results:
            status = "PASS" if p else "FAIL"
            line = f"  [{status}] {name}"
            if detail:
                line += f" — {detail}"
            print(line)
        print("")
        if passed == total:
            print("STATUS: ALL PASSED")
        else:
            print(f"STATUS: {total - passed} FAILED")
        return passed == total
