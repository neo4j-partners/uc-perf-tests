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
