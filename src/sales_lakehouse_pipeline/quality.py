"""Data-quality checks used between the Bronze, Silver and Gold layers.

Every check returns a CheckResult. `enforce` raises DataQualityError when any
check failed, which makes the Databricks task (and therefore the workflow) fail.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    """Raised when one or more data-quality checks fail."""


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str

    def __str__(self) -> str:
        return f"[{'PASS' if self.passed else 'FAIL'}] {self.name}: {self.detail}"


def check_row_count(df: DataFrame, name: str, min_rows: int = 1) -> CheckResult:
    """Fail if the table has fewer than `min_rows` rows."""
    count = df.count()
    return CheckResult(f"{name}: row count >= {min_rows}", count >= min_rows, f"{count} rows")


def check_no_nulls(df: DataFrame, name: str, columns: Sequence[str]) -> CheckResult:
    """Fail if any of `columns` contains a null."""
    aggregates = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]
    row = df.agg(*aggregates).collect()[0]
    bad = {c: int(row[c] or 0) for c in columns if (row[c] or 0) > 0}
    return CheckResult(
        f"{name}: no nulls in {list(columns)}",
        not bad,
        f"null counts {bad}" if bad else "no nulls found",
    )


def check_unique_key(df: DataFrame, name: str, key_columns: Sequence[str]) -> CheckResult:
    """Fail if any combination of `key_columns` appears more than once."""
    duplicates = df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
    return CheckResult(
        f"{name}: unique key {list(key_columns)}",
        duplicates == 0,
        f"{duplicates} duplicated keys" if duplicates else "no duplicated keys",
    )


def check_row_count_not_below(
    df: DataFrame, name: str, reference_count: int, reference_name: str
) -> CheckResult:
    """Fail if the table has fewer rows than the reference (e.g. Silver vs distinct Bronze keys)."""
    count = df.count()
    return CheckResult(
        f"{name}: rows >= {reference_name}",
        count >= reference_count,
        f"{count} rows vs {reference_count} in {reference_name}",
    )


def enforce(results: Iterable[CheckResult]) -> None:
    """Print every result, then raise DataQualityError if any check failed."""
    results = list(results)
    for result in results:
        print(result)
    failed = [r for r in results if not r.passed]
    if failed:
        raise DataQualityError(
            f"{len(failed)} data-quality check(s) failed: " + "; ".join(r.name for r in failed)
        )
    print(f"All {len(results)} data-quality checks passed.")
