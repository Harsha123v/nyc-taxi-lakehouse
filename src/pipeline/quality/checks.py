"""Reusable data quality checks. Each takes DataFrames and returns a CheckResult.

Kept free of I/O so the same functions run in the pipeline (dq_runner.py)
and in unit tests.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str
    failing_rows: int = 0
    extra: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return asdict(self)


def check_not_null(df: DataFrame, columns: list[str], name: str) -> CheckResult:
    condition = None
    for c in columns:
        cond = F.col(c).isNull()
        condition = cond if condition is None else condition | cond
    bad = df.filter(condition).count()
    return CheckResult(name, bad == 0, f"rows with NULL in any of {columns}: {bad}", bad)


def check_unique(df: DataFrame, key: str, name: str) -> CheckResult:
    dupes = df.groupBy(key).count().filter(F.col("count") > 1)
    n_keys = dupes.count()
    extra_rows = dupes.agg(F.sum(F.col("count") - 1)).first()[0] or 0
    return CheckResult(name, n_keys == 0, f"duplicate {key} values: {n_keys} (extra rows: {extra_rows})", int(extra_rows))


def check_condition(df: DataFrame, valid_condition, description: str, name: str) -> CheckResult:
    """Every row must satisfy `valid_condition` (a Column). NULL counts as failing."""
    bad = df.filter(~F.coalesce(valid_condition, F.lit(False))).count()
    return CheckResult(name, bad == 0, f"rows violating '{description}': {bad}", bad)


def check_date_in_range(df: DataFrame, column: str, start: date, end_exclusive: date, name: str) -> CheckResult:
    return check_condition(
        df,
        (F.col(column) >= F.lit(start)) & (F.col(column) < F.lit(end_exclusive)),
        f"{start} <= {column} < {end_exclusive}",
        name,
    )


def check_allowed_values(df: DataFrame, column: str, allowed: tuple[str, ...], name: str) -> CheckResult:
    return check_condition(df, F.col(column).isin(list(allowed)), f"{column} in {list(allowed)}", name)


def check_reconciliation(raw_rows: int, clean_rows: int, rejected_rows: int, name: str) -> CheckResult:
    diff = raw_rows - (clean_rows + rejected_rows)
    return CheckResult(
        name, diff == 0,
        f"raw {raw_rows:,} = clean {clean_rows:,} + rejected {rejected_rows:,} (difference {diff:,})",
        abs(diff),
        {"raw": raw_rows, "clean": clean_rows, "rejected": rejected_rows},
    )


def check_metrics_match_trips(trips: DataFrame, metrics: DataFrame, name: str, tolerance: float = 0.01) -> CheckResult:
    """daily_zone_metrics must add back up to trips_clean (counts exact, revenue within rounding)."""
    t = trips.agg(F.count(F.lit(1)).alias("n"), F.sum("total_amount").alias("rev")).first()
    m = metrics.agg(F.sum("trips").alias("n"), F.sum("total_revenue").alias("rev")).first()
    t_n, m_n = t.n or 0, m.n or 0
    t_rev, m_rev = float(t.rev or 0.0), float(m.rev or 0.0)
    # Each metric row's revenue is rounded to cents, so allow 1 cent per metric row.
    allowed = max(tolerance, 0.01 * metrics.count())
    ok = t_n == m_n and abs(t_rev - m_rev) <= allowed
    return CheckResult(
        name, ok,
        f"trips {t_n:,} vs metrics {m_n:,}; revenue {t_rev:,.2f} vs {m_rev:,.2f}",
        abs(t_n - m_n),
    )
