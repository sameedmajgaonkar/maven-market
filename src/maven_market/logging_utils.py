"""Reusable audit logging: every pipeline/job writes the same structured run record.

Rows are appended to the (config-driven) audit table, e.g. ``maven_market_uc.audit.audit_logs``.
The writer is injectable so the logic is unit-testable without Spark; the default writer
appends to a Delta table via the active SparkSession.
"""
from __future__ import annotations

import time
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Iterator

VALID_STATUSES = {"SUCCESS", "FAILED", "WARNING", "STARTED"}
_METRIC_KEYS = ("rows_in", "rows_out", "rows_rejected")

Writer = Callable[[str, dict], None]

# Explicit schema: Spark cannot infer a column type from None (e.g. ``error`` on success rows).
AUDIT_SCHEMA_DDL = (
    "run_id STRING, event_ts TIMESTAMP, stage STRING, source STRING, status STRING, "
    "error STRING, duration_s DOUBLE, rows_in LONG, rows_out LONG, rows_rejected LONG"
)


def _spark_writer(table: str, row: dict) -> None:
    """Default writer: append one row to a Delta table using the active SparkSession."""
    from pyspark.sql import SparkSession  # imported lazily; unavailable in plain unit tests

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    spark.createDataFrame([row], schema=AUDIT_SCHEMA_DDL).write.format("delta").mode("append").saveAsTable(table)


class AuditLogger:
    def __init__(self, table: str, writer: Writer | None = None, run_id: str | None = None):
        self.table = table
        self._writer = writer or _spark_writer
        self.run_id = run_id or uuid.uuid4().hex

    def log_event(
        self,
        stage: str,
        source: str,
        metrics: dict | None,
        status: str,
        error: str | None = None,
        duration_s: float | None = None,
    ) -> dict:
        """Append one audit row and return it."""
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}'; expected one of {sorted(VALID_STATUSES)}")
        metrics = metrics or {}
        row = {
            "run_id": self.run_id,
            "event_ts": datetime.now(timezone.utc),
            "stage": stage,
            "source": source,
            "status": status,
            "error": error,
            "duration_s": duration_s,
            **{k: int(metrics.get(k, 0)) for k in _METRIC_KEYS},
        }
        self._writer(self.table, row)
        return row

    @contextmanager
    def track(self, stage: str, source: str) -> Iterator[dict]:
        """Time a block; log SUCCESS on exit or FAILED (with traceback) on exception, then re-raise.

        Usage::

            with logger.track("bronze", "customers") as m:
                m["rows_in"] = ...
        """
        metrics: dict = {}
        start = time.monotonic()
        try:
            yield metrics
        except Exception as exc:
            self.log_event(
                stage, source, metrics, "FAILED",
                error="".join(traceback.format_exception_only(type(exc), exc)).strip(),
                duration_s=time.monotonic() - start,
            )
            raise
        else:
            self.log_event(stage, source, metrics, "SUCCESS", duration_s=time.monotonic() - start)
