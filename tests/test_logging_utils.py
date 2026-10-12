"""Tests for the audit logging framework (maven_market.logging_utils)."""
import pytest

from maven_market.logging_utils import AuditLogger


class Sink:
    def __init__(self):
        self.rows = []

    def __call__(self, table, row):
        self.rows.append((table, row))


def _logger(sink):
    return AuditLogger(table="maven_market_uc.audit.audit_logs", writer=sink)


def test_log_event_row_shape_and_timestamps():
    sink = Sink()
    _logger(sink).log_event(
        "bronze", "customers", {"rows_in": 10, "rows_out": 9, "rows_rejected": 1}, "SUCCESS"
    )
    table, row = sink.rows[0]
    assert table == "maven_market_uc.audit.audit_logs"
    assert row["stage"] == "bronze" and row["source"] == "customers"
    assert row["status"] == "SUCCESS" and row["error"] is None
    assert (row["rows_in"], row["rows_out"], row["rows_rejected"]) == (10, 9, 1)
    assert row["event_ts"] is not None and row["run_id"]


def test_missing_metrics_default_to_zero():
    sink = Sink()
    _logger(sink).log_event("silver", "dim_customer", {}, "SUCCESS")
    row = sink.rows[0][1]
    assert (row["rows_in"], row["rows_out"], row["rows_rejected"]) == (0, 0, 0)


def test_invalid_status_rejected():
    with pytest.raises(ValueError):
        _logger(Sink()).log_event("bronze", "x", {}, "WEIRD")


def test_track_logs_success_with_duration():
    sink = Sink()
    with _logger(sink).track("bronze", "stores") as metrics:
        metrics["rows_in"] = 5
        metrics["rows_out"] = 5
    row = sink.rows[0][1]
    assert row["status"] == "SUCCESS" and row["rows_out"] == 5
    assert row["duration_s"] >= 0


def test_track_failure_records_error_and_reraises():
    sink = Sink()
    with pytest.raises(RuntimeError):
        with _logger(sink).track("bronze", "stores"):
            raise RuntimeError("boom")
    row = sink.rows[0][1]
    assert row["status"] == "FAILED"
    assert "boom" in row["error"]
