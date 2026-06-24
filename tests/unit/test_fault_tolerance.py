"""Tests unitaires pour la fault-tolerance (retries API + alerting). Sans Spark."""

import json
import types
import pytest

from src.flight_extraction import retry_with_backoff
from src import alerting
from src.job_metrics import JobMetrics


class TestMetricsStatus:
    """W2 : une erreur (ex. échec Silver/Gold) marque le batch `failed`."""

    def test_status_failed_on_error(self):
        m = JobMetrics(batch_id="t")
        m.add_error("silver_gold_error", "boom", phase="silver_gold")
        out = m.finalize()
        assert out["status"] == "failed"
        assert out["num_errors"] == 1

    def test_status_success_without_error(self):
        out = JobMetrics(batch_id="t").finalize()
        assert out["status"] == "success"
        assert out["num_errors"] == 0


# ---------------------------------------------------------------------------
# retry_with_backoff
# ---------------------------------------------------------------------------

class TestRetryWithBackoff:
    def test_succeeds_first_try(self):
        calls = {"n": 0}

        def f():
            calls["n"] += 1
            return "ok"

        assert retry_with_backoff(f, max_retries=3, base_delay=0) == "ok"
        assert calls["n"] == 1

    def test_succeeds_after_retries(self):
        calls = {"n": 0}

        def f():
            calls["n"] += 1
            if calls["n"] < 3:
                raise ValueError("transient")
            return "ok"

        assert retry_with_backoff(f, max_retries=3, base_delay=0) == "ok"
        assert calls["n"] == 3

    def test_raises_after_exhaustion(self):
        calls = {"n": 0}

        def f():
            calls["n"] += 1
            raise RuntimeError("always")

        with pytest.raises(RuntimeError):
            retry_with_backoff(f, max_retries=3, base_delay=0)
        assert calls["n"] == 3

    def test_only_retries_listed_exceptions(self):
        def f():
            raise KeyError("not retried")

        # KeyError n'est pas dans `exceptions` -> remonte immédiatement
        with pytest.raises(KeyError):
            retry_with_backoff(f, max_retries=5, base_delay=0, exceptions=(ValueError,))


# ---------------------------------------------------------------------------
# alerting
# ---------------------------------------------------------------------------

def _config(tmp_path, threshold=70, sla_min=30):
    return types.SimpleNamespace(
        LOG_PATH=str(tmp_path / "_logs"),
        ALERT_THRESHOLD_PCT_VALID=threshold,
        COLLECTION_TIMEOUT_MINUTES=sla_min,
    )


class TestEvaluateAlerts:
    def test_no_alerts_on_healthy_batch(self, tmp_path):
        m = {
            "num_errors": 0,
            "extraction": {"rows": 1000},
            "validation": {"pct_valid": 95.0},
            "total_duration_seconds": 60,
        }
        assert alerting.evaluate_alerts(m, _config(tmp_path)) == []

    def test_errors_trigger_critical(self, tmp_path):
        m = {"num_errors": 2, "extraction": {"rows": 100},
             "validation": {"pct_valid": 99.0}, "total_duration_seconds": 10}
        alerts = alerting.evaluate_alerts(m, _config(tmp_path))
        assert any(a["severity"] == alerting.CRITICAL and a["type"] == "pipeline_errors" for a in alerts)

    def test_low_quality_warning(self, tmp_path):
        m = {"num_errors": 0, "extraction": {"rows": 100},
             "validation": {"pct_valid": 50.0}, "total_duration_seconds": 10}
        alerts = alerting.evaluate_alerts(m, _config(tmp_path, threshold=70))
        assert any(a["type"] == "low_data_quality" for a in alerts)

    def test_low_quality_ignored_when_no_flights(self, tmp_path):
        m = {"num_errors": 0, "extraction": {"rows": 0},
             "validation": {"pct_valid": 0.0}, "total_duration_seconds": 10}
        types_ = [a["type"] for a in alerting.evaluate_alerts(m, _config(tmp_path))]
        assert "low_data_quality" not in types_
        assert "empty_extraction" in types_

    def test_sla_breach_warning(self, tmp_path):
        m = {"num_errors": 0, "extraction": {"rows": 100},
             "validation": {"pct_valid": 99.0}, "total_duration_seconds": 9999}
        alerts = alerting.evaluate_alerts(m, _config(tmp_path, sla_min=1))
        assert any(a["type"] == "sla_breach" for a in alerts)


class TestDispatchAlerts:
    def test_writes_alert_file(self, tmp_path):
        cfg = _config(tmp_path)
        alerts = [{"severity": alerting.WARNING, "type": "x", "message": "y"}]
        path = alerting.dispatch_alerts(alerts, "BATCH1", cfg)
        assert path is not None
        data = json.loads(open(path).read())
        assert data["batch_id"] == "BATCH1"
        assert data["alert_count"] == 1

    def test_no_file_when_no_alerts(self, tmp_path):
        cfg = _config(tmp_path)
        assert alerting.dispatch_alerts([], "BATCH2", cfg) is None
