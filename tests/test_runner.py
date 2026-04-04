"""Tests for contracts/runner.py — ValidationRunner checks."""

import pandas as pd
import pytest

from contracts.runner import (
    apply_enforcement_mode,
    check_datetime_format,
    check_enum,
    check_range,
    check_required,
    check_sha256_pattern,
    check_statistical_drift,
    check_type,
    check_uuid_pattern,
)


# ---------------------------------------------------------------------------
# check_required
# ---------------------------------------------------------------------------

class TestCheckRequired:
    def test_pass_no_nulls(self):
        df = pd.DataFrame({"col": ["a", "b", "c"]})
        result = check_required(df, "col", {"required": True})
        assert result["status"] == "PASS"

    def test_fail_with_nulls(self):
        df = pd.DataFrame({"col": ["a", None, "c"]})
        result = check_required(df, "col", {"required": True})
        assert result["status"] == "FAIL"
        assert result["severity"] == "CRITICAL"

    def test_error_missing_column(self):
        df = pd.DataFrame({"other": [1, 2]})
        result = check_required(df, "missing_col", {"required": True})
        assert result["status"] == "ERROR"

    def test_skip_not_required(self):
        df = pd.DataFrame({"col": [None, None]})
        result = check_required(df, "col", {"required": False})
        assert result is None or result["status"] == "PASS"


# ---------------------------------------------------------------------------
# check_type
# ---------------------------------------------------------------------------

class TestCheckType:
    def test_string_match(self):
        df = pd.DataFrame({"col": ["a", "b"]})
        result = check_type(df, "col", {"type": "string"})
        assert result["status"] == "PASS"

    def test_number_match(self):
        df = pd.DataFrame({"col": [1.0, 2.5]})
        result = check_type(df, "col", {"type": "number"})
        assert result["status"] == "PASS"

    def test_integer_match(self):
        df = pd.DataFrame({"col": [1, 2, 3]})
        result = check_type(df, "col", {"type": "integer"})
        assert result["status"] == "PASS"


# ---------------------------------------------------------------------------
# check_range
# ---------------------------------------------------------------------------

class TestCheckRange:
    def test_pass_within_bounds(self):
        df = pd.DataFrame({"confidence": [0.5, 0.8, 0.9]})
        result = check_range(df, "confidence", {"minimum": 0.0, "maximum": 1.0})
        assert result["status"] == "PASS"

    def test_fail_scale_change(self):
        df = pd.DataFrame({"confidence": [55.0, 78.0, 98.0]})
        result = check_range(df, "confidence", {"minimum": 0.0, "maximum": 1.0})
        assert result["status"] == "FAIL"
        assert result["severity"] == "CRITICAL"

    def test_skip_no_range(self):
        df = pd.DataFrame({"col": [1, 2, 3]})
        result = check_range(df, "col", {"type": "integer"})
        assert result is None


# ---------------------------------------------------------------------------
# check_enum
# ---------------------------------------------------------------------------

class TestCheckEnum:
    def test_pass_all_valid(self):
        df = pd.DataFrame({"type": ["PERSON", "ORG", "LOCATION"]})
        result = check_enum(df, "type", {"enum": ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]})
        assert result["status"] == "PASS"

    def test_fail_invalid_value(self):
        df = pd.DataFrame({"type": ["PERSON", "CONCEPT", "ORG"]})
        result = check_enum(df, "type", {"enum": ["PERSON", "ORG", "LOCATION"]})
        assert result["status"] == "FAIL"


# ---------------------------------------------------------------------------
# check_uuid_pattern
# ---------------------------------------------------------------------------

class TestCheckUuid:
    def test_valid_uuids(self):
        import uuid
        df = pd.DataFrame({"id": [str(uuid.uuid4()) for _ in range(5)]})
        result = check_uuid_pattern(df, "id", {"format": "uuid"})
        assert result["status"] == "PASS"


# ---------------------------------------------------------------------------
# check_datetime_format
# ---------------------------------------------------------------------------

class TestCheckDatetime:
    def test_valid_iso8601(self):
        df = pd.DataFrame({"ts": ["2025-01-15T14:23:00Z", "2025-01-16T10:00:00+00:00"]})
        result = check_datetime_format(df, "ts", {"format": "date-time"})
        assert result["status"] == "PASS"


# ---------------------------------------------------------------------------
# check_sha256_pattern
# ---------------------------------------------------------------------------

class TestCheckSha256:
    def test_valid_hashes(self):
        df = pd.DataFrame({"hash": ["a" * 64, "b" * 64]})
        result = check_sha256_pattern(df, "hash", {"pattern": "^[a-f0-9]{64}$"})
        assert result is not None
        assert result["status"] == "PASS"


# ---------------------------------------------------------------------------
# check_statistical_drift
# ---------------------------------------------------------------------------

class TestCheckStatisticalDrift:
    def test_pass_within_threshold(self, sample_baselines):
        cols = sample_baselines["columns"]
        result = check_statistical_drift("fact_confidence", 0.78, 0.13, cols)
        assert result is not None
        assert result["status"] == "PASS"

    def test_warn_approaching(self, sample_baselines):
        cols = sample_baselines["columns"]
        # mean + 2.5 stddev = 0.77 + 0.3 = 1.07
        result = check_statistical_drift("fact_confidence", 1.07, 0.12, cols)
        assert result is not None
        assert result["status"] == "WARN"

    def test_fail_major_drift(self, sample_baselines):
        cols = sample_baselines["columns"]
        # 0-100 scale: mean=77 is 624 stddev from baseline 0.77
        result = check_statistical_drift("fact_confidence", 77.0, 12.0, cols)
        assert result is not None
        assert result["status"] == "FAIL"

    def test_no_baseline(self):
        result = check_statistical_drift("unknown_col", 5.0, 1.0, {})
        assert result is None


# ---------------------------------------------------------------------------
# apply_enforcement_mode
# ---------------------------------------------------------------------------

class TestEnforcementMode:
    def _make_report(self, fails):
        results = []
        for check_id, severity in fails:
            results.append({"check_id": check_id, "status": "FAIL", "severity": severity})
        results.append({"check_id": "ok.check", "status": "PASS", "severity": "LOW"})
        return {
            "total_checks": len(results),
            "passed": 1,
            "failed": len(fails),
            "warned": 0,
            "errored": 0,
            "results": results,
        }

    def test_audit_never_blocks(self):
        report = self._make_report([("conf.range", "CRITICAL")])
        result = apply_enforcement_mode(report, "AUDIT")
        assert result["enforcement_action"] == "LOGGED"
        assert result["blocking_violations"] == []

    def test_warn_blocks_critical(self):
        report = self._make_report([("conf.range", "CRITICAL"), ("drift", "HIGH")])
        result = apply_enforcement_mode(report, "WARN")
        assert result["enforcement_action"] == "BLOCKED"
        assert "conf.range" in result["blocking_violations"]
        assert "drift" not in result["blocking_violations"]

    def test_enforce_blocks_critical_and_high(self):
        report = self._make_report([("conf.range", "CRITICAL"), ("drift", "HIGH")])
        result = apply_enforcement_mode(report, "ENFORCE")
        assert result["enforcement_action"] == "BLOCKED"
        assert len(result["blocking_violations"]) == 2

    def test_enforce_passes_when_clean(self):
        report = self._make_report([])
        result = apply_enforcement_mode(report, "ENFORCE")
        assert result["enforcement_action"] == "PASSED"
