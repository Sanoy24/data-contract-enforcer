"""Tests for contracts/ai_extensions.py — AI contract extensions."""

import json
from pathlib import Path

import numpy as np
import pytest

from contracts.ai_extensions import (
    check_embedding_drift,
    check_output_violation_rate,
    validate_prompt_inputs,
)


class TestEmbeddingDrift:
    def test_baseline_set_on_first_run(self, tmp_path):
        baseline_path = str(tmp_path / "baseline.npz")
        texts = [f"sample text {i}" for i in range(10)]
        result = check_embedding_drift(texts, baseline_path=baseline_path)
        assert result["status"] == "BASELINE_SET"
        assert result["drift_score"] == 0.0
        assert Path(baseline_path).exists()

    def test_pass_same_data(self, tmp_path):
        baseline_path = str(tmp_path / "baseline.npz")
        texts = [f"sample text {i}" for i in range(10)]
        # First run: set baseline
        check_embedding_drift(texts, baseline_path=baseline_path)
        # Second run: compare
        result = check_embedding_drift(texts, baseline_path=baseline_path)
        assert result["status"] == "PASS"
        assert result["drift_score"] == pytest.approx(0.0, abs=0.01)

    def test_error_empty_texts(self):
        result = check_embedding_drift([])
        assert result["status"] == "ERROR"

    def test_drift_score_is_float(self, tmp_path):
        baseline_path = str(tmp_path / "baseline.npz")
        texts = [f"text {i}" for i in range(10)]
        check_embedding_drift(texts, baseline_path=baseline_path)
        result = check_embedding_drift(texts, baseline_path=baseline_path)
        assert isinstance(result["drift_score"], float)


class TestPromptInputValidation:
    def test_valid_records(self, sample_extraction_records, tmp_path):
        quarantine = str(tmp_path / "quarantine/")
        result = validate_prompt_inputs(
            sample_extraction_records, quarantine_path=quarantine
        )
        assert result["status"] == "PASS"
        assert result["valid"] == len(sample_extraction_records)
        assert result["quarantined"] == 0

    def test_invalid_doc_id_quarantined(self, tmp_path):
        quarantine = str(tmp_path / "quarantine/")
        bad_records = [
            {"doc_id": "short", "source_path": "file.pdf"},  # doc_id too short
        ]
        result = validate_prompt_inputs(bad_records, quarantine_path=quarantine)
        assert result["quarantined"] >= 1

    def test_missing_source_path(self, tmp_path):
        quarantine = str(tmp_path / "quarantine/")
        bad_records = [
            {"doc_id": "a" * 36},  # missing source_path
        ]
        result = validate_prompt_inputs(bad_records, quarantine_path=quarantine)
        assert result["quarantined"] >= 1


class TestOutputViolationRate:
    def test_pass_valid_verdicts(self, sample_verdict_records):
        result = check_output_violation_rate(sample_verdict_records)
        assert result["status"] == "PASS"
        assert result["violation_rate"] == 0.0
        assert result["total_outputs"] == len(sample_verdict_records)

    def test_warn_invalid_verdicts(self, invalid_verdict_records):
        result = check_output_violation_rate(
            invalid_verdict_records, warn_threshold=0.02
        )
        assert result["schema_violations"] > 0
        rate = result["violation_rate"]
        assert rate > 0

    def test_trend_rising(self, invalid_verdict_records):
        result = check_output_violation_rate(
            invalid_verdict_records, baseline_rate=0.01
        )
        # With 3 bad records out of 3, rate = 100% which is >> 0.01 * 1.5
        assert result["trend"] == "rising"

    def test_empty_records(self):
        result = check_output_violation_rate([])
        assert result["status"] == "ERROR"
