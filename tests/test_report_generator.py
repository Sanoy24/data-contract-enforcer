"""Tests for contracts/report_generator.py — report construction."""

import pytest

from contracts.report_generator import (
    build_recommendations,
    build_violation_section,
    compute_health_score,
)


class TestComputeHealthScore:
    def test_all_pass(self):
        reports = [{"total_checks": 10, "passed": 10, "results": [
            {"status": "PASS", "severity": "LOW"} for _ in range(10)
        ]}]
        score, narrative = compute_health_score(reports)
        assert score == 100

    def test_one_critical(self):
        reports = [{"total_checks": 10, "passed": 9, "results": [
            {"status": "PASS", "severity": "LOW"} for _ in range(9)
        ] + [{"status": "FAIL", "severity": "CRITICAL"}]}]
        score, _ = compute_health_score(reports)
        # (9/10)*100 - 20 = 70
        assert score == 70

    def test_many_criticals_clamped_at_zero(self):
        reports = [{"total_checks": 10, "passed": 4, "results": [
            {"status": "PASS", "severity": "LOW"} for _ in range(4)
        ] + [{"status": "FAIL", "severity": "CRITICAL"} for _ in range(6)]}]
        score, _ = compute_health_score(reports)
        # (4/10)*100 - (20*6) = 40 - 120 = -80 -> clamped to 0
        assert score == 0

    def test_empty_reports(self):
        score, _ = compute_health_score([])
        assert score == 100  # no data = no violations

    def test_narrative_contains_score(self):
        reports = [{"total_checks": 5, "passed": 5, "results": [
            {"status": "PASS", "severity": "LOW"} for _ in range(5)
        ]}]
        _, narrative = compute_health_score(reports)
        assert "100" in narrative


class TestBuildViolationSection:
    def test_severity_counts(self):
        violations = [
            {"severity": "CRITICAL", "contract_id": "c1", "column_name": "col", "message": "bad", "records_failing": 10},
            {"severity": "HIGH", "contract_id": "c1", "column_name": "col2", "message": "drift", "records_failing": 0},
            {"severity": "CRITICAL", "contract_id": "c2", "column_name": "col3", "message": "fail", "records_failing": 5},
        ]
        registry = {"subscriptions": []}
        section = build_violation_section(violations, registry)
        assert section["by_severity"]["CRITICAL"] == 2
        assert section["by_severity"]["HIGH"] == 1
        assert section["total_violations"] == 3

    def test_top_violations_limited_to_three(self):
        violations = [
            {"severity": "LOW", "contract_id": f"c{i}", "column_name": f"col{i}", "message": f"m{i}", "records_failing": 0}
            for i in range(10)
        ]
        registry = {"subscriptions": []}
        section = build_violation_section(violations, registry)
        assert len(section["top_violations"]) <= 3


class TestBuildRecommendations:
    def test_returns_three(self):
        violations = [
            {"severity": "CRITICAL", "contract_id": "week3-x", "column_name": "conf", "check_id": "conf.range"},
            {"severity": "HIGH", "contract_id": "week3-x", "column_name": "conf", "check_id": "conf.drift"},
        ]
        reports = [{"total_checks": 10, "passed": 8}]
        registry = {"subscriptions": []}
        recs = build_recommendations(violations, reports, registry)
        assert len(recs) == 3

    def test_critical_action_names_file(self):
        violations = [
            {"severity": "CRITICAL", "contract_id": "week3-test", "column_name": "confidence", "check_id": "confidence.range"},
        ]
        recs = build_recommendations(violations, [], {"subscriptions": []})
        assert any("contracts/runner.py" in r or "week3" in r for r in recs)
