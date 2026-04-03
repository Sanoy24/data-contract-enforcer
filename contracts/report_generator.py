"""
ReportGenerator — Auto-generates the Enforcer Report from live validation data.

Usage:
    uv run python contracts/report_generator.py \
        --output enforcer_report/report_data.json
"""

import argparse
import glob
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yaml


# ---------------------------------------------------------------------------
# Data loaders (read from directories, not hardcoded strings)
# ---------------------------------------------------------------------------


def load_all_reports(reports_dir: str = "validation_reports/") -> list[dict]:
    """Load all validation report JSON files from the reports directory."""
    reports = []
    for p in glob.glob(str(Path(reports_dir) / "*.json")):
        # Skip AI extensions and schema evolution reports — they have different schemas
        basename = Path(p).name
        if basename.startswith("ai_extensions") or basename.startswith("schema_evolution"):
            continue
        try:
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
            # Only include files that look like validation reports
            if "results" in data and "total_checks" in data:
                reports.append(data)
        except (json.JSONDecodeError, KeyError):
            continue
    return reports


def load_violations(violation_log_path: str = "violation_log/violations.jsonl") -> list[dict]:
    """Load violation entries from the violation log JSONL."""
    violations = []
    p = Path(violation_log_path)
    if not p.exists():
        return violations
    with open(p, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    violations.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return violations


def load_schema_evolution(reports_dir: str = "validation_reports/") -> list[dict]:
    """Load schema evolution reports if they exist."""
    reports = []
    for p in glob.glob(str(Path(reports_dir) / "schema_evolution*.json")):
        try:
            with open(p, encoding="utf-8") as f:
                reports.append(json.load(f))
        except (json.JSONDecodeError, KeyError):
            continue
    return reports


def load_ai_report(ai_report_path: str = "validation_reports/ai_extensions.json") -> dict:
    """Load the AI extensions report."""
    p = Path(ai_report_path)
    if not p.exists():
        return {}
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def load_registry(registry_path: str = "contract_registry/subscriptions.yaml") -> dict:
    """Load the contract registry for subscriber lookups."""
    p = Path(registry_path)
    if not p.exists():
        return {"subscriptions": []}
    with open(p, encoding="utf-8") as f:
        return yaml.safe_load(f) or {"subscriptions": []}


# ---------------------------------------------------------------------------
# Section 1: Data Health Score
# ---------------------------------------------------------------------------


def compute_health_score(reports: list[dict]) -> tuple[int, str]:
    """Compute health score: (passed/total)*100 - 20*critical_count.

    Clamped to [0, 100].
    """
    total_checks = 0
    passed = 0
    critical_fails = 0

    for r in reports:
        total_checks += r.get("total_checks", 0)
        passed += r.get("passed", 0)
        for result in r.get("results", []):
            if result.get("status") == "FAIL" and result.get("severity") == "CRITICAL":
                critical_fails += 1

    if total_checks == 0:
        score = 100
    else:
        score = int((passed / total_checks) * 100) - (20 * critical_fails)

    score = max(0, min(100, score))

    if score >= 90:
        narrative = f"Score {score}/100. All systems operating within contract parameters."
    elif score >= 70:
        narrative = (
            f"Score {score}/100. Minor issues detected. "
            f"{critical_fails} critical violation(s) require attention."
        )
    else:
        narrative = (
            f"Score {score}/100. {critical_fails} critical issue(s) require immediate action. "
            "Data pipeline reliability is at risk."
        )

    return score, narrative


# ---------------------------------------------------------------------------
# Section 2: Violations this week
# ---------------------------------------------------------------------------


def build_violation_section(
    violations: list[dict], registry: dict
) -> dict:
    """Count violations by severity and produce plain-language descriptions."""
    by_severity = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for v in violations:
        sev = v.get("severity", "LOW")
        if sev in by_severity:
            by_severity[sev] += 1
        else:
            by_severity[sev] = 1

    # Top 3 violations by severity
    priority_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    sorted_violations = sorted(
        violations,
        key=lambda v: priority_order.index(v.get("severity", "LOW"))
        if v.get("severity", "LOW") in priority_order
        else 99,
    )

    top3_descriptions = []
    subs = registry.get("subscriptions", [])

    for v in sorted_violations[:3]:
        contract_id = v.get("contract_id", "unknown")
        col_name = v.get("column_name", "unknown field")
        message = v.get("message", "")

        # Find downstream subscribers
        affected = [
            s["subscriber_id"]
            for s in subs
            if s.get("contract_id") == contract_id
        ]
        affected_str = ", ".join(affected) if affected else "no registered subscribers"

        # Determine failing system from contract_id
        failing_system = contract_id.split("-")[0] if "-" in contract_id else contract_id

        desc = (
            f"The '{col_name}' field in {failing_system} failed its contract check. "
            f"{message} "
            f"Downstream subscribers affected: {affected_str}. "
            f"Records failing: {v.get('records_failing', 'unknown')}."
        )
        top3_descriptions.append(desc)

    return {
        "total_violations": len(violations),
        "by_severity": by_severity,
        "top_violations": top3_descriptions,
    }


# ---------------------------------------------------------------------------
# Section 3: Schema changes detected
# ---------------------------------------------------------------------------


def build_schema_changes_section(reports_dir: str) -> dict:
    """Summarize schema evolution findings."""
    evo_reports = load_schema_evolution(reports_dir)

    if not evo_reports:
        return {
            "changes_detected": 0,
            "summary": "No schema evolution analysis found. Run schema_analyzer.py to detect changes.",
            "details": [],
        }

    total_changes = 0
    breaking = 0
    details = []

    for report in evo_reports:
        total_changes += report.get("total_changes", 0)
        breaking += report.get("breaking_changes", 0)
        verdict = report.get("compatibility_verdict", "UNKNOWN")

        for change in report.get("changes", []):
            details.append(
                {
                    "field": change.get("field", "unknown"),
                    "change_type": change.get("change_type", "unknown"),
                    "compatibility": change.get("compatibility", "unknown"),
                    "severity": change.get("severity", "unknown"),
                    "action": change.get("action", ""),
                }
            )

    summary = (
        f"{total_changes} schema change(s) detected in the past 7 days. "
        f"{breaking} breaking change(s). "
    )
    if breaking > 0:
        summary += "Migration action required for downstream consumers."
    else:
        summary += "All changes are backward-compatible."

    return {
        "changes_detected": total_changes,
        "breaking_changes": breaking,
        "summary": summary,
        "details": details,
    }


# ---------------------------------------------------------------------------
# Section 4: AI system risk assessment
# ---------------------------------------------------------------------------


def build_ai_risk_section(ai_report_path: str) -> dict:
    """Summarize AI contract extension results."""
    ai = load_ai_report(ai_report_path)

    if not ai:
        return {
            "status": "UNKNOWN",
            "summary": "No AI extension report found. Run ai_extensions.py first.",
            "embedding_drift": {"status": "N/A"},
            "prompt_schema": {"status": "N/A"},
            "output_violation_rate": {"status": "N/A"},
        }

    ed = ai.get("embedding_drift", {})
    ps = ai.get("prompt_schema", {})
    ovr = ai.get("output_violation_rate", {})

    parts = []
    if ed.get("status") == "FAIL":
        parts.append(
            f"Embedding drift DETECTED (score: {ed.get('drift_score', 'N/A')}, "
            f"threshold: {ed.get('threshold', 0.15)}). Semantic content has shifted."
        )
    elif ed.get("status") == "PASS":
        parts.append(f"Embedding drift within bounds (score: {ed.get('drift_score', 'N/A')}).")
    else:
        parts.append(f"Embedding drift: {ed.get('status', 'N/A')}.")

    if ps.get("quarantined", 0) > 0:
        parts.append(f"Prompt schema: {ps['quarantined']} records quarantined out of {ps.get('total', '?')}.")
    else:
        parts.append("Prompt input schema: all records conform.")

    rate = ovr.get("violation_rate", 0)
    trend = ovr.get("trend", "unknown")
    parts.append(f"LLM output violation rate: {rate:.2%} (trend: {trend}).")

    return {
        "status": ai.get("overall_status", "UNKNOWN"),
        "summary": " ".join(parts),
        "embedding_drift": {
            "status": ed.get("status", "N/A"),
            "drift_score": ed.get("drift_score", "N/A"),
            "threshold": ed.get("threshold", "N/A"),
        },
        "prompt_schema": {
            "status": ps.get("status", "N/A"),
            "valid": ps.get("valid", 0),
            "quarantined": ps.get("quarantined", 0),
        },
        "output_violation_rate": {
            "status": ovr.get("status", "N/A"),
            "violation_rate": ovr.get("violation_rate", "N/A"),
            "trend": ovr.get("trend", "N/A"),
        },
    }


# ---------------------------------------------------------------------------
# Section 5: Recommended actions
# ---------------------------------------------------------------------------


def build_recommendations(
    violations: list[dict], reports: list[dict], registry: dict
) -> list[str]:
    """Produce 3 prioritized, specific actions with file paths and contract clauses."""
    recommendations = []

    # Find the most critical violations
    critical = [v for v in violations if v.get("severity") == "CRITICAL"]
    high = [v for v in violations if v.get("severity") == "HIGH"]

    if critical:
        v = critical[0]
        col = v.get("column_name", "unknown")
        contract_id = v.get("contract_id", "unknown")
        # Determine a plausible file path
        week = contract_id.split("-")[0] if "-" in contract_id else "unknown"
        recommendations.append(
            f"CRITICAL: Fix the '{col}' field in the {week} producer. "
            f"The field violates contract '{contract_id}' clause {col}. "
            f"Update the producer code to output values within the contracted range. "
            f"File: contracts/runner.py validates this via check_id '{v.get('check_id', col)}'."
        )

    if high:
        v = high[0]
        col = v.get("column_name", "unknown")
        contract_id = v.get("contract_id", "unknown")
        recommendations.append(
            f"HIGH: Investigate statistical drift in '{col}' for contract '{contract_id}'. "
            f"Run: python contracts/runner.py --contract generated_contracts/ "
            f"--data outputs/ --mode ENFORCE to confirm the violation persists."
        )

    # Always recommend CI integration
    recommendations.append(
        "Add contracts/runner.py as a required CI step before any producer deployment. "
        "Run in AUDIT mode for the first 2 weeks, then switch to ENFORCE. "
        "Configure via: python contracts/runner.py --contract <contract.yaml> "
        "--data <data.jsonl> --mode AUDIT"
    )

    # If no critical/high violations, recommend baseline refresh
    if not critical and not high:
        recommendations.insert(
            0,
            "Schedule monthly baseline refresh for statistical drift thresholds. "
            "Re-run: python contracts/generator.py --source <latest_data.jsonl> "
            "to update schema_snapshots/baselines.json with current statistics.",
        )

    return recommendations[:3]


# ---------------------------------------------------------------------------
# Main report generation
# ---------------------------------------------------------------------------


def generate_report(
    reports_dir: str = "validation_reports/",
    violation_log_path: str = "violation_log/violations.jsonl",
    ai_report_path: str = "validation_reports/ai_extensions.json",
    registry_path: str = "contract_registry/subscriptions.yaml",
) -> dict:
    """Generate the full Enforcer Report from live validation data."""

    reports = load_all_reports(reports_dir)
    violations = load_violations(violation_log_path)
    registry = load_registry(registry_path)

    # Section 1: Data Health Score
    score, narrative = compute_health_score(reports)

    # Section 2: Violations this week
    violation_section = build_violation_section(violations, registry)

    # Section 3: Schema changes detected
    schema_section = build_schema_changes_section(reports_dir)

    # Section 4: AI risk assessment
    ai_section = build_ai_risk_section(ai_report_path)

    # Section 5: Recommended actions
    recommendations = build_recommendations(violations, reports, registry)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period": f"{(datetime.now(timezone.utc) - timedelta(days=7)).date()} to {datetime.now(timezone.utc).date()}",
        "data_health_score": score,
        "health_narrative": narrative,
        "violations_this_week": violation_section,
        "schema_changes": schema_section,
        "ai_risk_assessment": ai_section,
        "recommended_actions": recommendations,
        "reports_analyzed": len(reports),
        "violations_analyzed": len(violations),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="ReportGenerator — auto-generate the Enforcer Report from live data"
    )
    parser.add_argument(
        "--output",
        default="enforcer_report/report_data.json",
        help="Output path for the report JSON",
    )
    parser.add_argument(
        "--reports-dir",
        default="validation_reports/",
        help="Directory containing validation report JSONs",
    )
    parser.add_argument(
        "--violation-log",
        default="violation_log/violations.jsonl",
        help="Path to violation log JSONL",
    )
    parser.add_argument(
        "--ai-report",
        default="validation_reports/ai_extensions.json",
        help="Path to AI extensions report JSON",
    )
    parser.add_argument(
        "--registry",
        default="contract_registry/subscriptions.yaml",
        help="Path to contract registry subscriptions YAML",
    )

    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("ReportGenerator — Enforcer Report")
    print(f"{'='*60}")
    print()

    report = generate_report(
        reports_dir=args.reports_dir,
        violation_log_path=args.violation_log,
        ai_report_path=args.ai_report,
        registry_path=args.registry,
    )

    print(f"  Data Health Score: {report['data_health_score']}/100")
    print(f"  {report['health_narrative']}")
    print(f"  Violations: {report['violations_this_week']['total_violations']}")
    print(f"  Schema changes: {report['schema_changes']['changes_detected']}")
    print(f"  AI risk: {report['ai_risk_assessment']['status']}")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport written: {out_path}")


if __name__ == "__main__":
    main()
