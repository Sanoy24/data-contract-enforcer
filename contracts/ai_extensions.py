"""
AI Contract Extensions — Embedding drift, prompt schema, LLM output violation rate.

Usage:
    uv run python contracts/ai_extensions.py \
        --extractions outputs/week3/extractions.jsonl \
        --verdicts outputs/week2/verdicts.jsonl \
        --output validation_reports/ai_extensions.json
"""

import argparse
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    from jsonschema import validate, ValidationError
except ImportError:
    validate = None
    ValidationError = Exception


# ---------------------------------------------------------------------------
# Extension 1: Embedding Drift Detection
# ---------------------------------------------------------------------------

from contracts.llm_client import embed_texts as _llm_embed_texts, describe_config


def check_embedding_drift(
    texts: list[str],
    baseline_path: str = "schema_snapshots/embedding_baseline.npz",
    threshold: float = 0.15,
) -> dict:
    """Check embedding drift via cosine distance from stored centroid.

    On first run, establishes baseline. On subsequent runs, compares.
    """
    if not texts:
        return {
            "status": "ERROR",
            "message": "No texts provided for embedding drift check",
            "drift_score": 0.0,
            "threshold": threshold,
        }

    vecs = embed_texts(texts)
    centroid = vecs.mean(axis=0)

    bp = Path(baseline_path)
    if not bp.exists():
        bp.parent.mkdir(parents=True, exist_ok=True)
        np.savez(bp, centroid=centroid)
        return {
            "status": "BASELINE_SET",
            "drift_score": 0.0,
            "threshold": threshold,
            "sample_size": len(texts),
            "embedding_method": "openai" if _HAS_OPENAI else "hash_mock",
            "message": "Baseline established. Run again to detect drift.",
        }

    baseline_centroid = np.load(bp)["centroid"]
    cosine_sim = float(
        np.dot(centroid, baseline_centroid)
        / (np.linalg.norm(centroid) * np.linalg.norm(baseline_centroid) + 1e-9)
    )
    drift = 1.0 - cosine_sim

    return {
        "status": "FAIL" if drift > threshold else "PASS",
        "drift_score": round(drift, 4),
        "threshold": threshold,
        "cosine_similarity": round(cosine_sim, 4),
        "sample_size": len(texts),
        "embedding_method": "openai" if _HAS_OPENAI else "hash_mock",
        "interpretation": "semantic content shifted" if drift > threshold else "stable",
    }


# ---------------------------------------------------------------------------
# Extension 2: Prompt Input Schema Validation
# ---------------------------------------------------------------------------

PROMPT_INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path"],
    "properties": {
        "doc_id": {"type": "string", "minLength": 36, "maxLength": 36},
        "source_path": {"type": "string", "minLength": 1},
    },
}


def validate_prompt_inputs(
    records: list[dict],
    schema: dict = PROMPT_INPUT_SCHEMA,
    quarantine_path: str = "outputs/quarantine/",
) -> dict:
    """Validate prompt input records against a JSON Schema.

    Non-conforming records are routed to quarantine, never silently dropped.
    """
    if validate is None:
        return {
            "status": "ERROR",
            "message": "jsonschema not installed",
            "valid": len(records),
            "quarantined": 0,
        }

    valid_count = 0
    quarantined = []

    for r in records:
        # Extract the fields that would be passed to a prompt
        prompt_input = {
            "doc_id": r.get("doc_id", ""),
            "source_path": r.get("source_path", ""),
        }
        try:
            validate(instance=prompt_input, schema=schema)
            valid_count += 1
        except ValidationError as e:
            quarantined.append(
                {
                    "record_id": r.get("doc_id", "unknown"),
                    "error": str(e.message),
                    "path": list(e.path) if hasattr(e, "path") else [],
                }
            )

    # Write quarantined records
    if quarantined:
        q_dir = Path(quarantine_path)
        q_dir.mkdir(parents=True, exist_ok=True)
        with open(q_dir / "quarantine.jsonl", "a", encoding="utf-8") as f:
            for q in quarantined:
                f.write(json.dumps(q, default=str) + "\n")

    return {
        "status": "PASS" if not quarantined else "WARN",
        "valid": valid_count,
        "quarantined": len(quarantined),
        "total": len(records),
        "quarantine_path": quarantine_path if quarantined else None,
        "sample_errors": quarantined[:3] if quarantined else [],
    }


# ---------------------------------------------------------------------------
# Extension 3: LLM Output Schema Violation Rate
# ---------------------------------------------------------------------------


def check_output_violation_rate(
    verdict_records: list[dict],
    baseline_rate: float | None = None,
    warn_threshold: float = 0.02,
) -> dict:
    """Check the LLM output schema violation rate for Week 2 verdict records.

    Validates: overall_verdict enum, score integer range, confidence range.
    """
    total = len(verdict_records)
    if total == 0:
        return {
            "status": "ERROR",
            "message": "No verdict records to check",
            "total_outputs": 0,
            "schema_violations": 0,
            "violation_rate": 0.0,
        }

    violations = 0
    violation_details = []

    for v in verdict_records:
        issues = []

        # Check overall_verdict enum
        ov = v.get("overall_verdict")
        if ov not in ("PASS", "FAIL", "WARN"):
            issues.append(f"overall_verdict='{ov}' not in {{PASS, FAIL, WARN}}")

        # Check scores are integers 1-5
        scores = v.get("scores", {})
        if isinstance(scores, dict):
            for criterion, score_data in scores.items():
                if isinstance(score_data, dict):
                    s = score_data.get("score")
                    if not isinstance(s, int) or s < 1 or s > 5:
                        issues.append(f"scores.{criterion}.score={s} not int 1-5")

        # Check confidence range
        conf = v.get("confidence")
        if conf is not None:
            if not isinstance(conf, (int, float)) or conf < 0.0 or conf > 1.0:
                issues.append(f"confidence={conf} not in 0.0-1.0")

        if issues:
            violations += 1
            if len(violation_details) < 3:
                violation_details.append(
                    {"verdict_id": v.get("verdict_id", "unknown"), "issues": issues}
                )

    rate = violations / max(total, 1)

    # Trend calculation
    trend = "unknown"
    if baseline_rate is not None:
        if rate > baseline_rate * 1.5:
            trend = "rising"
        elif rate < baseline_rate * 0.5:
            trend = "falling"
        else:
            trend = "stable"

    status = "PASS"
    if trend == "rising" or rate > warn_threshold:
        status = "WARN"

    return {
        "status": status,
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "trend": trend,
        "baseline_rate": baseline_rate,
        "warn_threshold": warn_threshold,
        "sample_violations": violation_details,
    }


# ---------------------------------------------------------------------------
# Single entry point
# ---------------------------------------------------------------------------


def load_jsonl(path: str) -> list[dict]:
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def run_all_extensions(
    extractions_path: str = "outputs/week3/extractions.jsonl",
    verdicts_path: str = "outputs/week2/verdicts.jsonl",
    embedding_baseline: str = "schema_snapshots/embedding_baseline.npz",
    drift_threshold: float = 0.15,
) -> dict:
    """Run all three AI contract extensions and combine results."""

    results = {}

    # Extension 1: Embedding drift on extracted_facts[].text
    print("  [1/3] Embedding drift detection...")
    try:
        extractions = load_jsonl(extractions_path)
        texts = []
        for r in extractions:
            for fact in r.get("extracted_facts", []):
                t = fact.get("text", "")
                if t:
                    texts.append(t)
        results["embedding_drift"] = check_embedding_drift(
            texts, baseline_path=embedding_baseline, threshold=drift_threshold
        )
    except Exception as e:
        results["embedding_drift"] = {"status": "ERROR", "message": str(e)}

    # Extension 2: Prompt input schema validation
    print("  [2/3] Prompt input schema validation...")
    try:
        if not extractions:
            extractions = load_jsonl(extractions_path)
        results["prompt_schema"] = validate_prompt_inputs(extractions)
    except Exception as e:
        results["prompt_schema"] = {"status": "ERROR", "message": str(e)}

    # Extension 3: LLM output violation rate
    print("  [3/3] LLM output schema violation rate...")
    try:
        verdicts = load_jsonl(verdicts_path)
        results["output_violation_rate"] = check_output_violation_rate(verdicts)
    except Exception as e:
        results["output_violation_rate"] = {"status": "ERROR", "message": str(e)}

    # Write WARN to violation log if output violation rate triggers
    ovr = results.get("output_violation_rate", {})
    if ovr.get("status") == "WARN":
        _write_ai_violation(ovr, "output_violation_rate")

    # Overall status
    statuses = [r.get("status", "PASS") for r in results.values()]
    if "FAIL" in statuses:
        overall = "FAIL"
    elif "WARN" in statuses:
        overall = "WARN"
    elif "ERROR" in statuses:
        overall = "ERROR"
    else:
        overall = "PASS"

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_status": overall,
        **results,
    }


def _write_ai_violation(result: dict, extension_name: str) -> None:
    """Write a WARN entry to violation_log when AI extension threshold is breached."""
    log_dir = Path("violation_log")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "violations.jsonl"

    entry = {
        "violation_id": str(uuid.uuid4()),
        "check_id": f"ai_extension.{extension_name}",
        "contract_id": "ai-contract-extensions",
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "severity": "MEDIUM",
        "column_name": extension_name,
        "message": (
            f"AI extension '{extension_name}' triggered WARN: "
            f"violation_rate={result.get('violation_rate', 'N/A')}, "
            f"trend={result.get('trend', 'unknown')}"
        ),
        "actual_value": str(result.get("violation_rate", "")),
        "expected": f"<= {result.get('warn_threshold', 0.02)}",
        "records_failing": result.get("schema_violations", 0),
        "mode": "AUDIT",
        "enforcement_action": "LOGGED",
    }

    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, default=str) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="AI Contract Extensions — embedding drift, prompt schema, output violation rate"
    )
    parser.add_argument(
        "--extractions",
        default="outputs/week3/extractions.jsonl",
        help="Path to Week 3 extractions JSONL",
    )
    parser.add_argument(
        "--verdicts",
        default="outputs/week2/verdicts.jsonl",
        help="Path to Week 2 verdicts JSONL",
    )
    parser.add_argument(
        "--output",
        default="validation_reports/ai_extensions.json",
        help="Output path for AI extension results",
    )
    parser.add_argument(
        "--embedding-baseline",
        default="schema_snapshots/embedding_baseline.npz",
        help="Path to embedding baseline file",
    )
    parser.add_argument(
        "--drift-threshold",
        type=float,
        default=0.15,
        help="Cosine distance threshold for embedding drift (default: 0.15)",
    )

    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("AI Contract Extensions")
    print(f"{'='*60}")
    print(f"  Extractions: {args.extractions}")
    print(f"  Verdicts: {args.verdicts}")
    print()

    results = run_all_extensions(
        extractions_path=args.extractions,
        verdicts_path=args.verdicts,
        embedding_baseline=args.embedding_baseline,
        drift_threshold=args.drift_threshold,
    )

    print(f"\nOverall status: {results['overall_status']}")
    print(f"  Embedding drift: {results.get('embedding_drift', {}).get('status', 'N/A')}")
    print(f"  Prompt schema: {results.get('prompt_schema', {}).get('status', 'N/A')}")
    print(f"  Output violation rate: {results.get('output_violation_rate', {}).get('status', 'N/A')}")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nReport written: {out_path}")


if __name__ == "__main__":
    main()
