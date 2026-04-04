"""
ValidationRunner — Executes contract checks against data snapshots.

Usage:
    uv run python contracts/runner.py \
        --contract generated_contracts/week3_extractions.yaml \
        --data outputs/week3/extractions.jsonl \
        --output validation_reports/baseline_run.json
"""

import argparse
import json
import re
import uuid
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# Imports from generator (reuse flatten logic)
# ---------------------------------------------------------------------------

def load_jsonl(path: str, max_records: int | None = None) -> list[dict]:
    """Load JSONL file. Use max_records to cap memory for large files."""
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
                if max_records and len(records) >= max_records:
                    break
    return records


def flatten_for_profile(records: list[dict]) -> pd.DataFrame:
    """Same flatten logic as generator.py."""
    rows = []
    for r in records:
        base = {}
        nested_arrays = {}
        nested_dicts = {}

        for k, v in r.items():
            if isinstance(v, list) and k in (
                "extracted_facts", "entities", "code_refs",
            ):
                nested_arrays[k] = v
            elif isinstance(v, dict) and k in (
                "metadata", "token_count", "payload", "scores",
            ):
                nested_dicts[k] = v
            elif isinstance(v, (list, dict)):
                base[k] = json.dumps(v)
            else:
                base[k] = v

        for dict_key, d in nested_dicts.items():
            prefix = {
                "metadata": "metadata_",
                "token_count": "token_",
                "payload": "payload_",
                "scores": "score_",
            }.get(dict_key, f"{dict_key}_")
            if dict_key == "scores":
                for crit, crit_val in d.items():
                    if isinstance(crit_val, dict):
                        base[f"score_{crit}"] = crit_val.get("score")
                    else:
                        base[f"score_{crit}"] = crit_val
            else:
                for sub_k, sub_v in d.items():
                    if isinstance(sub_v, (dict, list)):
                        base[f"{prefix}{sub_k}"] = json.dumps(sub_v)
                    else:
                        base[f"{prefix}{sub_k}"] = sub_v

        if nested_arrays:
            for arr_key, arr in nested_arrays.items():
                prefix = {
                    "extracted_facts": "fact_",
                    "entities": "entity_",
                    "code_refs": "coderef_",
                }.get(arr_key, f"{arr_key}_")
                for item in arr if arr else [{}]:
                    row = dict(base)
                    if isinstance(item, dict):
                        for ik, iv in item.items():
                            if isinstance(iv, (dict, list)):
                                row[f"{prefix}{ik}"] = json.dumps(iv)
                            else:
                                row[f"{prefix}{ik}"] = iv
                    rows.append(row)
        else:
            rows.append(base)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Check functions
# ---------------------------------------------------------------------------

def check_required(df: pd.DataFrame, col_name: str, clause: dict) -> dict | None:
    """Check that a required column has no nulls."""
    if not clause.get("required"):
        return None
    if col_name not in df.columns:
        return {
            "check_id": f"{col_name}.required",
            "column_name": col_name,
            "check_type": "required",
            "status": "ERROR",
            "actual_value": "column not found in data",
            "expected": "column present and non-null",
            "severity": "CRITICAL",
            "records_failing": len(df),
            "sample_failing": [],
            "message": f"Column '{col_name}' not found in dataset. Schema mismatch.",
        }
    null_count = int(df[col_name].isna().sum())
    if null_count > 0:
        return {
            "check_id": f"{col_name}.required",
            "column_name": col_name,
            "check_type": "required",
            "status": "FAIL",
            "actual_value": f"{null_count} nulls found",
            "expected": "0 nulls",
            "severity": "CRITICAL",
            "records_failing": null_count,
            "sample_failing": df[df[col_name].isna()].index[:5].tolist(),
            "message": f"{col_name} has {null_count} null values but is marked required.",
        }
    return {
        "check_id": f"{col_name}.required",
        "column_name": col_name,
        "check_type": "required",
        "status": "PASS",
        "actual_value": "0 nulls",
        "expected": "0 nulls",
        "severity": "CRITICAL",
        "records_failing": 0,
        "sample_failing": [],
        "message": f"{col_name} has no null values.",
    }


def check_type(df: pd.DataFrame, col_name: str, clause: dict) -> dict | None:
    """Check column type matches contract specification."""
    expected_type = clause.get("type")
    if not expected_type or col_name not in df.columns:
        return None

    actual_dtype = str(df[col_name].dtype)
    type_map = {
        "number": ["float64", "float32", "int64", "int32"],
        "integer": ["int64", "int32"],
        "string": ["object", "str", "string"],
        "boolean": ["bool"],
    }
    acceptable = type_map.get(expected_type, [])

    if actual_dtype not in acceptable:
        # Check if it could be coerced
        if expected_type == "number" and actual_dtype == "object":
            try:
                pd.to_numeric(df[col_name].dropna())
                status = "WARN"
                msg = f"{col_name} is string but coercible to number."
                severity = "MEDIUM"
            except (ValueError, TypeError):
                status = "FAIL"
                msg = f"{col_name} has type {actual_dtype}, expected {expected_type}."
                severity = "CRITICAL"
        else:
            status = "PASS"  # Be lenient for numeric subtypes
            msg = f"{col_name} type is compatible."
            severity = "LOW"
            if expected_type == "number" and actual_dtype in ("int64", "int32"):
                status = "PASS"
            elif expected_type == "integer" and actual_dtype in ("float64",):
                status = "WARN"
                msg = f"{col_name} is float but expected integer."
                severity = "MEDIUM"
            else:
                status = "FAIL"
                msg = f"{col_name} has type {actual_dtype}, expected {expected_type}."
                severity = "CRITICAL"

        if status != "PASS":
            return {
                "check_id": f"{col_name}.type",
                "column_name": col_name,
                "check_type": "type",
                "status": status,
                "actual_value": actual_dtype,
                "expected": expected_type,
                "severity": severity,
                "records_failing": len(df) if status == "FAIL" else 0,
                "sample_failing": [],
                "message": msg,
            }

    return {
        "check_id": f"{col_name}.type",
        "column_name": col_name,
        "check_type": "type",
        "status": "PASS",
        "actual_value": actual_dtype,
        "expected": expected_type,
        "severity": "LOW",
        "records_failing": 0,
        "sample_failing": [],
        "message": f"{col_name} type matches contract.",
    }


def check_range(df: pd.DataFrame, col_name: str, clause: dict) -> dict | None:
    """Check numeric range (minimum/maximum)."""
    if col_name not in df.columns:
        return None
    if "minimum" not in clause and "maximum" not in clause:
        return None
    if not pd.api.types.is_numeric_dtype(df[col_name]):
        return None

    s = df[col_name].dropna()
    if len(s) == 0:
        return None

    data_min = float(s.min())
    data_max = float(s.max())
    data_mean = float(s.mean())
    contract_min = clause.get("minimum")
    contract_max = clause.get("maximum")

    violations = []
    if contract_min is not None and data_min < contract_min:
        violations.append(f"min={data_min} < contract_minimum={contract_min}")
    if contract_max is not None and data_max > contract_max:
        violations.append(f"max={data_max} > contract_maximum={contract_max}")

    if violations:
        failing = 0
        if contract_min is not None:
            failing += int((s < contract_min).sum())
        if contract_max is not None:
            failing += int((s > contract_max).sum())

        return {
            "check_id": f"{col_name}.range",
            "column_name": col_name,
            "check_type": "range",
            "status": "FAIL",
            "actual_value": f"min={data_min}, max={data_max}, mean={round(data_mean, 4)}",
            "expected": f"min>={contract_min}, max<={contract_max}",
            "severity": "CRITICAL",
            "records_failing": failing,
            "sample_failing": [],
            "message": f"Range violation: {'; '.join(violations)}. Breaking change detected.",
        }

    return {
        "check_id": f"{col_name}.range",
        "column_name": col_name,
        "check_type": "range",
        "status": "PASS",
        "actual_value": f"min={data_min}, max={data_max}, mean={round(data_mean, 4)}",
        "expected": f"min>={contract_min}, max<={contract_max}",
        "severity": "CRITICAL",
        "records_failing": 0,
        "sample_failing": [],
        "message": f"{col_name} values within contracted range.",
    }


def check_enum(df: pd.DataFrame, col_name: str, clause: dict) -> dict | None:
    """Check enum conformance."""
    if "enum" not in clause or col_name not in df.columns:
        return None

    allowed = set(clause["enum"])
    s = df[col_name].dropna()
    non_conforming = s[~s.isin(allowed)]

    if len(non_conforming) > 0:
        return {
            "check_id": f"{col_name}.enum",
            "column_name": col_name,
            "check_type": "enum",
            "status": "FAIL",
            "actual_value": f"{len(non_conforming)} non-conforming values",
            "expected": f"one of {sorted(allowed)}",
            "severity": "HIGH",
            "records_failing": int(len(non_conforming)),
            "sample_failing": non_conforming.unique()[:5].tolist(),
            "message": f"{col_name} contains values outside the allowed enum: {non_conforming.unique()[:3].tolist()}",
        }

    return {
        "check_id": f"{col_name}.enum",
        "column_name": col_name,
        "check_type": "enum",
        "status": "PASS",
        "actual_value": f"all values in {sorted(allowed)}",
        "expected": f"one of {sorted(allowed)}",
        "severity": "HIGH",
        "records_failing": 0,
        "sample_failing": [],
        "message": f"{col_name} all values conform to enum.",
    }


def check_uuid_pattern(df: pd.DataFrame, col_name: str, clause: dict) -> dict | None:
    """Check UUID format."""
    if clause.get("format") != "uuid" or col_name not in df.columns:
        return None

    s = df[col_name].dropna().astype(str)
    pattern = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)

    sample = s.head(min(100, len(s)))
    non_matching = sample[~sample.apply(lambda v: bool(pattern.match(str(v))))]

    if len(non_matching) > 0:
        return {
            "check_id": f"{col_name}.uuid_format",
            "column_name": col_name,
            "check_type": "pattern",
            "status": "FAIL",
            "actual_value": f"{len(non_matching)}/{len(sample)} sampled values don't match UUID pattern",
            "expected": "UUID v4 format",
            "severity": "HIGH",
            "records_failing": int(len(non_matching)),
            "sample_failing": non_matching.head(3).tolist(),
            "message": f"{col_name} has non-UUID values.",
        }

    return {
        "check_id": f"{col_name}.uuid_format",
        "column_name": col_name,
        "check_type": "pattern",
        "status": "PASS",
        "actual_value": f"{len(sample)}/{len(sample)} match UUID pattern",
        "expected": "UUID v4 format",
        "severity": "HIGH",
        "records_failing": 0,
        "sample_failing": [],
        "message": f"{col_name} all sampled values are valid UUIDs.",
    }


def check_datetime_format(df: pd.DataFrame, col_name: str, clause: dict) -> dict | None:
    """Check ISO 8601 datetime format."""
    if clause.get("format") != "date-time" or col_name not in df.columns:
        return None

    s = df[col_name].dropna().astype(str)
    failures = 0
    sample_fails = []
    for val in s.head(min(100, len(s))):
        try:
            datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            failures += 1
            if len(sample_fails) < 3:
                sample_fails.append(str(val))

    if failures > 0:
        return {
            "check_id": f"{col_name}.datetime_format",
            "column_name": col_name,
            "check_type": "datetime",
            "status": "FAIL",
            "actual_value": f"{failures} unparseable values",
            "expected": "ISO 8601",
            "severity": "HIGH",
            "records_failing": failures,
            "sample_failing": sample_fails,
            "message": f"{col_name} has {failures} values that don't parse as ISO 8601.",
        }

    return {
        "check_id": f"{col_name}.datetime_format",
        "column_name": col_name,
        "check_type": "datetime",
        "status": "PASS",
        "actual_value": "all values parse as ISO 8601",
        "expected": "ISO 8601",
        "severity": "HIGH",
        "records_failing": 0,
        "sample_failing": [],
        "message": f"{col_name} all values are valid ISO 8601 datetimes.",
    }


def check_sha256_pattern(df: pd.DataFrame, col_name: str, clause: dict) -> dict | None:
    """Check SHA-256 pattern."""
    if clause.get("pattern") != "^[a-f0-9]{64}$" or col_name not in df.columns:
        return None

    s = df[col_name].dropna().astype(str)
    pattern = re.compile(r"^[a-f0-9]{64}$")
    non_matching = s[~s.apply(lambda v: bool(pattern.match(str(v))))]

    if len(non_matching) > 0:
        return {
            "check_id": f"{col_name}.sha256_pattern",
            "column_name": col_name,
            "check_type": "pattern",
            "status": "FAIL",
            "actual_value": f"{len(non_matching)} non-matching values",
            "expected": "SHA-256 hex string (64 chars)",
            "severity": "HIGH",
            "records_failing": int(len(non_matching)),
            "sample_failing": non_matching.head(3).tolist(),
            "message": f"{col_name} contains values that don't match SHA-256 pattern.",
        }

    return {
        "check_id": f"{col_name}.sha256_pattern",
        "column_name": col_name,
        "check_type": "pattern",
        "status": "PASS",
        "actual_value": f"all values match SHA-256 pattern",
        "expected": "SHA-256 hex string (64 chars)",
        "severity": "HIGH",
        "records_failing": 0,
        "sample_failing": [],
        "message": f"{col_name} all values match SHA-256 pattern.",
    }


# ---------------------------------------------------------------------------
# Statistical drift
# ---------------------------------------------------------------------------

def load_baselines(path: str = "schema_snapshots/baselines.json") -> dict:
    """Load statistical baselines."""
    p = Path(path)
    if p.exists():
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("columns", {})
    return {}


def save_baselines(df: pd.DataFrame, path: str = "schema_snapshots/baselines.json"):
    """Write baselines for numeric columns."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    baselines = {}
    for col in df.select_dtypes(include="number").columns:
        s = df[col].dropna()
        if len(s) > 0:
            baselines[col] = {
                "mean": round(float(s.mean()), 6),
                "stddev": round(float(s.std()), 6),
            }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "written_at": datetime.now(timezone.utc).isoformat(),
                "columns": baselines,
            },
            f,
            indent=2,
        )
    print(f"  Baselines written: {path} ({len(baselines)} columns)")


def check_statistical_drift(col_name: str, current_mean: float, current_std: float, baselines: dict) -> dict | None:
    """Check statistical drift against baseline."""
    if col_name not in baselines:
        return None

    b = baselines[col_name]
    z_score = abs(current_mean - b["mean"]) / max(b["stddev"], 1e-9)

    if z_score > 3:
        return {
            "check_id": f"{col_name}.statistical_drift",
            "column_name": col_name,
            "check_type": "statistical_drift",
            "status": "FAIL",
            "actual_value": f"mean={round(current_mean, 4)}, z_score={round(z_score, 2)}",
            "expected": f"mean within 3 stddev of baseline (baseline_mean={b['mean']}, baseline_stddev={b['stddev']})",
            "severity": "HIGH",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"{col_name} mean drifted {z_score:.1f} stddev from baseline. Possible scale change.",
        }
    elif z_score > 2:
        return {
            "check_id": f"{col_name}.statistical_drift",
            "column_name": col_name,
            "check_type": "statistical_drift",
            "status": "WARN",
            "actual_value": f"mean={round(current_mean, 4)}, z_score={round(z_score, 2)}",
            "expected": f"mean within 2 stddev of baseline",
            "severity": "MEDIUM",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"{col_name} mean within warning range ({z_score:.1f} stddev from baseline).",
        }

    return {
        "check_id": f"{col_name}.statistical_drift",
        "column_name": col_name,
        "check_type": "statistical_drift",
        "status": "PASS",
        "actual_value": f"mean={round(current_mean, 4)}, z_score={round(z_score, 2)}",
        "expected": f"mean within 2 stddev of baseline",
        "severity": "LOW",
        "records_failing": 0,
        "sample_failing": [],
        "message": f"{col_name} is statistically stable (z={z_score:.2f}).",
    }


# ---------------------------------------------------------------------------
# Main validation pipeline
# ---------------------------------------------------------------------------

def run_validation(contract_path: str, data_path: str) -> dict:
    """Run all contract checks against the data and return structured report."""
    # Load contract
    with open(contract_path, encoding="utf-8") as f:
        contract = yaml.safe_load(f)

    # Load and flatten data
    records = load_jsonl(data_path)
    df = flatten_for_profile(records)

    # Compute snapshot hash
    with open(data_path, "rb") as f:
        snapshot_hash = hashlib.sha256(f.read()).hexdigest()

    schema = contract.get("schema", {})
    results = []

    # Load baselines for statistical drift
    baselines = load_baselines()

    # Run checks for each schema clause
    for col_name, clause in schema.items():
        # Skip internal metadata fields
        if col_name.startswith("_"):
            continue

        # 1. Required check
        result = check_required(df, col_name, clause)
        if result:
            results.append(result)

        # 2. Type check
        result = check_type(df, col_name, clause)
        if result:
            results.append(result)

        # 3. Range check
        result = check_range(df, col_name, clause)
        if result:
            results.append(result)

        # 4. Enum check
        result = check_enum(df, col_name, clause)
        if result:
            results.append(result)

        # 5. UUID pattern check
        result = check_uuid_pattern(df, col_name, clause)
        if result:
            results.append(result)

        # 6. DateTime format check
        result = check_datetime_format(df, col_name, clause)
        if result:
            results.append(result)

        # 7. SHA-256 pattern check
        result = check_sha256_pattern(df, col_name, clause)
        if result:
            results.append(result)

        # 8. Statistical drift for numeric columns
        if col_name in df.columns and pd.api.types.is_numeric_dtype(df[col_name]):
            s = df[col_name].dropna()
            if len(s) > 0:
                drift_result = check_statistical_drift(
                    col_name, float(s.mean()), float(s.std()), baselines
                )
                if drift_result:
                    results.append(drift_result)

    # Write baselines if first run
    if not baselines:
        save_baselines(df)

    # Build report
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    warned = sum(1 for r in results if r["status"] == "WARN")
    errored = sum(1 for r in results if r["status"] == "ERROR")

    report = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract.get("id", "unknown"),
        "snapshot_id": snapshot_hash,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "errored": errored,
        "results": results,
    }

    return report


# ---------------------------------------------------------------------------
# Enforcement mode logic
# ---------------------------------------------------------------------------


def apply_enforcement_mode(report: dict, mode: str) -> dict:
    """Apply enforcement mode to a completed validation report.

    AUDIT  — log only, never block pipeline.
    WARN   — block on CRITICAL violations only.
    ENFORCE — block on CRITICAL or HIGH violations.
    """
    blocking = []
    for r in report.get("results", []):
        if r["status"] != "FAIL":
            continue
        sev = r.get("severity", "LOW")
        if mode == "ENFORCE" and sev in ("CRITICAL", "HIGH"):
            blocking.append(r["check_id"])
        elif mode == "WARN" and sev == "CRITICAL":
            blocking.append(r["check_id"])
        # AUDIT never blocks

    if blocking:
        action = "BLOCKED"
    elif report.get("failed", 0) > 0:
        action = "PASSED_WITH_WARNINGS" if mode != "AUDIT" else "LOGGED"
    else:
        action = "PASSED" if mode != "AUDIT" else "LOGGED"

    report["mode"] = mode
    report["enforcement_action"] = action
    report["blocking_violations"] = blocking
    return report


def write_violation_log(report: dict, mode: str) -> None:
    """Append FAIL results to violation_log/violations.jsonl."""
    fails = [r for r in report.get("results", []) if r["status"] == "FAIL"]
    if not fails:
        return

    log_dir = Path("violation_log")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "violations.jsonl"

    with open(log_path, "a", encoding="utf-8") as f:
        for r in fails:
            entry = {
                "violation_id": str(uuid.uuid4()),
                "check_id": r["check_id"],
                "contract_id": report.get("contract_id", "unknown"),
                "detected_at": datetime.now(timezone.utc).isoformat(),
                "severity": r.get("severity", "LOW"),
                "column_name": r.get("column_name", ""),
                "message": r.get("message", ""),
                "actual_value": r.get("actual_value", ""),
                "expected": r.get("expected", ""),
                "records_failing": r.get("records_failing", 0),
                "mode": mode,
                "enforcement_action": report.get("enforcement_action", "LOGGED"),
            }
            f.write(json.dumps(entry, default=str) + "\n")

    print(f"\nViolation log updated: {log_path} ({len(fails)} entries)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ValidationRunner — execute contract checks on data snapshots"
    )
    parser.add_argument(
        "--contract", required=True, help="Path to contract YAML file"
    )
    parser.add_argument(
        "--data", required=True, help="Path to data JSONL file"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output path for validation report JSON",
    )
    parser.add_argument(
        "--mode",
        choices=["AUDIT", "WARN", "ENFORCE"],
        default="AUDIT",
        help="AUDIT=log only, WARN=block CRITICAL, ENFORCE=block CRITICAL+HIGH",
    )

    args = parser.parse_args()

    mode = args.mode

    print(f"\n{'='*60}")
    print(f"ValidationRunner")
    print(f"{'='*60}")
    print(f"  Contract: {args.contract}")
    print(f"  Data: {args.data}")
    print(f"  Mode: {mode}")
    print()

    report = run_validation(args.contract, args.data)

    # Apply enforcement mode
    report = apply_enforcement_mode(report, mode)

    print(f"\nResults: {report['total_checks']} checks")
    print(f"  PASS: {report['passed']}")
    print(f"  FAIL: {report['failed']}")
    print(f"  WARN: {report['warned']}")
    print(f"  ERROR: {report['errored']}")
    print(f"  Mode: {mode} -> {report['enforcement_action']}")

    if report["blocking_violations"]:
        print(f"\n  Blocking violations:")
        for cv in report["blocking_violations"]:
            print(f"    - {cv}")

    # Print failures
    for r in report["results"]:
        if r["status"] in ("FAIL", "ERROR"):
            print(f"\n  [{r['status']}] {r['check_id']}: {r['message']}")

    # Write violation log entries for FAIL results
    write_violation_log(report, mode)

    # Write output
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\nReport written: {out_path}")
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        out_dir = Path("validation_reports")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"validation_{ts}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\nReport written: {out_path}")


if __name__ == "__main__":
    main()
