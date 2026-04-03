"""
SchemaEvolutionAnalyzer — Diffs schema snapshots and classifies changes.

Usage:
    uv run python contracts/schema_analyzer.py \
        --contract-id week3-document-refinery-extractions \
        --output validation_reports/schema_evolution_week3.json
"""

import argparse
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yaml


# ---------------------------------------------------------------------------
# Snapshot loading
# ---------------------------------------------------------------------------


def load_snapshots(
    contract_id: str, since: str | None = None
) -> list[tuple[str, dict]]:
    """Load timestamped schema snapshots for a contract, sorted ascending.

    Returns list of (timestamp_str, contract_dict) tuples.
    """
    snap_dir = Path("schema_snapshots") / contract_id
    if not snap_dir.exists():
        return []

    snapshots = []
    for f in sorted(snap_dir.glob("*.yaml")):
        ts_str = f.stem  # e.g. "20260331_211149"
        try:
            ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
        except ValueError:
            continue

        if since:
            cutoff = _parse_since(since)
            if ts < cutoff:
                continue

        with open(f, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        snapshots.append((ts_str, data))

    return sorted(snapshots, key=lambda x: x[0])


def _parse_since(since_str: str) -> datetime:
    """Parse a --since argument like '7 days ago' or ISO date."""
    since_str = since_str.strip().lower()
    if "days ago" in since_str:
        days = int(since_str.split()[0])
        return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)
    try:
        return datetime.fromisoformat(since_str)
    except ValueError:
        return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)


# ---------------------------------------------------------------------------
# Schema diffing
# ---------------------------------------------------------------------------


def diff_schemas(old_contract: dict, new_contract: dict) -> list[dict]:
    """Compare two contract schemas and produce a list of changes."""
    old_schema = old_contract.get("schema", {})
    new_schema = new_contract.get("schema", {})

    changes = []

    all_fields = set(list(old_schema.keys()) + list(new_schema.keys()))

    for field in sorted(all_fields):
        old_clause = old_schema.get(field)
        new_clause = new_schema.get(field)

        if old_clause is None and new_clause is not None:
            changes.append(
                {
                    "field": field,
                    "change_type": "ADDED",
                    "old": None,
                    "new": _clause_summary(new_clause),
                    "details": f"New field '{field}' added",
                }
            )
        elif old_clause is not None and new_clause is None:
            changes.append(
                {
                    "field": field,
                    "change_type": "REMOVED",
                    "old": _clause_summary(old_clause),
                    "new": None,
                    "details": f"Field '{field}' removed",
                }
            )
        elif old_clause is not None and new_clause is not None:
            field_changes = _diff_clause(field, old_clause, new_clause)
            changes.extend(field_changes)

    return changes


def _clause_summary(clause: dict) -> dict:
    """Extract key properties from a clause for display."""
    if not isinstance(clause, dict):
        return {"value": str(clause)}
    return {
        k: v
        for k, v in clause.items()
        if k in ("type", "required", "enum", "minimum", "maximum", "format", "pattern")
    }


def _diff_clause(field: str, old: dict, new: dict) -> list[dict]:
    """Diff individual clause properties."""
    if not isinstance(old, dict) or not isinstance(new, dict):
        if old != new:
            return [
                {
                    "field": field,
                    "change_type": "MODIFIED",
                    "old": str(old),
                    "new": str(new),
                    "details": f"Value changed for '{field}'",
                }
            ]
        return []

    changes = []
    compare_keys = {"type", "required", "enum", "minimum", "maximum", "format", "pattern"}
    all_keys = (set(old.keys()) | set(new.keys())) & compare_keys

    for key in sorted(all_keys):
        old_val = old.get(key)
        new_val = new.get(key)
        if old_val != new_val:
            changes.append(
                {
                    "field": field,
                    "change_type": "MODIFIED",
                    "property": key,
                    "old": old_val,
                    "new": new_val,
                    "details": f"'{field}.{key}' changed: {old_val} -> {new_val}",
                }
            )

    # Check _observed_stats for statistical drift indicators
    old_stats = old.get("_observed_stats", {})
    new_stats = new.get("_observed_stats", {})
    if old_stats and new_stats:
        old_mean = old_stats.get("mean")
        new_mean = new_stats.get("mean")
        if old_mean is not None and new_mean is not None:
            if isinstance(old_mean, (int, float)) and isinstance(new_mean, (int, float)):
                if abs(new_mean - old_mean) > 10 * max(abs(old_mean), 0.01):
                    changes.append(
                        {
                            "field": field,
                            "change_type": "STATISTICAL_SHIFT",
                            "property": "mean",
                            "old": old_mean,
                            "new": new_mean,
                            "details": (
                                f"'{field}' mean shifted dramatically: "
                                f"{old_mean} -> {new_mean}. Possible scale change."
                            ),
                        }
                    )

    return changes


# ---------------------------------------------------------------------------
# Change classification
# ---------------------------------------------------------------------------


def classify_change(change: dict) -> dict:
    """Classify a schema change using the evolution taxonomy.

    Returns the change dict enriched with compatibility and severity.
    """
    ctype = change.get("change_type", "")
    prop = change.get("property", "")
    old_val = change.get("old")
    new_val = change.get("new")

    if ctype == "ADDED":
        # Check if new field is required
        new_clause = change.get("new", {})
        is_required = False
        if isinstance(new_clause, dict):
            is_required = new_clause.get("required", False)
        if is_required:
            change["compatibility"] = "BREAKING"
            change["severity"] = "CRITICAL"
            change["action"] = (
                f"New required field '{change['field']}' added. "
                "Coordinate with all consumers. Provide default or migration script."
            )
        else:
            change["compatibility"] = "COMPATIBLE"
            change["severity"] = "LOW"
            change["action"] = f"New optional field '{change['field']}'. No consumer action needed."

    elif ctype == "REMOVED":
        change["compatibility"] = "BREAKING"
        change["severity"] = "HIGH"
        change["action"] = (
            f"Field '{change['field']}' removed. "
            "Two-sprint deprecation minimum. Each subscriber must acknowledge removal."
        )

    elif ctype == "STATISTICAL_SHIFT":
        change["compatibility"] = "BREAKING"
        change["severity"] = "CRITICAL"
        change["action"] = (
            f"Statistical shift detected in '{change['field']}'. "
            f"Mean changed from {old_val} to {new_val}. "
            "Possible scale change (e.g., float 0.0-1.0 to int 0-100). "
            "CRITICAL: Requires migration plan with rollback."
        )

    elif ctype == "MODIFIED":
        if prop == "type":
            # Check for narrow type (breaking) vs widen type (compatible)
            narrow = _is_narrow_type(old_val, new_val)
            if narrow:
                change["compatibility"] = "BREAKING"
                change["severity"] = "CRITICAL"
                change["action"] = (
                    f"Type narrowed for '{change['field']}': {old_val} -> {new_val}. "
                    "CRITICAL breaking change. Requires migration plan with rollback. "
                    "Statistical baseline must be re-established."
                )
            else:
                change["compatibility"] = "COMPATIBLE"
                change["severity"] = "LOW"
                change["action"] = f"Type widened for '{change['field']}': {old_val} -> {new_val}. Generally safe."

        elif prop == "required":
            if new_val is True and old_val is not True:
                change["compatibility"] = "BREAKING"
                change["severity"] = "CRITICAL"
                change["action"] = f"Field '{change['field']}' changed from optional to required."
            else:
                change["compatibility"] = "COMPATIBLE"
                change["severity"] = "LOW"
                change["action"] = f"Field '{change['field']}' changed from required to optional."

        elif prop == "enum":
            old_set = set(old_val) if isinstance(old_val, list) else set()
            new_set = set(new_val) if isinstance(new_val, list) else set()
            removed = old_set - new_set
            added = new_set - old_set
            if removed:
                change["compatibility"] = "BREAKING"
                change["severity"] = "HIGH"
                change["action"] = f"Enum values removed from '{change['field']}': {removed}. Breaking change."
            elif added:
                change["compatibility"] = "COMPATIBLE"
                change["severity"] = "LOW"
                change["action"] = f"Enum values added to '{change['field']}': {added}. Safe."
            else:
                change["compatibility"] = "COMPATIBLE"
                change["severity"] = "LOW"
                change["action"] = "No material enum change."

        elif prop in ("minimum", "maximum"):
            # Range changes are potentially breaking
            change["compatibility"] = "BREAKING"
            change["severity"] = "HIGH"
            change["action"] = (
                f"Range changed for '{change['field']}': "
                f"{prop} {old_val} -> {new_val}. "
                "Verify no downstream consumers rely on the previous range."
            )

        else:
            change["compatibility"] = "COMPATIBLE"
            change["severity"] = "LOW"
            change["action"] = f"Minor change to '{change['field']}.{prop}'."

    else:
        change["compatibility"] = "UNKNOWN"
        change["severity"] = "MEDIUM"
        change["action"] = "Review manually."

    return change


def _is_narrow_type(old_type, new_type) -> bool:
    """Detect narrowing type changes (breaking).

    float -> int is narrowing (data loss).
    number -> integer is narrowing.
    string -> number is narrowing.
    """
    narrow_map = {
        ("number", "integer"): True,
        ("float", "integer"): True,
        ("float64", "int64"): True,
        ("string", "number"): True,
        ("string", "integer"): True,
    }
    return narrow_map.get((str(old_type), str(new_type)), False)


# ---------------------------------------------------------------------------
# Migration report construction
# ---------------------------------------------------------------------------


def generate_migration_report(
    contract_id: str,
    old_ts: str,
    new_ts: str,
    classified_changes: list[dict],
    registry_path: str = "contract_registry/subscriptions.yaml",
) -> dict:
    """Generate a full migration impact report."""

    # Load registry for blast radius
    try:
        with open(registry_path, encoding="utf-8") as f:
            reg = yaml.safe_load(f)
        subs = reg.get("subscriptions", [])
    except Exception:
        subs = []

    # Affected subscribers
    affected_subs = [
        s for s in subs if s.get("contract_id") == contract_id
    ]

    breaking_changes = [c for c in classified_changes if c.get("compatibility") == "BREAKING"]
    compatible_changes = [c for c in classified_changes if c.get("compatibility") == "COMPATIBLE"]

    verdict = "BREAKING" if breaking_changes else "COMPATIBLE"

    # Human-readable diff
    diff_lines = []
    for c in classified_changes:
        symbol = "!" if c.get("compatibility") == "BREAKING" else "~"
        diff_lines.append(
            f"  {symbol} {c['field']}: {c.get('details', 'changed')} "
            f"[{c.get('compatibility', '?')}, {c.get('severity', '?')}]"
        )

    # Per-consumer failure mode analysis
    consumer_impacts = []
    for sub in affected_subs:
        consumed = sub.get("fields_consumed", [])
        impact_fields = [
            c["field"]
            for c in breaking_changes
            if c["field"] in consumed
            or any(c["field"].startswith(f.replace(".", "_")) for f in consumed)
        ]
        consumer_impacts.append(
            {
                "subscriber_id": sub["subscriber_id"],
                "contact": sub.get("contact", "unknown"),
                "affected_fields": impact_fields if impact_fields else ["indirect impact via transitive dependency"],
                "failure_mode": (
                    f"Subscriber '{sub['subscriber_id']}' consumes {consumed}. "
                    f"Breaking changes in {[c['field'] for c in breaking_changes]} "
                    "may cause validation failures or silent data corruption."
                ),
                "validation_mode": sub.get("validation_mode", "AUDIT"),
            }
        )

    # Migration checklist
    checklist = []
    if breaking_changes:
        checklist.append("1. Notify all affected subscribers listed in blast_radius.affected_subscribers")
        checklist.append("2. Review each breaking change and confirm no consumer relies on the removed/changed behavior")
        for i, bc in enumerate(breaking_changes, 3):
            checklist.append(f"{i}. {bc.get('action', 'Review change to ' + bc['field'])}")
        checklist.append(f"{len(breaking_changes) + 3}. Update statistical baselines after migration")
        checklist.append(f"{len(breaking_changes) + 4}. Run ValidationRunner in AUDIT mode for 2 weeks post-migration")
    else:
        checklist.append("1. No breaking changes detected. Verify with downstream consumers as a courtesy.")

    # Rollback plan
    rollback_plan = {
        "steps": [
            f"1. Revert to schema snapshot {old_ts}",
            "2. Re-deploy producer with previous schema version",
            "3. Re-run ValidationRunner to confirm rollback success",
            "4. Notify affected subscribers that rollback is complete",
        ],
        "estimated_downtime": "< 1 hour for Tier 1 (same-repo) rollback",
        "data_loss_risk": "LOW if rollback occurs within the same batch cycle" if not breaking_changes
        else "MEDIUM — records processed under new schema may need reprocessing",
    }

    return {
        "report_id": str(__import__("uuid").uuid4()),
        "contract_id": contract_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_old": old_ts,
        "snapshot_new": new_ts,
        "compatibility_verdict": verdict,
        "total_changes": len(classified_changes),
        "breaking_changes": len(breaking_changes),
        "compatible_changes": len(compatible_changes),
        "diff": diff_lines,
        "changes": classified_changes,
        "blast_radius": {
            "affected_subscribers": [
                {"subscriber_id": s["subscriber_id"], "contact": s.get("contact", "")}
                for s in affected_subs
            ],
            "subscriber_count": len(affected_subs),
        },
        "per_consumer_failure_modes": consumer_impacts,
        "migration_checklist": checklist,
        "rollback_plan": rollback_plan,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="SchemaEvolutionAnalyzer — diff schema snapshots and classify changes"
    )
    parser.add_argument(
        "--contract-id",
        required=True,
        help="Contract ID to analyze (matches schema_snapshots/ subdirectory)",
    )
    parser.add_argument(
        "--since",
        default=None,
        help="Only consider snapshots since this time (e.g., '7 days ago')",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output path for migration report JSON",
    )
    parser.add_argument(
        "--registry",
        default="contract_registry/subscriptions.yaml",
        help="Path to contract registry subscriptions YAML",
    )

    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("SchemaEvolutionAnalyzer")
    print(f"{'='*60}")
    print(f"  Contract: {args.contract_id}")
    print(f"  Since: {args.since or 'all time'}")
    print()

    snapshots = load_snapshots(args.contract_id, args.since)

    if len(snapshots) < 2:
        print(f"  Found {len(snapshots)} snapshot(s). Need at least 2 to diff.")
        # Still produce output with no changes
        report = {
            "contract_id": args.contract_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "compatibility_verdict": "NO_CHANGES_DETECTED",
            "total_changes": 0,
            "breaking_changes": 0,
            "compatible_changes": 0,
            "diff": [],
            "changes": [],
            "snapshots_found": len(snapshots),
            "note": "Need at least 2 snapshots to perform diff. Run generator on different data to create another snapshot.",
        }
    else:
        old_ts, old_contract = snapshots[-2]
        new_ts, new_contract = snapshots[-1]
        print(f"  Diffing: {old_ts} -> {new_ts}")

        changes = diff_schemas(old_contract, new_contract)
        classified = [classify_change(c) for c in changes]

        report = generate_migration_report(
            args.contract_id, old_ts, new_ts, classified, args.registry
        )

        breaking = [c for c in classified if c.get("compatibility") == "BREAKING"]
        compatible = [c for c in classified if c.get("compatibility") == "COMPATIBLE"]

        print(f"\n  Changes detected: {len(classified)}")
        print(f"  Breaking: {len(breaking)}")
        print(f"  Compatible: {len(compatible)}")
        print(f"  Verdict: {report['compatibility_verdict']}")

        if breaking:
            print("\n  Breaking changes:")
            for bc in breaking:
                print(f"    [{bc.get('severity', '?')}] {bc['field']}: {bc.get('details', '')}")

    # Write output
    if args.output:
        out_path = Path(args.output)
    else:
        out_path = Path("validation_reports") / f"schema_evolution_{args.contract_id}.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport written: {out_path}")


if __name__ == "__main__":
    main()
