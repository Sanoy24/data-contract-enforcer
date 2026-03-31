"""
ContractGenerator — Auto-generates baseline data contracts from JSONL outputs.

Usage:
    uv run python contracts/generator.py \
        --source outputs/week3/extractions.jsonl \
        --contract-id week3-document-refinery-extractions \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --output generated_contracts/
"""

import argparse
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

# ---------------------------------------------------------------------------
# JSONL loading & flattening
# ---------------------------------------------------------------------------


def load_jsonl(path: str) -> list[dict]:
    """Load a JSONL file into a list of dicts."""
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def flatten_for_profile(records: list[dict]) -> pd.DataFrame:
    """Flatten nested JSONL records into a flat DataFrame for profiling.

    - Top-level scalars are kept as-is.
    - `extracted_facts[]` → exploded with `fact_` prefix.
    - `entities[]` → exploded with `entity_` prefix.
    - `metadata` dict → flattened with `metadata_` prefix.
    - `token_count` dict → flattened with `token_` prefix.
    - `scores` dict → exploded with `score_` prefix.
    - `payload` dict → flattened with `payload_` prefix.
    - Other nested dicts/lists are JSON-serialised as strings.
    """
    rows = []
    for r in records:
        base = {}
        nested_arrays = {}
        nested_dicts = {}

        for k, v in r.items():
            if isinstance(v, list) and k in (
                "extracted_facts",
                "entities",
                "code_refs",
            ):
                nested_arrays[k] = v
            elif isinstance(v, dict) and k in (
                "metadata",
                "token_count",
                "payload",
                "scores",
            ):
                nested_dicts[k] = v
            elif isinstance(v, (list, dict)):
                base[k] = json.dumps(v)
            else:
                base[k] = v

        # Flatten nested dicts
        for dict_key, d in nested_dicts.items():
            prefix = {
                "metadata": "metadata_",
                "token_count": "token_",
                "payload": "payload_",
                "scores": "score_",
            }.get(dict_key, f"{dict_key}_")
            if dict_key == "scores":
                # scores is {criterion: {score, evidence, notes}} — flatten to score values
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

        # Explode nested arrays
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
# Column profiling
# ---------------------------------------------------------------------------


def profile_column(series: pd.Series, col_name: str) -> dict:
    """Profile a single column: type, nulls, cardinality, stats, patterns."""
    result = {
        "name": col_name,
        "dtype": str(series.dtype),
        "null_fraction": round(float(series.isna().mean()), 4),
        "cardinality_estimate": int(series.nunique()),
        "sample_values": [str(v) for v in series.dropna().unique()[:5]],
    }

    if pd.api.types.is_numeric_dtype(series):
        s = series.dropna()
        if len(s) > 0:
            try:
                result["stats"] = {
                    "min": round(float(s.min()), 4),
                    "max": round(float(s.max()), 4),
                    "mean": round(float(s.mean()), 4),
                    "p25": round(float(s.quantile(0.25)), 4),
                    "p50": round(float(s.quantile(0.50)), 4),
                    "p75": round(float(s.quantile(0.75)), 4),
                    "p95": round(float(s.quantile(0.95)), 4),
                    "p99": round(float(s.quantile(0.99)), 4),
                    "stddev": round(float(s.std()), 4),
                }
            except (TypeError, ValueError):
                pass  # Skip stats for columns with incompatible types

    # Detect dominant pattern for string columns
    if series.dtype == "object":
        non_null = series.dropna()
        if len(non_null) > 0:
            sample = non_null.head(20).tolist()
            # UUID pattern
            uuid_pat = re.compile(
                r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
            )
            if all(uuid_pat.match(str(v)) for v in sample if v):
                result["detected_pattern"] = "uuid"
            # ISO datetime
            elif all(_is_iso_datetime(str(v)) for v in sample if v):
                result["detected_pattern"] = "datetime"
            # SHA-256
            elif all(re.match(r"^[a-f0-9]{64}$", str(v)) for v in sample if v):
                result["detected_pattern"] = "sha256"

    return result


def _is_iso_datetime(s: str) -> bool:
    try:
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return True
    except (ValueError, AttributeError):
        return False


# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------


def infer_type(dtype_str: str) -> str:
    mapping = {
        "float64": "number",
        "float32": "number",
        "int64": "integer",
        "int32": "integer",
        "bool": "boolean",
        "object": "string",
    }
    return mapping.get(dtype_str, "string")


# ---------------------------------------------------------------------------
# Profile → Bitol YAML clause
# ---------------------------------------------------------------------------


def column_to_clause(profile: dict) -> dict:
    """Translate a column profile into a Bitol contract clause."""
    clause: dict = {
        "type": infer_type(profile["dtype"]),
        "required": profile["null_fraction"] == 0.0,
    }

    name = profile["name"]

    # Confidence columns: must be 0.0–1.0
    if "confidence" in name and clause["type"] == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        clause["description"] = (
            "Confidence score. Must remain 0.0-1.0 float. "
            "BREAKING if changed to 0-100 integer scale."
        )

    # Processing time: positive integer
    if "processing_time" in name and clause["type"] in ("integer", "number"):
        clause["minimum"] = 0
        clause["description"] = "Processing time in milliseconds. Must be non-negative."

    # Sequence number: positive integer
    if "sequence_number" in name and clause["type"] == "integer":
        clause["minimum"] = 1
        clause["description"] = (
            "Monotonically increasing per aggregate. No gaps or duplicates."
        )

    # Score columns (1–5 scale)
    if name.startswith("score_") and clause["type"] in ("integer", "number"):
        clause["minimum"] = 1
        clause["maximum"] = 5
        clause["description"] = "Criterion score on 1-5 integer scale."

    # ID fields: UUID
    if name.endswith("_id") or name == "id":
        pattern = profile.get("detected_pattern", "")
        if pattern == "uuid" or "id" in name:
            clause["format"] = "uuid"
            clause["pattern"] = (
                "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
            )

    # Timestamp fields
    if name.endswith("_at") or name.endswith("_time"):
        clause["format"] = "date-time"
        clause["description"] = clause.get("description", "") or "ISO 8601 timestamp."

    # Hash fields
    if "hash" in name:
        if profile.get("detected_pattern") == "sha256":
            clause["pattern"] = "^[a-f0-9]{64}$"
            clause["description"] = "SHA-256 hash. 64 hex characters."

    # Enum columns (low cardinality string)
    if (
        clause["type"] == "string"
        and profile["cardinality_estimate"] <= 10
        and profile["cardinality_estimate"] > 0
        and profile.get("detected_pattern") is None
    ):
        vals = profile["sample_values"]
        if len(vals) == profile["cardinality_estimate"]:
            clause["enum"] = vals
            clause["description"] = f"Must be one of: {', '.join(vals)}"

    # Add statistical summary for numeric columns
    if "stats" in profile:
        clause["_observed_stats"] = profile["stats"]

    return clause


# ---------------------------------------------------------------------------
# Lineage context injection
# ---------------------------------------------------------------------------


def inject_lineage(contract: dict, lineage_path: str | None, contract_id: str) -> dict:
    """Inject lineage info from Week 4 lineage snapshots."""
    if lineage_path is None or not Path(lineage_path).exists():
        contract["lineage"] = {"upstream": [], "downstream": []}
        return contract

    with open(lineage_path, encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]
    if not lines:
        contract["lineage"] = {"upstream": [], "downstream": []}
        return contract

    snapshot = json.loads(lines[-1])  # latest snapshot

    # Determine which week this contract is for
    week_match = re.search(r"week(\d)", contract_id)
    week_tag = f"week{week_match.group(1)}" if week_match else ""

    # Find nodes that consume this week's output
    consumers = []
    seen = set()
    for edge in snapshot.get("edges", []):
        src = edge.get("source", "")
        tgt = edge.get("target", "")
        if week_tag and week_tag in src and tgt not in seen:
            seen.add(tgt)
            consumers.append(
                {
                    "id": tgt,
                    "description": f"Consumes data from {week_tag} output",
                    "fields_consumed": list(contract.get("schema", {}).keys())[:5],
                    "breaking_if_changed": [],
                }
            )

    contract["lineage"] = {
        "upstream": [],
        "downstream": consumers[:5],
    }
    return contract


# ---------------------------------------------------------------------------
# dbt schema.yml generation
# ---------------------------------------------------------------------------


def generate_dbt_yaml(contract: dict, output_path: Path):
    """Generate a dbt-compatible schema.yml from the contract."""
    schema = contract.get("schema", {})
    table_name = contract.get("id", "unknown_table").replace("-", "_")

    columns = []
    for col_name, clause in schema.items():
        col = {"name": col_name, "description": clause.get("description", "")}
        tests = []

        if clause.get("required"):
            tests.append("not_null")
        if clause.get("format") == "uuid":
            tests.append("unique")
        if "enum" in clause:
            tests.append({"accepted_values": {"values": clause["enum"]}})

        if tests:
            col["tests"] = tests
        columns.append(col)

    dbt_doc = {
        "version": 2,
        "models": [
            {
                "name": table_name,
                "description": contract.get("info", {}).get("description", ""),
                "columns": columns,
            }
        ],
    }

    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(dbt_doc, f, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# Build the full contract
# ---------------------------------------------------------------------------


def build_contract(
    column_profiles: dict,
    contract_id: str,
    source_path: str,
) -> dict:
    """Assemble Bitol-compatible contract YAML from column profiles."""
    schema_clauses = {}
    for col_name, profile in column_profiles.items():
        schema_clauses[col_name] = column_to_clause(profile)

    # Infer title from contract_id
    title_parts = contract_id.replace("-", " ").title()

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": title_parts,
            "version": "1.0.0",
            "owner": contract_id.split("-")[0] + "-team",
            "description": (
                f"Auto-generated data contract for {contract_id}. "
                f"Source: {source_path}. "
                "Each clause is machine-checkable."
            ),
        },
        "servers": {
            "local": {
                "type": "local",
                "path": source_path,
                "format": "jsonl",
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish.",
            "limitations": "Confidence fields must remain in 0.0-1.0 float range.",
        },
        "schema": schema_clauses,
        "quality": {
            "type": "SodaChecks",
            "specification": {
                f"checks for {contract_id}": [
                    f"row_count >= 1",
                ]
                + [
                    f"missing_count({col}) = 0"
                    for col, clause in schema_clauses.items()
                    if clause.get("required")
                ][
                    :5
                ],  # limit to avoid huge spec
            },
        },
    }

    return contract


# ---------------------------------------------------------------------------
# Schema snapshot
# ---------------------------------------------------------------------------


def write_schema_snapshot(contract_id: str, output_path: Path):
    """Copy the generated contract to schema_snapshots for evolution tracking."""
    snapshot_dir = Path("schema_snapshots") / contract_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_path = snapshot_dir / f"{ts}.yaml"
    shutil.copy(output_path, snapshot_path)
    print(f"  Schema snapshot: {snapshot_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="ContractGenerator — auto-generate data contracts from JSONL"
    )
    parser.add_argument("--source", required=True, help="Path to JSONL source file")
    parser.add_argument(
        "--contract-id", required=True, help="Unique contract identifier"
    )
    parser.add_argument(
        "--lineage",
        default=None,
        help="Path to Week 4 lineage snapshots JSONL",
    )
    parser.add_argument(
        "--output",
        default="generated_contracts/",
        help="Output directory for generated contracts",
    )

    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"ContractGenerator — {args.contract_id}")
    print(f"{'='*60}")
    print(f"  Source: {args.source}")
    print(f"  Lineage: {args.lineage or 'none'}")
    print()

    # Step 1: Load data
    print("[1/6] Loading JSONL data...")
    records = load_jsonl(args.source)
    print(f"  Loaded {len(records)} records")

    # Step 2: Flatten for profiling
    print("[2/6] Flattening nested structures...")
    df = flatten_for_profile(records)
    print(f"  Flattened to {len(df)} rows × {len(df.columns)} columns")
    print(f"  Columns: {list(df.columns)}")

    # Step 3: Profile each column
    print("[3/6] Profiling columns...")
    column_profiles = {}
    for col in df.columns:
        column_profiles[col] = profile_column(df[col], col)
        ptype = column_profiles[col]["dtype"]
        nulls = column_profiles[col]["null_fraction"]
        card = column_profiles[col]["cardinality_estimate"]
        print(f"    {col}: {ptype}, null={nulls}, cardinality={card}")

    # Step 4: Build contract
    print("[4/6] Building Bitol YAML contract...")
    contract = build_contract(column_profiles, args.contract_id, args.source)

    # Step 5: Inject lineage
    print("[5/6] Injecting lineage context...")
    contract = inject_lineage(contract, args.lineage, args.contract_id)
    num_downstream = len(contract.get("lineage", {}).get("downstream", []))
    print(f"  Found {num_downstream} downstream consumers")

    # Step 6: Write outputs
    print("[6/6] Writing contract files...")
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Derive filename from contract-id
    filename = (
        args.contract_id.split("-")[-1] if "-" in args.contract_id else args.contract_id
    )
    # For known contracts use the canonical names
    if "week3" in args.contract_id:
        filename = "week3_extractions"
    elif "week5" in args.contract_id:
        filename = "week5_events"
    elif "week1" in args.contract_id:
        filename = "week1_intent_records"
    elif "week2" in args.contract_id:
        filename = "week2_verdicts"
    elif "week4" in args.contract_id:
        filename = "week4_lineage"
    elif "langsmith" in args.contract_id:
        filename = "langsmith_traces"

    yaml_path = output_dir / f"{filename}.yaml"
    with open(yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(
            contract, f, default_flow_style=False, sort_keys=False, allow_unicode=True
        )
    print(f"  Contract: {yaml_path}")

    # Count clauses
    num_clauses = len(contract.get("schema", {}))
    print(f"  Total clauses: {num_clauses}")

    # dbt schema.yml
    dbt_path = output_dir / f"{filename}_dbt.yml"
    generate_dbt_yaml(contract, dbt_path)
    print(f"  dbt YAML: {dbt_path}")

    # Schema snapshot
    write_schema_snapshot(args.contract_id, yaml_path)

    print(f"\n✅ Contract generated: {num_clauses} clauses")
    if num_clauses < 8:
        print(f"  ⚠  Warning: Only {num_clauses} clauses. Target is ≥8.")


if __name__ == "__main__":
    main()
