"""Shared test fixtures for the Data Contract Enforcer test suite."""

import json
import sys
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Ensure contracts package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ---------------------------------------------------------------------------
# Sample extraction records (Week 3)
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_extraction_records():
    """5 extraction records matching the Week 3 schema."""
    records = []
    for i in range(5):
        records.append({
            "doc_id": str(uuid.uuid4()),
            "source_path": f"documents/doc_{i}.pdf",
            "source_hash": f"{'a' * 64}",
            "extracted_facts": [
                {
                    "fact_id": str(uuid.uuid4()),
                    "text": f"Revenue exceeded ${(i + 1) * 100} million in Q{i + 1}.",
                    "entity_refs": [str(uuid.uuid4())],
                    "confidence": round(0.7 + i * 0.05, 2),
                    "page_ref": i + 1,
                    "source_excerpt": f"Excerpt from document {i}",
                },
            ],
            "entities": [
                {
                    "entity_id": str(uuid.uuid4()),
                    "name": f"Entity {i}",
                    "type": ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT"][i % 5],
                    "canonical_value": f"Value {i}",
                },
            ],
            "extraction_model": "claude-3-5-sonnet-20241022",
            "processing_time_ms": 1000 + i * 200,
            "token_count": {"input": 3000 + i * 500, "output": 800 + i * 100},
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        })
    return records


@pytest.fixture
def violated_extraction_records(sample_extraction_records):
    """Extraction records with confidence scaled to 0-100 (injected violation)."""
    import copy
    records = copy.deepcopy(sample_extraction_records)
    for r in records:
        for fact in r.get("extracted_facts", []):
            fact["confidence"] = round(fact["confidence"] * 100, 1)
    return records


# ---------------------------------------------------------------------------
# Sample verdict records (Week 2)
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_verdict_records():
    """5 verdict records matching the Week 2 schema."""
    records = []
    for i in range(5):
        records.append({
            "verdict_id": str(uuid.uuid4()),
            "target_ref": f"src/module_{i}.py",
            "rubric_id": "a" * 64,
            "rubric_version": "1.0.0",
            "scores": {
                "clarity": {"score": min(5, i + 1), "evidence": ["good"], "notes": ""},
                "correctness": {"score": min(5, i + 2), "evidence": ["ok"], "notes": ""},
            },
            "overall_verdict": ["PASS", "FAIL", "WARN", "PASS", "PASS"][i],
            "overall_score": 3.0 + i * 0.3,
            "confidence": round(0.8 + i * 0.03, 2),
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
        })
    return records


@pytest.fixture
def invalid_verdict_records():
    """Verdict records with schema violations."""
    return [
        {"verdict_id": str(uuid.uuid4()), "overall_verdict": "INVALID", "scores": {}, "confidence": 0.9},
        {"verdict_id": str(uuid.uuid4()), "overall_verdict": "PASS", "scores": {"x": {"score": 99}}, "confidence": 0.5},
        {"verdict_id": str(uuid.uuid4()), "overall_verdict": "PASS", "scores": {}, "confidence": 5.0},
    ]


# ---------------------------------------------------------------------------
# Sample event records (Week 5)
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_event_records():
    """5 event records matching the Week 5 schema."""
    agg_id = str(uuid.uuid4())
    records = []
    for i in range(5):
        ts = datetime.now(timezone.utc) - timedelta(minutes=5 - i)
        records.append({
            "event_id": str(uuid.uuid4()),
            "event_type": "DocumentProcessed",
            "aggregate_id": agg_id,
            "aggregate_type": "Document",
            "sequence_number": i + 1,
            "payload": {"detail": f"event {i}", "processed": True},
            "metadata": {
                "causation_id": str(uuid.uuid4()) if i > 0 else None,
                "correlation_id": str(uuid.uuid4()),
                "user_id": str(uuid.uuid4()),
                "source_service": "week3-document-refinery",
            },
            "schema_version": "1.0",
            "occurred_at": ts.isoformat(),
            "recorded_at": (ts + timedelta(seconds=1)).isoformat(),
        })
    return records


# ---------------------------------------------------------------------------
# Lineage graph
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_lineage_graph():
    """Lineage graph dict with nodes and edges for week3/week4/week5."""
    return {
        "snapshot_id": str(uuid.uuid4()),
        "codebase_root": "/repo",
        "git_commit": "a" * 40,
        "nodes": [
            {"node_id": "model::week3-extraction-pipeline", "type": "MODEL", "label": "Week 3 Extractor",
             "metadata": {"path": "src/week3/extractor.py", "language": "python", "purpose": "extraction", "last_modified": "2025-01-14T09:00:00Z"}},
            {"node_id": "file::src/week3/extractor.py", "type": "FILE", "label": "extractor.py",
             "metadata": {"path": "src/week3/extractor.py", "language": "python", "purpose": "extraction", "last_modified": "2025-01-14T09:00:00Z"}},
            {"node_id": "model::week4-cartographer", "type": "MODEL", "label": "Week 4 Cartographer",
             "metadata": {"path": "src/week4/processor.py", "language": "python", "purpose": "lineage mapping", "last_modified": "2025-01-14T09:00:00Z"}},
            {"node_id": "file::src/week5/processor.py", "type": "FILE", "label": "processor.py",
             "metadata": {"path": "src/week5/processor.py", "language": "python", "purpose": "event processing", "last_modified": "2025-01-14T09:00:00Z"}},
        ],
        "edges": [
            {"source": "file::src/week3/extractor.py", "target": "model::week3-extraction-pipeline", "relationship": "PRODUCES", "confidence": 0.95},
            {"source": "model::week3-extraction-pipeline", "target": "model::week4-cartographer", "relationship": "CONSUMES", "confidence": 0.90},
            {"source": "model::week4-cartographer", "target": "file::src/week5/processor.py", "relationship": "WRITES", "confidence": 0.85},
        ],
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Contract registry
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_registry():
    """List of 4 subscription dicts matching the registry schema."""
    return [
        {
            "contract_id": "week3-document-refinery-extractions",
            "subscriber_id": "week4-cartographer",
            "subscriber_team": "week4",
            "fields_consumed": ["doc_id", "extracted_facts", "extraction_model"],
            "breaking_fields": [
                {"field": "extracted_facts.confidence", "reason": "used for node ranking"},
                {"field": "doc_id", "reason": "primary key for node identity"},
            ],
            "validation_mode": "ENFORCE",
            "registered_at": "2025-01-10T09:00:00Z",
            "contact": "week4-team@org.com",
        },
        {
            "contract_id": "week3-document-refinery-extractions",
            "subscriber_id": "week7-ai-extensions",
            "subscriber_team": "week7",
            "fields_consumed": ["extracted_facts.confidence", "extracted_facts.text"],
            "breaking_fields": [
                {"field": "extracted_facts.confidence", "reason": "embedding baseline"},
            ],
            "validation_mode": "AUDIT",
            "registered_at": "2025-01-13T09:00:00Z",
            "contact": "week7-team@org.com",
        },
        {
            "contract_id": "week4-brownfield-cartographer-lineage",
            "subscriber_id": "week7-violation-attributor",
            "subscriber_team": "week7",
            "fields_consumed": ["nodes", "edges"],
            "breaking_fields": [{"field": "edges", "reason": "blame chain traversal"}],
            "validation_mode": "ENFORCE",
            "registered_at": "2025-01-13T09:00:00Z",
            "contact": "week7-team@org.com",
        },
        {
            "contract_id": "week5-event-records",
            "subscriber_id": "week7-schema-enforcer",
            "subscriber_team": "week7",
            "fields_consumed": ["event_id", "sequence_number"],
            "breaking_fields": [{"field": "sequence_number", "reason": "monotonic ordering"}],
            "validation_mode": "WARN",
            "registered_at": "2025-01-13T09:00:00Z",
            "contact": "week7-team@org.com",
        },
    ]


# ---------------------------------------------------------------------------
# Sample contract (Bitol YAML structure)
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_contract():
    """Bitol YAML contract dict for week3 extractions."""
    return {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": "week3-document-refinery-extractions",
        "info": {"title": "Week 3 Extractions", "version": "1.0.0", "owner": "week3-team"},
        "schema": {
            "doc_id": {"type": "string", "format": "uuid", "required": True},
            "source_path": {"type": "string", "required": True},
            "source_hash": {"type": "string", "pattern": "^[a-f0-9]{64}$", "required": True},
            "extraction_model": {"type": "string", "enum": ["claude-3-5-sonnet-20241022", "claude-3-haiku-20240307"], "required": True},
            "processing_time_ms": {"type": "integer", "minimum": 0, "required": True},
            "fact_confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
            "entity_type": {"type": "string", "enum": ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]},
        },
    }


# ---------------------------------------------------------------------------
# Baselines
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_baselines():
    """Statistical baselines for drift detection."""
    return {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "columns": {
            "fact_confidence": {"mean": 0.77, "stddev": 0.12},
            "processing_time_ms": {"mean": 1600.0, "stddev": 700.0},
            "token_input": {"mean": 4000.0, "stddev": 1200.0},
        },
    }


# ---------------------------------------------------------------------------
# Flattened DataFrame
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df(sample_extraction_records):
    """Flattened DataFrame from extraction records."""
    from contracts.generator import flatten_for_profile
    return flatten_for_profile(sample_extraction_records)


# ---------------------------------------------------------------------------
# JSONL file helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def write_jsonl(tmp_path):
    """Factory fixture that writes records to a JSONL file and returns the path."""
    def _write(records, filename="data.jsonl"):
        path = tmp_path / filename
        with open(path, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, default=str) + "\n")
        return str(path)
    return _write


@pytest.fixture
def write_yaml(tmp_path):
    """Factory fixture that writes a dict to a YAML file and returns the path."""
    import yaml
    def _write(data, filename="contract.yaml"):
        path = tmp_path / filename
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False)
        return str(path)
    return _write
