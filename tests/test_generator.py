"""Tests for contracts/generator.py — ContractGenerator profiling + clause building."""

import pandas as pd
import pytest

from contracts.generator import (
    build_contract,
    column_to_clause,
    flatten_for_profile,
    infer_type,
    profile_column,
)


class TestFlattenForProfile:
    def test_flattens_extraction_records(self, sample_extraction_records):
        df = flatten_for_profile(sample_extraction_records)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        # Should have flattened fact_ and entity_ prefixed columns
        cols = list(df.columns)
        assert "doc_id" in cols
        assert any("fact_" in c for c in cols)

    def test_handles_empty_records(self):
        df = flatten_for_profile([])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestProfileColumn:
    def test_numeric_column_stats(self):
        series = pd.Series([0.5, 0.7, 0.8, 0.9, 0.6])
        profile = profile_column(series, "confidence")
        assert profile["name"] == "confidence"
        assert profile["null_fraction"] == 0.0
        assert "stats" in profile
        assert "min" in profile["stats"]
        assert "max" in profile["stats"]
        assert "mean" in profile["stats"]
        assert "stddev" in profile["stats"] or "std" in profile["stats"]

    def test_string_column(self):
        series = pd.Series(["a", "b", "c", "a", "b"])
        profile = profile_column(series, "name")
        assert profile["dtype"] in ("object", "str")  # "str" on Python 3.12+
        assert profile["cardinality_estimate"] == 3

    def test_null_fraction(self):
        series = pd.Series([1.0, None, 3.0, None, 5.0])
        profile = profile_column(series, "col")
        assert profile["null_fraction"] == pytest.approx(0.4)


class TestColumnToClause:
    def test_confidence_gets_range(self):
        profile = {
            "name": "fact_confidence",
            "dtype": "float64",
            "null_fraction": 0.0,
            "cardinality_estimate": 50,
            "sample_values": ["0.87", "0.92"],
        }
        clause = column_to_clause(profile)
        assert clause["type"] == "number"
        assert clause["minimum"] == 0.0
        assert clause["maximum"] == 1.0

    def test_id_field_gets_uuid_format(self):
        profile = {
            "name": "doc_id",
            "dtype": "object",
            "null_fraction": 0.0,
            "cardinality_estimate": 100,
            "sample_values": ["550e8400-e29b-41d4-a716-446655440000"],
            "detected_pattern": "uuid",
        }
        clause = column_to_clause(profile)
        assert clause.get("format") == "uuid"

    def test_timestamp_gets_datetime_format(self):
        profile = {
            "name": "extracted_at",
            "dtype": "object",
            "null_fraction": 0.0,
            "cardinality_estimate": 50,
            "sample_values": ["2025-01-15T14:23:00Z"],
            "detected_pattern": "datetime",
        }
        clause = column_to_clause(profile)
        assert clause.get("format") == "date-time"

    def test_low_cardinality_string_gets_enum(self):
        profile = {
            "name": "status",
            "dtype": "object",
            "null_fraction": 0.0,
            "cardinality_estimate": 3,
            "sample_values": ["PASS", "FAIL", "WARN"],
        }
        clause = column_to_clause(profile)
        assert "enum" in clause
        assert set(clause["enum"]) == {"PASS", "FAIL", "WARN"}


class TestInferType:
    def test_float_to_number(self):
        assert infer_type("float64") == "number"

    def test_int_to_integer(self):
        assert infer_type("int64") == "integer"

    def test_object_to_string(self):
        assert infer_type("object") == "string"

    def test_bool_to_boolean(self):
        assert infer_type("bool") == "boolean"

    def test_unknown_defaults_to_string(self):
        assert infer_type("category") == "string"


class TestBuildContract:
    def test_contract_structure(self):
        profiles = {
            "doc_id": {
                "name": "doc_id", "dtype": "object", "null_fraction": 0.0,
                "cardinality_estimate": 50, "sample_values": ["abc-123"],
            },
        }
        contract = build_contract(profiles, "test-contract", "test.jsonl")
        assert contract["kind"] == "DataContract"
        assert contract["apiVersion"] == "v3.0.0"
        assert contract["id"] == "test-contract"
        assert "schema" in contract
        assert "quality" in contract
        assert "doc_id" in contract["schema"]
