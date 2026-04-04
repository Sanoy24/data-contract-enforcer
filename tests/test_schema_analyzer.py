"""Tests for contracts/schema_analyzer.py — diffing + classification."""

import pytest

from contracts.schema_analyzer import (
    _is_narrow_type,
    classify_change,
    diff_schemas,
)


class TestDiffSchemas:
    def test_no_changes(self, sample_contract):
        changes = diff_schemas(sample_contract, sample_contract)
        assert len(changes) == 0

    def test_field_added(self, sample_contract):
        import copy
        new = copy.deepcopy(sample_contract)
        new["schema"]["new_field"] = {"type": "string", "required": False}
        changes = diff_schemas(sample_contract, new)
        added = [c for c in changes if c["change_type"] == "ADDED"]
        assert len(added) == 1
        assert added[0]["field"] == "new_field"

    def test_field_removed(self, sample_contract):
        import copy
        new = copy.deepcopy(sample_contract)
        del new["schema"]["fact_confidence"]
        changes = diff_schemas(sample_contract, new)
        removed = [c for c in changes if c["change_type"] == "REMOVED"]
        assert len(removed) == 1
        assert removed[0]["field"] == "fact_confidence"

    def test_type_changed(self, sample_contract):
        import copy
        new = copy.deepcopy(sample_contract)
        new["schema"]["fact_confidence"]["type"] = "integer"
        changes = diff_schemas(sample_contract, new)
        modified = [c for c in changes if c["change_type"] == "MODIFIED"]
        assert len(modified) >= 1
        type_change = [c for c in modified if c.get("property") == "type"]
        assert len(type_change) == 1

    def test_enum_changed(self, sample_contract):
        import copy
        new = copy.deepcopy(sample_contract)
        new["schema"]["entity_type"]["enum"] = ["PERSON", "ORG"]  # removed values
        changes = diff_schemas(sample_contract, new)
        enum_changes = [c for c in changes if c.get("property") == "enum"]
        assert len(enum_changes) == 1


class TestClassifyChange:
    def test_add_nullable_compatible(self):
        change = {"field": "notes", "change_type": "ADDED", "new": {"required": False}}
        classified = classify_change(change)
        assert classified["compatibility"] == "COMPATIBLE"
        assert classified["severity"] == "LOW"

    def test_add_required_breaking(self):
        change = {"field": "mandatory", "change_type": "ADDED", "new": {"required": True}}
        classified = classify_change(change)
        assert classified["compatibility"] == "BREAKING"
        assert classified["severity"] == "CRITICAL"

    def test_remove_field_breaking(self):
        change = {"field": "old_field", "change_type": "REMOVED", "old": {"type": "string"}}
        classified = classify_change(change)
        assert classified["compatibility"] == "BREAKING"
        assert classified["severity"] == "HIGH"

    def test_narrow_type_critical(self):
        change = {
            "field": "confidence",
            "change_type": "MODIFIED",
            "property": "type",
            "old": "number",
            "new": "integer",
        }
        classified = classify_change(change)
        assert classified["compatibility"] == "BREAKING"
        assert classified["severity"] == "CRITICAL"

    def test_widen_type_compatible(self):
        change = {
            "field": "count",
            "change_type": "MODIFIED",
            "property": "type",
            "old": "integer",
            "new": "number",
        }
        classified = classify_change(change)
        assert classified["compatibility"] == "COMPATIBLE"

    def test_enum_removal_breaking(self):
        change = {
            "field": "status",
            "change_type": "MODIFIED",
            "property": "enum",
            "old": ["A", "B", "C"],
            "new": ["A", "B"],
        }
        classified = classify_change(change)
        assert classified["compatibility"] == "BREAKING"

    def test_enum_addition_compatible(self):
        change = {
            "field": "status",
            "change_type": "MODIFIED",
            "property": "enum",
            "old": ["A", "B"],
            "new": ["A", "B", "C"],
        }
        classified = classify_change(change)
        assert classified["compatibility"] == "COMPATIBLE"

    def test_statistical_shift_breaking(self):
        change = {
            "field": "confidence",
            "change_type": "STATISTICAL_SHIFT",
            "old": 0.77,
            "new": 77.25,
        }
        classified = classify_change(change)
        assert classified["compatibility"] == "BREAKING"
        assert classified["severity"] == "CRITICAL"


class TestIsNarrowType:
    def test_number_to_integer(self):
        assert _is_narrow_type("number", "integer") is True

    def test_integer_to_number(self):
        assert _is_narrow_type("integer", "number") is False

    def test_string_to_number(self):
        assert _is_narrow_type("string", "number") is True

    def test_same_type(self):
        assert _is_narrow_type("string", "string") is False
