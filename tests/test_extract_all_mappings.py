"""Tests for extract_all_mappings() in mapping_parser."""

import json
import os

import pytest

from es_ddl_converter.mapping_parser import extract_all_mappings

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def _load(name):
    with open(os.path.join(FIXTURES_DIR, name), "r") as f:
        return json.load(f)


class TestExtractAllMappings:

    def test_multi_index_response(self):
        """ES _mapping API response with multiple indexes."""
        raw = _load("multi_index_mapping.json")
        result = extract_all_mappings(raw)
        assert "logs" in result
        assert "users" in result
        assert "events" in result
        # .kibana should be skipped
        assert ".kibana" not in result

    def test_multi_index_has_properties(self):
        raw = _load("multi_index_mapping.json")
        result = extract_all_mappings(raw)
        assert "properties" in result["logs"]
        assert "@timestamp" in result["logs"]["properties"]
        assert "name" in result["users"]["properties"]

    def test_single_index_format1(self):
        """Single-index ES 7+ format delegates correctly."""
        raw = _load("full_example_mapping.json")
        result = extract_all_mappings(raw)
        assert "my_logs" in result
        assert len(result) == 1
        assert "properties" in result["my_logs"]

    def test_single_index_format3(self):
        """Simplified format with no index name."""
        raw = _load("simple_mapping.json")
        result = extract_all_mappings(raw)
        assert "unnamed_index" in result
        assert len(result) == 1

    def test_es6_format(self):
        """ES 6.x format with type wrapper."""
        raw = _load("es6_mapping.json")
        result = extract_all_mappings(raw)
        assert len(result) == 1
        # Should have properties at top level
        idx_name = list(result.keys())[0]
        assert "properties" in result[idx_name]

    def test_system_indexes_skipped(self):
        """Indexes starting with . are skipped."""
        raw = {
            ".kibana": {"mappings": {"properties": {"x": {"type": "keyword"}}}},
            ".security": {"mappings": {"properties": {"y": {"type": "keyword"}}}},
            "real_index": {"mappings": {"properties": {"z": {"type": "keyword"}}}},
        }
        result = extract_all_mappings(raw)
        assert len(result) == 1
        assert "real_index" in result

    def test_empty_raises(self):
        """Empty or invalid JSON raises ValueError."""
        with pytest.raises(ValueError, match="No valid index mappings"):
            extract_all_mappings({})

    def test_only_system_indexes_raises(self):
        """Only system indexes → raises ValueError."""
        raw = {
            ".kibana": {"mappings": {"properties": {"x": {"type": "keyword"}}}},
        }
        with pytest.raises(ValueError, match="No valid index mappings"):
            extract_all_mappings(raw)

    def test_multi_index_with_metadata_keys(self):
        """Keys starting with _ are skipped."""
        raw = {
            "_shards": {"total": 5},
            "my_index": {"mappings": {"properties": {"f": {"type": "keyword"}}}},
        }
        result = extract_all_mappings(raw)
        assert len(result) == 1
        assert "my_index" in result

    def test_multi_index_es6_format(self):
        """Multiple indexes in ES 6.x format (with type wrapper)."""
        raw = {
            "idx_a": {"mappings": {"doc": {"properties": {"a": {"type": "keyword"}}}}},
            "idx_b": {"mappings": {"_doc": {"properties": {"b": {"type": "integer"}}}}},
        }
        result = extract_all_mappings(raw)
        assert len(result) == 2
        assert "a" in result["idx_a"]["properties"]
        assert "b" in result["idx_b"]["properties"]
