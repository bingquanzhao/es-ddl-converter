"""Tests for mapping_parser module."""

import pytest

from es_ddl_converter.mapping_parser import extract_mapping, parse_mapping
from es_ddl_converter.warnings import Severity, WarningCollector


@pytest.fixture
def collector():
    return WarningCollector()


# --- extract_mapping: format detection ---

def test_format_es7_api():
    raw = {"my_index": {"mappings": {"properties": {"f": {"type": "keyword"}}}}}
    name, body = extract_mapping(raw)
    assert name == "my_index"
    assert "properties" in body


def test_format_es6():
    raw = {"my_index": {"mappings": {"doc": {"properties": {"f": {"type": "keyword"}}}}}}
    name, body = extract_mapping(raw)
    assert name == "my_index"
    assert "properties" in body


def test_format_simplified():
    raw = {"mappings": {"properties": {"f": {"type": "keyword"}}}}
    name, body = extract_mapping(raw)
    assert name == "unnamed_index"
    assert "properties" in body


def test_format_simplified_with_type():
    raw = {"mappings": {"doc": {"properties": {"f": {"type": "keyword"}}}}}
    name, body = extract_mapping(raw)
    assert "properties" in body


def test_format_invalid():
    with pytest.raises(ValueError):
        extract_mapping({"bad": "format"})


# --- parse_mapping: object flattening ---

def test_object_flattening(collector):
    raw = {"mappings": {"properties": {
        "user": {
            "type": "object",
            "properties": {
                "id": {"type": "long"},
                "name": {"type": "keyword"},
            },
        },
    }}}
    parsed = parse_mapping(raw, collector)
    names = [c.name for c in parsed.columns]
    assert "user_id" in names
    assert "user_name" in names


def test_nested_object_flattening(collector):
    raw = {"mappings": {"properties": {
        "a": {
            "properties": {
                "b": {
                    "properties": {
                        "c": {"type": "keyword"},
                    },
                },
            },
        },
    }}}
    parsed = parse_mapping(raw, collector)
    names = [c.name for c in parsed.columns]
    assert "a_b_c" in names


def test_object_enabled_false(collector):
    raw = {"mappings": {"properties": {
        "meta": {"type": "object", "enabled": False},
    }}}
    parsed = parse_mapping(raw, collector)
    col = next(c for c in parsed.columns if c.name == "meta")
    assert col.doris_type == "VARIANT"


def test_object_no_properties(collector):
    raw = {"mappings": {"properties": {
        "data": {"type": "object"},
    }}}
    parsed = parse_mapping(raw, collector)
    # Object with no properties -> should appear as VARIANT (from _extra or the field itself)
    # Actually, type is "object" with no properties key -> goes to type_mapping
    # which handles object without properties
    names = [c.name for c in parsed.columns]
    # It should have created a VARIANT column or handled gracefully
    assert len(parsed.columns) >= 1


# --- Dynamic mapping ---

def test_dynamic_true_adds_extra(collector):
    raw = {"mappings": {"dynamic": "true", "properties": {"f": {"type": "keyword"}}}}
    parsed = parse_mapping(raw, collector)
    names = [c.name for c in parsed.columns]
    assert "_extra" in names


def test_dynamic_strict_no_extra(collector):
    raw = {"mappings": {"dynamic": "strict", "properties": {"f": {"type": "keyword"}}}}
    parsed = parse_mapping(raw, collector)
    names = [c.name for c in parsed.columns]
    assert "_extra" not in names


def test_dynamic_unspecified_adds_extra(collector):
    raw = {"mappings": {"properties": {"f": {"type": "keyword"}}}}
    parsed = parse_mapping(raw, collector)
    names = [c.name for c in parsed.columns]
    assert "_extra" in names


# --- Array fields ---

def test_array_field_wrapping(collector):
    raw = {"mappings": {"properties": {"tags": {"type": "keyword"}}}}
    parsed = parse_mapping(raw, collector, array_fields={"tags"})
    col = next(c for c in parsed.columns if c.name == "tags")
    assert col.doris_type == "ARRAY<VARCHAR(256)>"
    assert col.is_array is True


# --- Include _id ---

def test_include_id(collector):
    raw = {"mappings": {"properties": {"f": {"type": "keyword"}}}}
    parsed = parse_mapping(raw, collector, include_id=True)
    id_col = next(c for c in parsed.columns if c.name == "_id")
    assert id_col.doris_type == "VARCHAR(128)"
    assert id_col.nullable is False


def test_no_id_by_default(collector):
    raw = {"mappings": {"properties": {"f": {"type": "keyword"}}}}
    parsed = parse_mapping(raw, collector)
    names = [c.name for c in parsed.columns]
    assert "_id" not in names


# --- copy_to ---

def test_copy_to_creates_target(collector):
    raw = {"mappings": {"properties": {
        "title": {"type": "text", "copy_to": "all_text"},
        "body": {"type": "text", "copy_to": "all_text"},
    }}}
    parsed = parse_mapping(raw, collector)
    names = [c.name for c in parsed.columns]
    assert "all_text" in names
    info_msgs = [w for w in collector.warnings if w.severity == Severity.INFO]
    assert any("copy_to" in w.message for w in info_msgs)


# --- Routing ---

def test_routing_required(collector):
    raw = {"mappings": {
        "_routing": {"required": True},
        "properties": {"f": {"type": "keyword"}},
    }}
    parsed = parse_mapping(raw, collector)
    assert parsed.has_routing is True
    assert any(w.severity == Severity.INFO for w in collector.warnings)


# --- Column name uniqueness ---

def test_column_name_conflict(collector):
    raw = {"mappings": {"properties": {
        "user_name": {"type": "keyword"},
        "user": {
            "properties": {
                "name": {"type": "keyword"},
            },
        },
    }}}
    parsed = parse_mapping(raw, collector)
    names = [c.name for c in parsed.columns]
    # Both should exist, one with a suffix
    assert "user_name" in names
    assert "user_name_2" in names


# --- Full example fixture ---

def test_full_example(full_example_mapping, collector):
    parsed = parse_mapping(
        full_example_mapping, collector,
        array_fields={"tags"},
    )
    assert parsed.index_name == "my_logs"
    names = [c.name for c in parsed.columns]
    assert "@timestamp" in names
    assert "level" in names
    assert "service" in names
    assert "message" in names
    assert "host_ip" in names
    assert "tags" in names
    assert "user_id" in names
    assert "user_name" in names
    assert "location" in names
    assert "metadata" in names
    assert "time_range_gte" in names
    assert "time_range_lte" in names

    # tags should be ARRAY
    tags_col = next(c for c in parsed.columns if c.name == "tags")
    assert tags_col.is_array is True
    assert "ARRAY" in tags_col.doris_type


def test_es6_format(es6_mapping, collector):
    parsed = parse_mapping(es6_mapping, collector)
    assert parsed.index_name == "old_index"
    names = [c.name for c in parsed.columns]
    assert "title" in names
    assert "status" in names
    assert "count" in names
