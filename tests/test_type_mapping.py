"""Tests for type_mapping module."""

import pytest

from es_ddl_converter.type_mapping import (
    DorisColumn,
    map_es_field,
    resolve_analyzer_parser,
    resolve_date_type,
    resolve_keyword_type,
    resolve_scaled_float,
)
from es_ddl_converter.warnings import Severity, WarningCollector


@pytest.fixture
def collector():
    return WarningCollector()


# --- Direct type mapping ---

@pytest.mark.parametrize("es_type,expected_doris", [
    ("byte", "TINYINT"),
    ("short", "SMALLINT"),
    ("integer", "INT"),
    ("long", "BIGINT"),
    ("unsigned_long", "LARGEINT"),
    ("float", "FLOAT"),
    ("double", "DOUBLE"),
    ("half_float", "FLOAT"),
    ("boolean", "BOOLEAN"),
    ("binary", "STRING"),
    ("token_count", "INT"),
    ("version", "VARCHAR(64)"),
    ("rank_feature", "DOUBLE"),
    ("rank_features", "VARIANT"),
    ("histogram", "VARIANT"),
    ("sparse_vector", "VARIANT"),
    ("geo_shape", "VARIANT"),
    ("shape", "VARIANT"),
])
def test_direct_type_mapping(es_type, expected_doris, collector):
    cols = map_es_field("f", "f", {"type": es_type}, collector)
    assert len(cols) == 1
    assert cols[0].doris_type == expected_doris


# --- Keyword ---

def test_keyword_default(collector):
    cols = map_es_field("status", "status", {"type": "keyword"}, collector)
    assert cols[0].doris_type == "VARCHAR(256)"


def test_keyword_with_ignore_above(collector):
    cols = map_es_field("f", "f", {"type": "keyword", "ignore_above": 128}, collector)
    assert cols[0].doris_type == "VARCHAR(128)"


def test_keyword_ignore_above_large(collector):
    cols = map_es_field("f", "f", {"type": "keyword", "ignore_above": 100000}, collector)
    assert cols[0].doris_type == "STRING"


def test_keyword_long_value_name(collector):
    cols = map_es_field("request_url", "request_url", {"type": "keyword"}, collector)
    assert cols[0].doris_type == "STRING"


def test_keyword_message_name(collector):
    cols = map_es_field("error_message", "error_message", {"type": "keyword"}, collector)
    assert cols[0].doris_type == "STRING"


def test_constant_keyword(collector):
    cols = map_es_field("env", "env", {"type": "constant_keyword"}, collector)
    assert cols[0].doris_type == "VARCHAR(256)"
    assert cols[0].es_type == "constant_keyword"


# --- resolve_keyword_type ---

def test_resolve_keyword_type_with_ignore_above():
    assert resolve_keyword_type("f", 512) == "VARCHAR(512)"


def test_resolve_keyword_type_no_ignore_above():
    assert resolve_keyword_type("user_id", None) == "VARCHAR(256)"


def test_resolve_keyword_type_long_name():
    assert resolve_keyword_type("description", None) == "STRING"


# --- Wildcard ---

def test_wildcard(collector):
    cols = map_es_field("pattern", "pattern", {"type": "wildcard"}, collector)
    assert cols[0].doris_type == "STRING"
    assert cols[0].es_type == "wildcard"


# --- Text ---

def test_text_default(collector):
    cols = map_es_field("body", "body", {"type": "text"}, collector)
    assert cols[0].doris_type == "TEXT"
    assert cols[0].analyzer is None


def test_text_with_analyzer(collector):
    cols = map_es_field("body", "body", {"type": "text", "analyzer": "ik_max_word"}, collector)
    assert cols[0].analyzer == "ik_max_word"


def test_text_with_keyword_subfield(collector):
    field_def = {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
    }
    cols = map_es_field("title", "title", field_def, collector)
    assert len(cols) == 1
    assert cols[0].has_keyword_subfield is True


def test_match_only_text(collector):
    cols = map_es_field("f", "f", {"type": "match_only_text"}, collector)
    assert cols[0].doris_type == "TEXT"


# --- Date ---

def test_date_default(collector):
    cols = map_es_field("ts", "ts", {"type": "date"}, collector)
    assert cols[0].doris_type == "DATETIME(3)"


def test_date_epoch_millis(collector):
    cols = map_es_field("ts", "ts", {"type": "date", "format": "epoch_millis"}, collector)
    assert cols[0].doris_type == "DATETIME(3)"


def test_date_epoch_second(collector):
    cols = map_es_field("ts", "ts", {"type": "date", "format": "epoch_second"}, collector)
    assert cols[0].doris_type == "DATETIME(0)"


def test_date_date_only(collector):
    cols = map_es_field("d", "d", {"type": "date", "format": "yyyy-MM-dd"}, collector)
    assert cols[0].doris_type == "DATE"


def test_date_nanos(collector):
    cols = map_es_field("ts", "ts", {"type": "date_nanos"}, collector)
    assert cols[0].doris_type == "DATETIME(6)"
    assert any(w.severity == Severity.WARN for w in collector.warnings)


def test_date_multi_format(collector):
    cols = map_es_field("ts", "ts", {
        "type": "date", "format": "strict_date_optional_time||epoch_millis"
    }, collector)
    assert cols[0].doris_type == "DATETIME(3)"


# --- resolve_date_type ---

def test_resolve_date_type_none():
    assert resolve_date_type(None) == "DATETIME(3)"


def test_resolve_date_type_date_only():
    assert resolve_date_type("date") == "DATE"
    assert resolve_date_type("strict_date") == "DATE"


def test_resolve_date_type_epoch():
    assert resolve_date_type("epoch_millis") == "DATETIME(3)"
    assert resolve_date_type("epoch_second") == "DATETIME(0)"


# --- Scaled float ---

def test_scaled_float(collector):
    cols = map_es_field("price", "price", {
        "type": "scaled_float", "scaling_factor": 100
    }, collector)
    assert cols[0].doris_type == "DECIMAL(38, 2)"


def test_scaled_float_1000(collector):
    cols = map_es_field("v", "v", {
        "type": "scaled_float", "scaling_factor": 1000
    }, collector)
    assert cols[0].doris_type == "DECIMAL(38, 3)"


def test_resolve_scaled_float_none():
    assert resolve_scaled_float(None) == "DECIMAL(38, 2)"


def test_resolve_scaled_float_10():
    assert resolve_scaled_float(10) == "DECIMAL(38, 1)"


# --- IP ---

def test_ip_default_ipv6(collector):
    cols = map_es_field("addr", "addr", {"type": "ip"}, collector)
    assert cols[0].doris_type == "IPv6"


def test_ip_ipv4(collector):
    cols = map_es_field("addr", "addr", {"type": "ip"}, collector, ip_type="IPv4")
    assert cols[0].doris_type == "IPv4"


# --- Geo point ---

def test_geo_point(collector):
    cols = map_es_field("loc", "loc", {"type": "geo_point"}, collector)
    assert len(cols) == 1
    assert cols[0].name == "loc"
    assert cols[0].doris_type == "VARIANT"
    assert any(w.severity == Severity.WARN for w in collector.warnings)


# --- Point (Cartesian) ---

def test_point(collector):
    cols = map_es_field("pos", "pos", {"type": "point"}, collector)
    assert len(cols) == 1
    assert cols[0].name == "pos"
    assert cols[0].doris_type == "VARIANT"


# --- Range types ---

@pytest.mark.parametrize("es_type,expected_base", [
    ("integer_range", "INT"),
    ("long_range", "BIGINT"),
    ("float_range", "FLOAT"),
    ("double_range", "DOUBLE"),
    ("date_range", "DATETIME(3)"),
    ("ip_range", "VARCHAR(64)"),
])
def test_range_types(es_type, expected_base, collector):
    cols = map_es_field("r", "r", {"type": es_type}, collector)
    assert len(cols) == 2
    assert cols[0].name == "r_gte"
    assert cols[0].doris_type == expected_base
    assert cols[1].name == "r_lte"


# --- Aggregate metric double ---

def test_aggregate_metric_double(collector):
    cols = map_es_field("agg", "agg", {
        "type": "aggregate_metric_double",
        "metrics": ["min", "max", "sum", "value_count"],
    }, collector)
    assert len(cols) == 4
    names = [c.name for c in cols]
    assert "agg_min" in names
    assert "agg_max" in names
    assert "agg_sum" in names
    assert "agg_count" in names


# --- Nested ---

def test_nested(collector):
    cols = map_es_field("items", "items", {"type": "nested"}, collector)
    assert cols[0].doris_type == "VARIANT"
    assert any(w.severity == Severity.WARN for w in collector.warnings)


# --- Flattened ---

def test_flattened(collector):
    cols = map_es_field("data", "data", {"type": "flattened"}, collector)
    assert cols[0].doris_type == "VARIANT"


# --- Dense vector ---

def test_dense_vector(collector):
    cols = map_es_field("vec", "vec", {"type": "dense_vector", "dims": 128}, collector)
    assert cols[0].doris_type == "ARRAY<FLOAT>"
    assert any(w.severity == Severity.WARN for w in collector.warnings)


# --- Unsupported types ---

def test_join_error(collector):
    cols = map_es_field("rel", "rel", {"type": "join"}, collector)
    assert len(cols) == 0
    assert collector.has_errors()


def test_percolator_error(collector):
    cols = map_es_field("q", "q", {"type": "percolator"}, collector)
    assert len(cols) == 0
    assert collector.has_errors()


# --- Skip types ---

def test_alias_skipped(collector):
    cols = map_es_field("a", "a", {"type": "alias"}, collector)
    assert len(cols) == 0


def test_runtime_skipped(collector):
    cols = map_es_field("r", "r", {"type": "runtime"}, collector)
    assert len(cols) == 0


# --- Unknown type ---

def test_unknown_type(collector):
    cols = map_es_field("u", "u", {"type": "some_future_type"}, collector)
    assert cols[0].doris_type == "VARIANT"
    assert any(w.severity == Severity.WARN for w in collector.warnings)


# --- Analyzer to parser ---

@pytest.mark.parametrize("analyzer,expected_parser", [
    ("standard", "unicode"),
    ("simple", "unicode"),
    ("whitespace", "english"),
    ("english", "english"),
    ("ik_max_word", "ik"),
    ("ik_smart", "ik"),
    ("smartcn", "chinese"),
    ("cjk", "unicode"),
    ("pattern", "unicode"),
    ("keyword", None),
    (None, "unicode"),
])
def test_analyzer_to_parser(analyzer, expected_parser, collector):
    result = resolve_analyzer_parser(analyzer, "f", collector)
    assert result == expected_parser


def test_custom_analyzer_warns(collector):
    result = resolve_analyzer_parser("my_custom_analyzer", "f", collector)
    assert result == "unicode"
    assert len(collector.warnings) == 1
    assert collector.warnings[0].severity == Severity.WARN


# --- null_value -> default ---

def test_null_value_default(collector):
    cols = map_es_field("f", "f", {"type": "keyword", "null_value": "N/A"}, collector)
    assert cols[0].default_value == "N/A"


# --- index: false ---

def test_index_false(collector):
    cols = map_es_field("f", "f", {"type": "keyword", "index": False}, collector)
    assert cols[0].index_disabled is True


# --- Completion / search_as_you_type ---

def test_completion(collector):
    cols = map_es_field("suggest", "suggest", {"type": "completion"}, collector)
    assert cols[0].doris_type == "STRING"


def test_search_as_you_type(collector):
    cols = map_es_field("q", "q", {"type": "search_as_you_type"}, collector)
    assert cols[0].doris_type == "TEXT"
