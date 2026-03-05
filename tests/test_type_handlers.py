"""Tests for the handler registry and individual handler functions.

These tests validate the refactored handler-registry design in type_mapping:
- Registry completeness: every constant type group is registered.
- _common_field_props: shared extraction logic works correctly.
- Individual handlers: each can be tested in isolation without going through
  the full map_es_field dispatch.
"""

import pytest

from es_ddl_converter.type_mapping import (
    DIRECT_TYPE_MAP,
    RANGE_BASE_TYPES,
    SKIP_TYPES,
    UNSUPPORTED_TYPES,
    _TYPE_HANDLERS,
    _common_field_props,
    _handle_date,
    _handle_flattened,
    _handle_geo_point,
    _handle_ip,
    _handle_keyword,
    _handle_nested,
    _handle_skip,
    _handle_text,
    _handle_wildcard,
)
from es_ddl_converter.warnings import Severity, WarningCollector


@pytest.fixture
def collector():
    return WarningCollector()


# ---------------------------------------------------------------------------
# Registry completeness
# ---------------------------------------------------------------------------

class TestRegistryCompleteness:
    def test_all_direct_types_registered(self):
        for es_type in DIRECT_TYPE_MAP:
            assert es_type in _TYPE_HANDLERS, (
                "DIRECT_TYPE_MAP type '{}' is missing from _TYPE_HANDLERS".format(es_type)
            )

    def test_all_range_types_registered(self):
        for es_type in RANGE_BASE_TYPES:
            assert es_type in _TYPE_HANDLERS, (
                "RANGE_BASE_TYPES type '{}' is missing from _TYPE_HANDLERS".format(es_type)
            )

    def test_all_skip_types_registered(self):
        for es_type in SKIP_TYPES:
            assert es_type in _TYPE_HANDLERS, (
                "SKIP_TYPES type '{}' is missing from _TYPE_HANDLERS".format(es_type)
            )

    def test_all_unsupported_types_registered(self):
        for es_type in UNSUPPORTED_TYPES:
            assert es_type in _TYPE_HANDLERS, (
                "UNSUPPORTED_TYPES type '{}' is missing from _TYPE_HANDLERS".format(es_type)
            )

    def test_explicitly_implemented_handlers_registered(self):
        explicit_types = [
            "keyword", "constant_keyword",
            "wildcard",
            "text", "match_only_text",
            "completion",
            "search_as_you_type",
            "scaled_float",
            "date",
            "date_nanos",
            "ip",
            "dense_vector",
            "geo_point",
            "point",
            "aggregate_metric_double",
            "nested",
            "flattened",
        ]
        for es_type in explicit_types:
            assert es_type in _TYPE_HANDLERS, (
                "Handler for '{}' is missing from _TYPE_HANDLERS".format(es_type)
            )


# ---------------------------------------------------------------------------
# _common_field_props
# ---------------------------------------------------------------------------

class TestCommonFieldProps:
    def test_index_disabled_when_index_false(self):
        index_disabled, _ = _common_field_props({"type": "keyword", "index": False})
        assert index_disabled is True

    def test_index_enabled_by_default(self):
        index_disabled, _ = _common_field_props({"type": "keyword"})
        assert index_disabled is False

    def test_index_true_is_not_disabled(self):
        index_disabled, _ = _common_field_props({"type": "keyword", "index": True})
        assert index_disabled is False

    def test_default_value_from_null_value(self):
        _, default_str = _common_field_props({"type": "keyword", "null_value": "N/A"})
        assert default_str == "N/A"

    def test_default_value_none_when_absent(self):
        _, default_str = _common_field_props({"type": "keyword"})
        assert default_str is None

    def test_null_value_zero_becomes_string(self):
        _, default_str = _common_field_props({"null_value": 0})
        assert default_str == "0"


# ---------------------------------------------------------------------------
# Individual handler isolation tests
# ---------------------------------------------------------------------------

class TestHandleKeyword:
    def test_default_varchar(self, collector):
        cols = _handle_keyword("status", "status", {"type": "keyword"}, collector, "IPv6")
        assert cols[0].doris_type == "VARCHAR(256)"
        assert cols[0].es_type == "keyword"

    def test_with_ignore_above(self, collector):
        cols = _handle_keyword("f", "f", {"type": "keyword", "ignore_above": 64}, collector, "IPv6")
        assert cols[0].doris_type == "VARCHAR(64)"
        assert "ignore_above=64" in cols[0].comment

    def test_constant_keyword(self, collector):
        cols = _handle_keyword("env", "env", {"type": "constant_keyword"}, collector, "IPv6")
        assert cols[0].es_type == "constant_keyword"

    def test_index_disabled_propagated(self, collector):
        cols = _handle_keyword("f", "f", {"type": "keyword", "index": False}, collector, "IPv6")
        assert cols[0].index_disabled is True

    def test_null_value_propagated(self, collector):
        cols = _handle_keyword("f", "f", {"type": "keyword", "null_value": "NONE"}, collector, "IPv6")
        assert cols[0].default_value == "NONE"


class TestHandleText:
    def test_basic(self, collector):
        cols = _handle_text("body", "body", {"type": "text"}, collector, "IPv6")
        assert cols[0].doris_type == "TEXT"
        assert cols[0].analyzer is None

    def test_with_analyzer(self, collector):
        cols = _handle_text("body", "body", {"type": "text", "analyzer": "ik_max_word"}, collector, "IPv6")
        assert cols[0].analyzer == "ik_max_word"
        assert "analyzer=ik_max_word" in cols[0].comment

    def test_keyword_subfield_detected(self, collector):
        field_def = {"type": "text", "fields": {"keyword": {"type": "keyword"}}}
        cols = _handle_text("title", "title", field_def, collector, "IPv6")
        assert cols[0].has_keyword_subfield is True

    def test_no_keyword_subfield(self, collector):
        cols = _handle_text("body", "body", {"type": "text"}, collector, "IPv6")
        assert cols[0].has_keyword_subfield is False

    def test_match_only_text(self, collector):
        cols = _handle_text("f", "f", {"type": "match_only_text"}, collector, "IPv6")
        assert cols[0].doris_type == "TEXT"
        assert cols[0].es_type == "match_only_text"


class TestHandleDate:
    def test_no_format_defaults_to_datetime3(self, collector):
        cols = _handle_date("ts", "ts", {"type": "date"}, collector, "IPv6")
        assert cols[0].doris_type == "DATETIME(3)"

    def test_format_included_in_comment(self, collector):
        cols = _handle_date("ts", "ts", {"type": "date", "format": "epoch_millis"}, collector, "IPv6")
        assert "format=epoch_millis" in cols[0].comment

    def test_no_warnings_emitted(self, collector):
        _handle_date("ts", "ts", {"type": "date"}, collector, "IPv6")
        assert len(collector.warnings) == 0


class TestHandleIp:
    def test_ipv6_by_default(self, collector):
        cols = _handle_ip("addr", "addr", {"type": "ip"}, collector, "IPv6")
        assert cols[0].doris_type == "IPv6"

    def test_ipv4_when_configured(self, collector):
        cols = _handle_ip("addr", "addr", {"type": "ip"}, collector, "IPv4")
        assert cols[0].doris_type == "IPv4"

    def test_ipv4_case_insensitive(self, collector):
        cols = _handle_ip("addr", "addr", {"type": "ip"}, collector, "ipv4")
        assert cols[0].doris_type == "IPv4"


class TestHandleGeoPoint:
    def test_produces_single_column(self, collector):
        cols = _handle_geo_point("location", "location", {"type": "geo_point"}, collector, "IPv6")
        assert len(cols) == 1

    def test_column_name_unchanged(self, collector):
        cols = _handle_geo_point("loc", "loc", {"type": "geo_point"}, collector, "IPv6")
        assert cols[0].name == "loc"

    def test_maps_to_variant(self, collector):
        cols = _handle_geo_point("loc", "loc", {"type": "geo_point"}, collector, "IPv6")
        assert cols[0].doris_type == "VARIANT"

    def test_emits_warning(self, collector):
        _handle_geo_point("loc", "loc", {"type": "geo_point"}, collector, "IPv6")
        assert len(collector.warnings) == 1
        assert collector.warnings[0].severity == Severity.WARN


class TestHandleNested:
    def test_maps_to_variant(self, collector):
        cols = _handle_nested("items", "items", {"type": "nested"}, collector, "IPv6")
        assert cols[0].doris_type == "VARIANT"

    def test_emits_warning(self, collector):
        _handle_nested("items", "items", {"type": "nested"}, collector, "IPv6")
        assert any(w.severity == Severity.WARN for w in collector.warnings)


class TestHandleWildcard:
    def test_maps_to_string(self, collector):
        cols = _handle_wildcard("pattern", "pattern", {"type": "wildcard"}, collector, "IPv6")
        assert cols[0].doris_type == "STRING"


class TestHandleFlattened:
    def test_maps_to_variant(self, collector):
        cols = _handle_flattened("meta", "meta", {"type": "flattened"}, collector, "IPv6")
        assert cols[0].doris_type == "VARIANT"

    def test_no_warning_emitted(self, collector):
        _handle_flattened("meta", "meta", {"type": "flattened"}, collector, "IPv6")
        assert len(collector.warnings) == 0


class TestHandleSkip:
    def test_returns_empty_list(self, collector):
        result = _handle_skip("alias_field", "alias_field", {"type": "alias"}, collector, "IPv6")
        assert result == []

    def test_no_warnings_emitted(self, collector):
        _handle_skip("alias_field", "alias_field", {"type": "alias"}, collector, "IPv6")
        assert len(collector.warnings) == 0
