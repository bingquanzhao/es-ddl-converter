"""Integration tests: full pipeline from mapping JSON to DDL string."""

import json

import pytest

from es_ddl_converter.ddl_renderer import render_ddl
from es_ddl_converter.index_strategy import determine_indexes
from es_ddl_converter.mapping_parser import parse_mapping
from es_ddl_converter.table_builder import build_table
from es_ddl_converter.warnings import WarningCollector


def _run_pipeline(raw_json, array_fields=None, table_model="duplicate",
                  table_name=None, include_id=False, key_columns=None):
    collector = WarningCollector()
    parsed = parse_mapping(
        raw_json, collector,
        array_fields=set(array_fields or []),
        include_id=include_id,
    )
    indexes = determine_indexes(parsed.columns, collector)
    name = table_name or parsed.index_name
    table_def = build_table(
        table_name=name,
        columns=parsed.columns,
        indexes=indexes,
        collector=collector,
        table_model=table_model,
        key_columns=key_columns,
    )
    ddl = render_ddl(table_def)
    return ddl, collector


def test_full_example(full_example_mapping):
    ddl, collector = _run_pipeline(
        full_example_mapping,
        array_fields=["tags"],
        table_name="target_table",
    )
    # Basic structure checks
    assert "CREATE TABLE IF NOT EXISTS `target_table`" in ddl
    assert "DUPLICATE KEY" in ddl
    assert "AUTO PARTITION BY RANGE" in ddl
    assert "DISTRIBUTED BY" in ddl
    assert "PROPERTIES" in ddl

    # Column presence
    assert "`@timestamp`" in ddl
    assert "`level`" in ddl
    assert "`service`" in ddl
    assert "`message`" in ddl
    assert "`host_ip`" in ddl
    assert "`tags`" in ddl
    assert "`user_id`" in ddl
    assert "`user_name`" in ddl
    assert "`location`" in ddl
    assert "`metadata`" in ddl
    assert "`time_range_gte`" in ddl
    assert "`time_range_lte`" in ddl

    # Type checks
    assert "DATETIME(3)" in ddl
    assert "SMALLINT" in ddl
    assert "ARRAY<VARCHAR(256)>" in ddl
    assert "IPv6" in ddl
    assert "VARIANT" in ddl

    # Index checks
    assert "USING INVERTED" in ddl
    assert "parser" in ddl


def test_simple_mapping(simple_mapping):
    ddl, collector = _run_pipeline(simple_mapping)
    assert "CREATE TABLE" in ddl
    assert "`name`" in ddl
    assert "`age`" in ddl
    assert "`active`" in ddl
    assert "VARCHAR(256)" in ddl
    assert "INT" in ddl
    assert "BOOLEAN" in ddl


def test_es6_mapping(es6_mapping):
    ddl, collector = _run_pipeline(es6_mapping)
    assert "CREATE TABLE IF NOT EXISTS `old_index`" in ddl
    assert "`title`" in ddl
    assert "`status`" in ddl
    assert "`count`" in ddl


def test_unique_model():
    raw = {"mappings": {"properties": {"name": {"type": "keyword"}}}}
    ddl, _ = _run_pipeline(raw, table_model="unique", include_id=True)
    assert "UNIQUE KEY" in ddl
    assert "`_id`" in ddl


def test_ddl_valid_sql_syntax():
    """Basic validation: DDL should end with semicolon and have balanced parens."""
    raw = {"mappings": {"properties": {
        "ts": {"type": "date"},
        "msg": {"type": "text"},
    }}}
    ddl, _ = _run_pipeline(raw)
    assert ddl.strip().endswith(";")
    assert ddl.count("(") == ddl.count(")")


def test_compression_in_properties():
    raw = {"mappings": {"properties": {"f": {"type": "keyword"}}}}
    ddl, _ = _run_pipeline(raw)
    assert '"compression" = "ZSTD"' in ddl


def test_no_index_for_float():
    raw = {"mappings": {"properties": {"score": {"type": "float"}}}}
    ddl, _ = _run_pipeline(raw)
    # FLOAT should not have any index
    assert "idx_score" not in ddl
