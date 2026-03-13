"""Tests for table_builder module."""

import pytest

from es_ddl_converter.index_strategy import IndexDef
from es_ddl_converter.table_builder import TableDef, build_table
from es_ddl_converter.type_mapping import DorisColumn
from es_ddl_converter.warnings import WarningCollector


@pytest.fixture
def collector():
    return WarningCollector()


def _col(name, doris_type, es_type="keyword", nullable=True):
    return DorisColumn(
        name=name,
        doris_type=doris_type,
        nullable=nullable,
        es_type=es_type,
        es_field_path=name,
    )


def test_duplicate_key_default(collector):
    cols = [
        _col("@timestamp", "DATETIME(3)", es_type="date"),
        _col("level", "VARCHAR(256)"),
        _col("message", "TEXT", es_type="text"),
    ]
    table = build_table("test_table", cols, [], collector)
    assert table.table_model == "DUPLICATE KEY"
    assert "@timestamp" in table.key_columns


def test_unique_key(collector):
    cols = [
        _col("_id", "VARCHAR(128)"),
        _col("name", "VARCHAR(256)"),
    ]
    table = build_table("test_table", cols, [], collector, table_model="unique")
    assert table.table_model == "UNIQUE KEY"
    assert "_id" in table.key_columns


def test_auto_key_selection_time_first(collector):
    cols = [
        _col("status", "VARCHAR(256)"),
        _col("@timestamp", "DATETIME(3)", es_type="date"),
        _col("level", "VARCHAR(256)"),
    ]
    table = build_table("t", cols, [], collector)
    assert table.key_columns[0] == "@timestamp"


def test_explicit_key_columns(collector):
    cols = [
        _col("@timestamp", "DATETIME(3)", es_type="date"),
        _col("region", "VARCHAR(256)"),
        _col("host", "VARCHAR(256)"),
    ]
    table = build_table("t", cols, [], collector, key_columns=["region", "host"])
    assert table.key_columns == ["region", "host"]


def test_invalid_key_column_warned(collector):
    cols = [
        _col("message", "TEXT", es_type="text"),
        _col("level", "VARCHAR(256)"),
    ]
    table = build_table("t", cols, [], collector, key_columns=["message"])
    # TEXT cannot be key, should warn and fall back
    assert "message" not in table.key_columns
    assert len(collector.warnings) > 0


def test_key_columns_reordered_first(collector):
    cols = [
        _col("c", "VARCHAR(256)"),
        _col("a", "DATETIME(3)", es_type="date"),
        _col("b", "VARCHAR(256)"),
    ]
    table = build_table("t", cols, [], collector, key_columns=["a", "b"])
    assert table.columns[0].name == "a"
    assert table.columns[1].name == "b"


def test_key_columns_not_null(collector):
    cols = [
        _col("@timestamp", "DATETIME(3)", es_type="date"),
        _col("level", "VARCHAR(256)"),
    ]
    table = build_table("t", cols, [], collector)
    for col in table.columns:
        if col.name in table.key_columns:
            assert col.nullable is False


def test_auto_partition_on_timestamp(collector):
    cols = [_col("@timestamp", "DATETIME(3)", es_type="date")]
    table = build_table("t", cols, [], collector)
    assert table.partition_expr is not None
    assert "@timestamp" in table.partition_expr


def test_no_partition_without_time(collector):
    cols = [_col("name", "VARCHAR(256)")]
    table = build_table("t", cols, [], collector)
    assert table.partition_expr is None


def test_explicit_partition_field(collector):
    cols = [
        _col("ts", "DATETIME(3)", es_type="date"),
        _col("created", "DATETIME(3)", es_type="date"),
    ]
    table = build_table("t", cols, [], collector, partition_field="created")
    assert "created" in table.partition_expr


def test_random_distribution_default(collector):
    cols = [_col("f", "VARCHAR(256)")]
    table = build_table("t", cols, [], collector)
    assert "RANDOM" in table.distribution_expr


def test_hash_distribution(collector):
    cols = [_col("user_id", "BIGINT", es_type="long")]
    table = build_table("t", cols, [], collector, bucket_strategy="hash(user_id)")
    assert "HASH" in table.distribution_expr
    assert "user_id" in table.distribution_expr


def test_properties_include_compression(collector):
    cols = [_col("f", "VARCHAR(256)")]
    table = build_table("t", cols, [], collector)
    assert table.properties["compression"] == "ZSTD"


def test_time_series_compaction_with_partition(collector):
    cols = [_col("@timestamp", "DATETIME(3)", es_type="date")]
    table = build_table("t", cols, [], collector)
    assert table.properties.get("compaction_policy") == "time_series"


def test_no_compaction_policy_without_partition(collector):
    cols = [_col("name", "VARCHAR(256)")]
    table = build_table("t", cols, [], collector)
    assert "compaction_policy" not in table.properties


def test_enable_single_replica_compaction_default(collector):
    cols = [_col("f", "VARCHAR(256)")]
    table = build_table("t", cols, [], collector)
    assert table.properties["enable_single_replica_compaction"] == "true"


def test_string_cannot_be_key(collector):
    cols = [
        _col("body", "STRING", es_type="text"),
        _col("level", "VARCHAR(256)"),
    ]
    table = build_table("t", cols, [], collector, key_columns=["body"])
    assert "body" not in table.key_columns
