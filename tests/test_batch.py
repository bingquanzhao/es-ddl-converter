"""Tests for batch.py orchestrator."""

import os
import json

import pytest

from es_ddl_converter.batch import (
    BatchResult,
    IndexResult,
    convert_one_index,
    format_batch_report,
    run_batch,
    _sanitize_table_name,
)


class TestSanitizeTableName:

    def test_hyphens_preserved(self):
        assert _sanitize_table_name("my-index") == "my-index"

    def test_dots_preserved(self):
        assert _sanitize_table_name("logs.2024.01") == "logs.2024.01"

    def test_leading_hyphen_preserved(self):
        assert _sanitize_table_name("-index-") == "-index-"

    def test_empty(self):
        assert _sanitize_table_name("") == "unnamed"

    def test_normal(self):
        assert _sanitize_table_name("my_index") == "my_index"

    def test_backtick_replaced(self):
        assert _sanitize_table_name("my`index") == "my_index"

    def test_no_collision(self):
        a = _sanitize_table_name("logs-2024.01")
        b = _sanitize_table_name("logs-2024_01")
        c = _sanitize_table_name("logs.2024.01")
        assert len({a, b, c}) == 3, "table names must not collide"


class TestConvertOneIndex:

    def test_simple_index(self):
        mapping_body = {
            "properties": {
                "name": {"type": "keyword"},
                "age": {"type": "integer"},
            }
        }
        result = convert_one_index("test_idx", mapping_body, config={})
        assert result.status in ("ok", "warning")
        assert result.ddl is not None
        assert "CREATE TABLE" in result.ddl
        assert "test_idx" in result.ddl

    def test_table_name_prefix(self):
        mapping_body = {
            "properties": {"f": {"type": "keyword"}}
        }
        result = convert_one_index(
            "my_idx", mapping_body, config={},
            table_name_prefix="doris_",
        )
        assert result.table_name == "doris_my_idx"
        assert "doris_my_idx" in result.ddl

    def test_hyphen_in_index_name(self):
        mapping_body = {
            "properties": {"f": {"type": "keyword"}}
        }
        result = convert_one_index("logs-2024-01", mapping_body, config={})
        assert result.table_name == "logs-2024-01"

    def test_unsupported_type_has_error(self):
        mapping_body = {
            "properties": {"rel": {"type": "join"}}
        }
        result = convert_one_index("bad_idx", mapping_body, config={})
        assert result.status == "error"

    def test_config_array_fields(self):
        mapping_body = {
            "properties": {"tags": {"type": "keyword"}}
        }
        result = convert_one_index(
            "idx", mapping_body,
            config={"array_fields": ["tags"]},
        )
        assert "ARRAY" in result.ddl

    def test_unique_model(self):
        mapping_body = {
            "properties": {"name": {"type": "keyword"}}
        }
        result = convert_one_index(
            "idx", mapping_body, config={},
            table_model="unique", include_id=True,
        )
        assert "UNIQUE KEY" in result.ddl


class TestRunBatch:

    def _make_slices(self):
        return {
            "logs": {
                "properties": {
                    "@timestamp": {"type": "date"},
                    "level": {"type": "keyword"},
                }
            },
            "users": {
                "properties": {
                    "name": {"type": "keyword"},
                    "age": {"type": "integer"},
                }
            },
        }

    def test_basic_batch(self, tmp_path):
        slices = self._make_slices()
        result = run_batch(
            index_mapping_slices=slices,
            config={},
            output_dir=str(tmp_path),
        )
        assert result.total == 2
        assert result.errors == 0
        assert len(result.results) == 2

        # Check files were written
        files = os.listdir(str(tmp_path))
        assert "logs.sql" in files
        assert "users.sql" in files

    def test_exclude_pattern(self, tmp_path):
        slices = self._make_slices()
        result = run_batch(
            index_mapping_slices=slices,
            config={},
            output_dir=str(tmp_path),
            exclude_pattern="^users$",
        )
        assert result.total == 2
        assert result.skipped == 1
        assert result.ok + result.warnings == 1

    def test_table_prefix(self, tmp_path):
        slices = {"idx": {"properties": {"f": {"type": "keyword"}}}}
        result = run_batch(
            index_mapping_slices=slices,
            config={},
            output_dir=str(tmp_path),
            table_name_prefix="doris_",
        )
        assert result.results[0].table_name == "doris_idx"
        assert "doris_idx.sql" in os.listdir(str(tmp_path))

    def test_warnings_only_no_files(self, tmp_path):
        slices = self._make_slices()
        result = run_batch(
            index_mapping_slices=slices,
            config={},
            output_dir=str(tmp_path),
            warnings_only=True,
        )
        assert result.total == 2
        # No files should be written
        assert len(os.listdir(str(tmp_path))) == 0

    def test_fail_fast(self, tmp_path):
        slices = {
            "aaa_bad": {"properties": {"rel": {"type": "join"}}},
            "bbb_good": {"properties": {"f": {"type": "keyword"}}},
            "ccc_good": {"properties": {"f": {"type": "keyword"}}},
        }
        result = run_batch(
            index_mapping_slices=slices,
            config={},
            output_dir=str(tmp_path),
            fail_fast=True,
        )
        assert result.errors >= 1
        assert result.skipped >= 1
        # Should have stopped after first error
        assert result.total == 3


class TestFormatBatchReport:

    def test_report_format(self):
        result = BatchResult(
            total=3, ok=2, warnings=0, errors=1, skipped=0,
            results=[
                IndexResult("idx1", "idx1", "ok"),
                IndexResult("idx2", "idx2", "ok"),
                IndexResult("idx3", "idx3", "error",
                            error_message="something broke"),
            ],
        )
        report = format_batch_report(result, "/tmp/out")
        assert "Total:    3" in report
        assert "Success: 2" in report
        assert "Error:   1" in report
        assert "idx3" in report
        assert "something broke" in report
        assert "/tmp/out" in report
