"""Tests for config_validator: job file (-f) and table config (-c) validation."""

import pytest

from es_ddl_converter.config_validator import (
    ConfigValidationError,
    validate_job_file,
    validate_table_config,
)


# ---------------------------------------------------------------------------
# validate_table_config  (-c)
# ---------------------------------------------------------------------------

class TestValidateTableConfig:

    def test_empty_config_is_valid(self):
        validate_table_config({})  # must not raise

    def test_valid_full_config(self):
        validate_table_config({
            "model": "duplicate",
            "include_id": False,
            "replication_num": 3,
            "ip_type": "ipv6",
            "array_fields": ["tags", "categories"],
            "key_columns": ["@timestamp"],
            "partition_field": "@timestamp",
            "bucket_strategy": "random",
        })

    def test_unknown_key_raises(self):
        with pytest.raises(ConfigValidationError) as exc_info:
            validate_table_config({"partition_filed": "@timestamp"})  # typo
        assert "partition_filed" in str(exc_info.value)

    def test_multiple_errors_reported_at_once(self):
        with pytest.raises(ConfigValidationError) as exc_info:
            validate_table_config({
                "typo_key": "x",
                "another_typo": "y",
                "replication_num": "three",
            })
        msg = str(exc_info.value)
        assert "typo_key" in msg
        assert "another_typo" in msg
        assert "replication_num" in msg

    def test_invalid_model(self):
        with pytest.raises(ConfigValidationError, match="model"):
            validate_table_config({"model": "merge"})

    def test_valid_model_unique(self):
        validate_table_config({"model": "unique"})

    def test_table_model_alias_accepted(self):
        validate_table_config({"table_model": "duplicate"})

    def test_replication_num_string_raises(self):
        with pytest.raises(ConfigValidationError, match="replication_num"):
            validate_table_config({"replication_num": "three"})

    def test_replication_num_zero_raises(self):
        with pytest.raises(ConfigValidationError, match="replication_num"):
            validate_table_config({"replication_num": 0})

    def test_replication_num_valid(self):
        validate_table_config({"replication_num": 1})

    def test_ip_type_invalid(self):
        with pytest.raises(ConfigValidationError, match="ip_type"):
            validate_table_config({"ip_type": "v6"})

    def test_ip_type_case_insensitive(self):
        validate_table_config({"ip_type": "IPv4"})
        validate_table_config({"ip_type": "IPV6"})

    def test_array_fields_not_list_raises(self):
        with pytest.raises(ConfigValidationError, match="array_fields"):
            validate_table_config({"array_fields": "tags"})

    def test_array_fields_list_of_non_strings_raises(self):
        with pytest.raises(ConfigValidationError, match="array_fields"):
            validate_table_config({"array_fields": [1, 2]})

    def test_key_columns_not_list_raises(self):
        with pytest.raises(ConfigValidationError, match="key_columns"):
            validate_table_config({"key_columns": "@timestamp"})

    def test_include_id_non_bool_raises(self):
        with pytest.raises(ConfigValidationError, match="include_id"):
            validate_table_config({"include_id": "yes"})


# ---------------------------------------------------------------------------
# validate_job_file  (-f)
# ---------------------------------------------------------------------------

class TestValidateJobFile:

    def test_empty_job_is_valid(self):
        validate_job_file({})

    def test_valid_full_job(self):
        validate_job_file({
            "source": {"url": "http://localhost:9200", "index": "logs-*"},
            "output": {"dir": "./out/"},
            "doris": {"execute": False, "host": "127.0.0.1", "port": 9030},
            "table": {"model": "duplicate", "replication_num": 3},
            "exclude": ".*test.*",
            "fail_fast": False,
            "warnings_only": False,
        })

    def test_unknown_top_level_key_raises(self):
        with pytest.raises(ConfigValidationError, match="souce"):  # typo
            validate_job_file({"souce": {"url": "http://localhost:9200"}})

    def test_unknown_source_key_raises(self):
        with pytest.raises(ConfigValidationError, match="pasword"):  # typo
            validate_job_file({"source": {"url": "http://x", "pasword": "s"}})

    def test_source_multiple_inputs_raises(self):
        with pytest.raises(ConfigValidationError, match="only one of"):
            validate_job_file({"source": {"url": "http://x", "dir": "./mappings/"}})

    def test_unknown_output_key_raises(self):
        with pytest.raises(ConfigValidationError, match="diir"):  # typo
            validate_job_file({"output": {"diir": "./out/"}})

    def test_unknown_doris_key_raises(self):
        with pytest.raises(ConfigValidationError, match="databse"):  # typo
            validate_job_file({"doris": {"databse": "mydb"}})

    def test_doris_port_string_raises(self):
        with pytest.raises(ConfigValidationError, match="doris.port"):
            validate_job_file({"doris": {"port": "9030"}})

    def test_doris_execute_non_bool_raises(self):
        with pytest.raises(ConfigValidationError, match="doris.execute"):
            validate_job_file({"doris": {"execute": "yes"}})

    def test_fail_fast_non_bool_raises(self):
        with pytest.raises(ConfigValidationError, match="fail_fast"):
            validate_job_file({"fail_fast": 1})

    def test_warnings_only_non_bool_raises(self):
        with pytest.raises(ConfigValidationError, match="warnings_only"):
            validate_job_file({"warnings_only": "true"})

    def test_table_section_validated(self):
        with pytest.raises(ConfigValidationError, match="replication_num"):
            validate_job_file({"table": {"replication_num": -1}})

    def test_multiple_errors_in_job(self):
        with pytest.raises(ConfigValidationError) as exc_info:
            validate_job_file({
                "unknown_top": "x",
                "source": {"url": "http://x", "bad_key": "y"},
                "fail_fast": "yes",
            })
        msg = str(exc_info.value)
        assert "unknown_top" in msg
        assert "bad_key" in msg
        assert "fail_fast" in msg
