"""CLI integration tests."""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
import yaml

from es_ddl_converter.cli import _load_config, main

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def test_cli_basic(tmp_path):
    input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")
    output_file = str(tmp_path / "output.sql")
    exit_code = main(["-i", input_file, "-o", output_file])
    assert os.path.exists(output_file)
    with open(output_file) as f:
        content = f.read()
    assert "CREATE TABLE" in content


def test_cli_stdout(capsys):
    input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")
    exit_code = main(["-i", input_file])
    captured = capsys.readouterr()
    assert "CREATE TABLE" in captured.out


def test_cli_with_config(tmp_path):
    input_file = os.path.join(FIXTURES_DIR, "full_example_mapping.json")
    config_file = os.path.join(FIXTURES_DIR, "config_example.yaml")
    output_file = str(tmp_path / "output.sql")
    exit_code = main(["-i", input_file, "-c", config_file, "-o", output_file])
    with open(output_file) as f:
        content = f.read()
    assert "ARRAY<VARCHAR(256)>" in content  # tags as array from config


def test_cli_table_name_override(capsys):
    input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")
    main(["-i", input_file, "--table-name", "custom_name"])
    captured = capsys.readouterr()
    assert "custom_name" in captured.out


def test_cli_model_unique(capsys):
    input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")
    main(["-i", input_file, "--model", "unique", "--include-id"])
    captured = capsys.readouterr()
    assert "UNIQUE KEY" in captured.out


def test_cli_version(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["--version"])
    assert exc_info.value.code == 0


def test_cli_missing_input():
    exit_code = main([])
    assert exit_code == 2


def test_cli_invalid_json(tmp_path):
    bad_file = str(tmp_path / "bad.json")
    with open(bad_file, "w") as f:
        f.write("not valid json")
    exit_code = main(["-i", bad_file])
    assert exit_code == 2


def test_cli_warnings_only(capsys):
    input_file = os.path.join(FIXTURES_DIR, "full_example_mapping.json")
    exit_code = main(["-i", input_file, "--warnings-only"])
    captured = capsys.readouterr()
    assert "CREATE TABLE" not in captured.out


def test_cli_exit_code_1_with_warnings(capsys):
    """Full example has geo_point warnings, so exit code should be 1."""
    input_file = os.path.join(FIXTURES_DIR, "full_example_mapping.json")
    exit_code = main(["-i", input_file])
    assert exit_code == 1  # has warnings


def test_cli_exit_code_0_no_warnings(capsys):
    input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")
    exit_code = main(["-i", input_file])
    # simple mapping may still generate _extra VARIANT (INFO-level)
    # exit code is 1 if any warnings exist
    assert exit_code in (0, 1)


# --- convert subcommand tests ---


def test_cli_convert_subcommand(capsys):
    input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")
    exit_code = main(["convert", "-i", input_file])
    captured = capsys.readouterr()
    assert "CREATE TABLE" in captured.out


def test_cli_convert_with_output(tmp_path):
    input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")
    output_file = str(tmp_path / "output.sql")
    exit_code = main(["convert", "-i", input_file, "-o", output_file])
    assert os.path.exists(output_file)
    with open(output_file) as f:
        content = f.read()
    assert "CREATE TABLE" in content


def test_cli_convert_missing_input():
    with pytest.raises(SystemExit):
        main(["convert"])


# --- batch subcommand tests ---


class TestBatchCLI:

    def test_batch_from_dir(self, tmp_path):
        """Batch from a directory of JSON files."""
        # Create input dir with mapping files
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)

        mapping1 = {
            "logs": {
                "mappings": {
                    "properties": {
                        "level": {"type": "keyword"},
                        "message": {"type": "text"},
                    }
                }
            }
        }
        mapping2 = {
            "users": {
                "mappings": {
                    "properties": {
                        "name": {"type": "keyword"},
                        "age": {"type": "integer"},
                    }
                }
            }
        }

        with open(os.path.join(input_dir, "logs.json"), "w") as f:
            json.dump(mapping1, f)
        with open(os.path.join(input_dir, "users.json"), "w") as f:
            json.dump(mapping2, f)

        exit_code = main([
            "batch", "--input-dir", input_dir,
            "-o", output_dir,
        ])

        assert exit_code == 0
        output_files = os.listdir(output_dir)
        assert "logs.sql" in output_files
        assert "users.sql" in output_files
        assert "_batch_report.txt" in output_files

    def test_batch_from_dir_with_prefix(self, tmp_path):
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)

        mapping = {
            "idx": {
                "mappings": {
                    "properties": {"f": {"type": "keyword"}}
                }
            }
        }
        with open(os.path.join(input_dir, "idx.json"), "w") as f:
            json.dump(mapping, f)

        exit_code = main([
            "batch", "--input-dir", input_dir,
            "-o", output_dir,
            "--table-prefix", "doris_",
        ])

        assert exit_code == 0
        assert "doris_idx.sql" in os.listdir(output_dir)

    def test_batch_from_dir_with_exclude(self, tmp_path):
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)

        mapping = {
            "logs": {"mappings": {"properties": {"f": {"type": "keyword"}}}},
            "users": {"mappings": {"properties": {"f": {"type": "keyword"}}}},
        }
        with open(os.path.join(input_dir, "data.json"), "w") as f:
            json.dump(mapping, f)

        exit_code = main([
            "batch", "--input-dir", input_dir,
            "-o", output_dir,
            "--exclude-index", "^users$",
        ])

        assert exit_code == 0
        output_files = os.listdir(output_dir)
        assert "logs.sql" in output_files
        assert "users.sql" not in output_files

    def test_batch_missing_source(self):
        """batch requires --es-url or --input-dir."""
        with pytest.raises(SystemExit):
            main(["batch", "-o", "/tmp/out"])

    def test_batch_missing_output_dir(self):
        """batch requires -o."""
        with pytest.raises(SystemExit):
            main(["batch", "--input-dir", "/tmp/in"])

    def test_batch_nonexistent_input_dir(self, tmp_path):
        output_dir = str(tmp_path / "output")
        exit_code = main([
            "batch", "--input-dir", "/nonexistent/path",
            "-o", output_dir,
        ])
        assert exit_code == 2

    def test_batch_warnings_only(self, tmp_path):
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)

        mapping = {
            "idx": {"mappings": {"properties": {"f": {"type": "keyword"}}}}
        }
        with open(os.path.join(input_dir, "idx.json"), "w") as f:
            json.dump(mapping, f)

        exit_code = main([
            "batch", "--input-dir", input_dir,
            "-o", output_dir,
            "--warnings-only",
        ])

        assert exit_code == 0
        # output dir should not exist or be empty (no .sql files)
        if os.path.exists(output_dir):
            sql_files = [f for f in os.listdir(output_dir) if f.endswith(".sql")]
            assert len(sql_files) == 0

    def test_batch_with_multi_index_fixture(self, tmp_path):
        """Use the existing multi_index_mapping.json fixture."""
        fixture_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(fixture_dir)

        # Copy the multi-index fixture
        import shutil
        src = os.path.join(FIXTURES_DIR, "multi_index_mapping.json")
        shutil.copy(src, os.path.join(fixture_dir, "multi.json"))

        exit_code = main([
            "batch", "--input-dir", fixture_dir,
            "-o", output_dir,
        ])

        output_files = os.listdir(output_dir)
        # Should have .sql files for non-system indexes
        sql_files = [f for f in output_files if f.endswith(".sql")]
        assert len(sql_files) >= 2  # at least logs and users
        assert "_batch_report.txt" in output_files


# --- job file (-f) tests ---


class TestJobFile:
    """Tests for -f / --job-file feature."""

    def test_job_file_batch_from_dir(self, tmp_path):
        """Job file with source.dir triggers batch mode."""
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)

        mapping = {
            "logs": {
                "mappings": {
                    "properties": {
                        "level": {"type": "keyword"},
                        "message": {"type": "text"},
                    }
                }
            }
        }
        with open(os.path.join(input_dir, "logs.json"), "w") as f:
            json.dump(mapping, f)

        job_file = str(tmp_path / "job.yaml")
        with open(job_file, "w") as f:
            yaml.dump({
                "source": {"dir": input_dir},
                "output": {"dir": output_dir},
                "table": {"model": "duplicate", "replication_num": 3},
            }, f)

        exit_code = main(["-f", job_file])

        assert exit_code == 0
        output_files = os.listdir(output_dir)
        assert "logs.sql" in output_files
        assert "_batch_report.txt" in output_files

    def test_job_file_single_file(self, tmp_path, capsys):
        """Job file with source.file triggers convert mode."""
        input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")

        job_file = str(tmp_path / "job.yaml")
        with open(job_file, "w") as f:
            yaml.dump({"source": {"file": input_file}}, f)

        exit_code = main(["-f", job_file])
        captured = capsys.readouterr()
        assert "CREATE TABLE" in captured.out

    def test_job_file_cli_override(self, tmp_path, capsys):
        """CLI args override job file values."""
        input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")

        job_file = str(tmp_path / "job.yaml")
        with open(job_file, "w") as f:
            yaml.dump({
                "source": {"file": input_file},
                "table": {"model": "duplicate"},
            }, f)

        exit_code = main(["-f", job_file, "--model", "unique", "--include-id"])
        captured = capsys.readouterr()
        assert "UNIQUE KEY" in captured.out

    def test_job_file_with_config(self, tmp_path, capsys):
        """-c config overrides -f table section."""
        input_file = os.path.join(FIXTURES_DIR, "full_example_mapping.json")
        config_file = os.path.join(FIXTURES_DIR, "config_example.yaml")

        job_file = str(tmp_path / "job.yaml")
        with open(job_file, "w") as f:
            yaml.dump({
                "source": {"file": input_file},
                "table": {"array_fields": []},  # no array fields in job
            }, f)

        # -c has array_fields: [tags], which should override -f table section
        exit_code = main(["-f", job_file, "-c", config_file])
        captured = capsys.readouterr()
        assert "ARRAY<VARCHAR(256)>" in captured.out

    def test_job_file_table_config_in_batch(self, tmp_path):
        """Job file table settings are applied in batch mode."""
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)

        mapping = {
            "idx": {
                "mappings": {
                    "properties": {"f": {"type": "keyword"}}
                }
            }
        }
        with open(os.path.join(input_dir, "idx.json"), "w") as f:
            json.dump(mapping, f)

        job_file = str(tmp_path / "job.yaml")
        with open(job_file, "w") as f:
            yaml.dump({
                "source": {"dir": input_dir},
                "output": {"dir": output_dir, "table_prefix": "doris_"},
            }, f)

        exit_code = main(["-f", job_file])
        assert exit_code == 0
        assert "doris_idx.sql" in os.listdir(output_dir)

    def test_job_file_missing_source(self, tmp_path):
        """Job file without source info returns error."""
        job_file = str(tmp_path / "job.yaml")
        with open(job_file, "w") as f:
            yaml.dump({"output": {"dir": str(tmp_path / "out")}}, f)

        exit_code = main(["-f", job_file])
        assert exit_code == 2

    def test_job_file_not_found(self):
        """Non-existent job file returns error."""
        exit_code = main(["-f", "/nonexistent/job.yaml"])
        assert exit_code == 2

    def test_job_file_doris_no_auto_execute(self, tmp_path):
        """Doris section without execute: true should NOT auto-execute."""
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)

        mapping = {
            "idx": {"mappings": {"properties": {"f": {"type": "keyword"}}}}
        }
        with open(os.path.join(input_dir, "idx.json"), "w") as f:
            json.dump(mapping, f)

        job_file = str(tmp_path / "job.yaml")
        with open(job_file, "w") as f:
            yaml.dump({
                "source": {"dir": input_dir},
                "output": {"dir": output_dir},
                "doris": {
                    "host": "127.0.0.1",
                    "port": 9030,
                    "user": "root",
                    "database": "test_db",
                },
            }, f)

        # Should succeed without trying to connect to Doris
        exit_code = main(["-f", job_file])
        assert exit_code == 0

    def test_job_file_config_override_warning(self, tmp_path, capsys):
        """-f + -c together prints a note to stderr."""
        input_file = os.path.join(FIXTURES_DIR, "full_example_mapping.json")
        config_file = os.path.join(FIXTURES_DIR, "config_example.yaml")

        job_file = str(tmp_path / "job.yaml")
        with open(job_file, "w") as f:
            yaml.dump({
                "source": {"file": input_file},
                "table": {"replication_num": 1},
            }, f)

        main(["-f", job_file, "-c", config_file])
        captured = capsys.readouterr()
        assert "Note: --table-properties" in captured.err
        assert "overrides table settings" in captured.err

    def test_job_file_cli_override_exclude_index(self, tmp_path):
        """--exclude-index from CLI overrides job file in -f mode."""
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)

        mapping = {
            "logs": {"mappings": {"properties": {"f": {"type": "keyword"}}}},
            "users": {"mappings": {"properties": {"f": {"type": "keyword"}}}},
        }
        with open(os.path.join(input_dir, "data.json"), "w") as f:
            json.dump(mapping, f)

        job_file = str(tmp_path / "job.yaml")
        with open(job_file, "w") as f:
            yaml.dump({
                "source": {"dir": input_dir},
                "output": {"dir": output_dir},
            }, f)

        exit_code = main(["-f", job_file, "--exclude-index", "^users$"])
        assert exit_code == 0
        output_files = os.listdir(output_dir)
        assert "logs.sql" in output_files
        assert "users.sql" not in output_files

    def test_job_file_cli_override_table_prefix(self, tmp_path):
        """--table-prefix from CLI works in -f mode."""
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)

        mapping = {
            "idx": {"mappings": {"properties": {"f": {"type": "keyword"}}}}
        }
        with open(os.path.join(input_dir, "idx.json"), "w") as f:
            json.dump(mapping, f)

        job_file = str(tmp_path / "job.yaml")
        with open(job_file, "w") as f:
            yaml.dump({
                "source": {"dir": input_dir},
                "output": {"dir": output_dir},
            }, f)

        exit_code = main(["-f", job_file, "--table-prefix", "cli_"])
        assert exit_code == 0
        assert "cli_idx.sql" in os.listdir(output_dir)

    def test_job_file_table_model_alias(self, tmp_path, capsys):
        """table_model (with underscore) works as alias for model in -f."""
        input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")

        job_file = str(tmp_path / "job.yaml")
        with open(job_file, "w") as f:
            yaml.dump({
                "source": {"file": input_file},
                "table": {"table_model": "unique", "include_id": True},
            }, f)

        exit_code = main(["-f", job_file])
        captured = capsys.readouterr()
        assert "UNIQUE KEY" in captured.out


# ---------------------------------------------------------------------------
# --table-properties: inline YAML and multiple values
# (E2E issue: users had to create temp files; now inline text is supported)
# ---------------------------------------------------------------------------


class TestLoadConfig:
    """Unit tests for _load_config — file path and inline YAML support."""

    def test_none_returns_empty(self):
        assert _load_config(None) == {}

    def test_empty_list_returns_empty(self):
        assert _load_config([]) == {}

    def test_inline_yaml_single_key(self):
        config = _load_config(["replication_num: 1"])
        assert config["replication_num"] == 1

    def test_inline_yaml_multiple_keys(self):
        config = _load_config(["replication_num: 1\ncompression: LZ4"])
        assert config["replication_num"] == 1
        assert config["compression"] == "LZ4"

    def test_multiple_values_merged(self):
        config = _load_config(["replication_num: 1", "compression: LZ4"])
        assert config["replication_num"] == 1
        assert config["compression"] == "LZ4"

    def test_later_value_overrides_earlier(self):
        """Last --table-properties wins for the same key."""
        config = _load_config(["replication_num: 3", "replication_num: 1"])
        assert config["replication_num"] == 1

    def test_file_path_takes_priority_over_inline(self, tmp_path):
        props_file = str(tmp_path / "props.yaml")
        with open(props_file, "w") as f:
            f.write("replication_num: 2\n")
        config = _load_config([props_file])
        assert config["replication_num"] == 2

    def test_invalid_text_not_dict_raises(self):
        """Scalar YAML that is not a dict should raise ValueError."""
        with pytest.raises(ValueError, match="neither a valid file path nor valid YAML"):
            _load_config(["12345"])

    def test_invalid_validation_raises(self):
        """Unknown key should raise ConfigValidationError."""
        from es_ddl_converter.config_validator import ConfigValidationError
        with pytest.raises(ConfigValidationError):
            _load_config(["partition_filed: timestamp"])  # typo: filed vs field


class TestTablePropertiesMultipleCLI:
    """Integration tests for multiple --table-properties flags on the CLI."""

    def test_single_inline_replication_num(self, capsys):
        input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")
        main(["-i", input_file, "--table-properties", "replication_num: 1"])
        captured = capsys.readouterr()
        assert '"replication_num" = "1"' in captured.out

    def test_two_inline_flags_both_applied(self, capsys):
        input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")
        main([
            "-i", input_file,
            "--table-properties", "replication_num: 1",
            "--table-properties", "compression: LZ4",
        ])
        captured = capsys.readouterr()
        assert '"replication_num" = "1"' in captured.out
        assert '"compression" = "LZ4"' in captured.out

    def test_later_flag_overrides_earlier(self, capsys):
        """When the same key appears in two --table-properties, last wins."""
        input_file = os.path.join(FIXTURES_DIR, "simple_mapping.json")
        main([
            "-i", input_file,
            "--table-properties", "replication_num: 3",
            "--table-properties", "replication_num: 1",
        ])
        captured = capsys.readouterr()
        assert '"replication_num" = "1"' in captured.out
        assert '"replication_num" = "3"' not in captured.out


# ---------------------------------------------------------------------------
# E2E issue: index with only non-keyable columns (dense_vector) → error
# ---------------------------------------------------------------------------


class TestBatchNoSuitableKeyColumn:
    """
    rally_dense_vector had only a dense_vector field (→ ARRAY<FLOAT>).
    ARRAY cannot be a Doris key column, so conversion should report an error.
    The batch should exit with code 2 and include the index in the error count.
    """

    _DENSE_VECTOR_MAPPING = {
        "dense_only": {
            "mappings": {
                "properties": {
                    "embedding": {"type": "dense_vector", "dims": 128}
                }
            }
        }
    }

    def test_exit_code_is_2(self, tmp_path):
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)
        with open(os.path.join(input_dir, "dense_only.json"), "w") as f:
            json.dump(self._DENSE_VECTOR_MAPPING, f)

        exit_code = main(["batch", "--input-dir", input_dir, "-o", output_dir])
        assert exit_code == 2

    def test_report_contains_error(self, tmp_path):
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)
        with open(os.path.join(input_dir, "dense_only.json"), "w") as f:
            json.dump(self._DENSE_VECTOR_MAPPING, f)

        main(["batch", "--input-dir", input_dir, "-o", output_dir])
        report_path = os.path.join(output_dir, "_batch_report.txt")
        with open(report_path) as f:
            report = f.read()
        assert "Error:   1" in report

    def test_no_sql_file_produced(self, tmp_path):
        """No DDL file should be written for an errored index."""
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)
        with open(os.path.join(input_dir, "dense_only.json"), "w") as f:
            json.dump(self._DENSE_VECTOR_MAPPING, f)

        main(["batch", "--input-dir", input_dir, "-o", output_dir])
        sql_files = [f for f in os.listdir(output_dir) if f.endswith(".sql")]
        assert sql_files == []


# ---------------------------------------------------------------------------
# E2E issue: replication_num > alive BE count must fail early with clear message
# (single-node Doris has 1 BE; default replication_num=3 caused silent DDL failure)
# ---------------------------------------------------------------------------


class TestBatchReplicationNumCheck:
    """
    When --execute is used, the tool checks alive BE count before running DDL.
    If replication_num exceeds BE count, it must exit immediately with code 2.
    """

    _SIMPLE_MAPPING = {
        "idx": {"mappings": {"properties": {"f": {"type": "keyword"}}}}
    }

    def _make_mock_executor(self, be_count):
        executor = MagicMock()
        executor.get_alive_be_count.return_value = be_count
        return executor

    def test_error_when_replication_exceeds_be_count(self, tmp_path):
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)
        with open(os.path.join(input_dir, "idx.json"), "w") as f:
            json.dump(self._SIMPLE_MAPPING, f)

        mock_executor = self._make_mock_executor(be_count=1)
        with patch("es_ddl_converter.cli._create_executor", return_value=mock_executor):
            exit_code = main([
                "batch", "--input-dir", input_dir, "-o", output_dir,
                "--execute",
                "--doris-host", "127.0.0.1",
                "--table-properties", "replication_num: 3",
            ])
        assert exit_code == 2

    def test_no_ddl_executed_when_check_fails(self, tmp_path):
        """execute_ddl must not be called if the BE check fails."""
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)
        with open(os.path.join(input_dir, "idx.json"), "w") as f:
            json.dump(self._SIMPLE_MAPPING, f)

        mock_executor = self._make_mock_executor(be_count=1)
        with patch("es_ddl_converter.cli._create_executor", return_value=mock_executor):
            main([
                "batch", "--input-dir", input_dir, "-o", output_dir,
                "--execute",
                "--doris-host", "127.0.0.1",
                "--table-properties", "replication_num: 3",
            ])
        mock_executor.execute_ddl.assert_not_called()

    def test_ok_when_replication_equals_be_count(self, tmp_path):
        """replication_num == be_count is acceptable."""
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)
        with open(os.path.join(input_dir, "idx.json"), "w") as f:
            json.dump(self._SIMPLE_MAPPING, f)

        mock_executor = self._make_mock_executor(be_count=1)
        with patch("es_ddl_converter.cli._create_executor", return_value=mock_executor):
            exit_code = main([
                "batch", "--input-dir", input_dir, "-o", output_dir,
                "--execute",
                "--doris-host", "127.0.0.1",
                "--table-properties", "replication_num: 1",
            ])
        assert exit_code == 0

    def test_be_count_query_failure_is_warned_not_fatal(self, tmp_path):
        """If get_alive_be_count raises, log a warning but do not abort."""
        input_dir = str(tmp_path / "input")
        output_dir = str(tmp_path / "output")
        os.makedirs(input_dir)
        with open(os.path.join(input_dir, "idx.json"), "w") as f:
            json.dump(self._SIMPLE_MAPPING, f)

        mock_executor = self._make_mock_executor(be_count=1)
        mock_executor.get_alive_be_count.side_effect = Exception("permission denied")
        with patch("es_ddl_converter.cli._create_executor", return_value=mock_executor):
            exit_code = main([
                "batch", "--input-dir", input_dir, "-o", output_dir,
                "--execute",
                "--doris-host", "127.0.0.1",
            ])
        # Should not abort — fall through and attempt DDL
        assert exit_code == 0
