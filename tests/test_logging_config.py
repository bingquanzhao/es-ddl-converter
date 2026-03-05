"""Tests for CLI logging configuration (_CliFormatter and _setup_logging)."""

import logging

import pytest

from es_ddl_converter.cli import _CliFormatter, _setup_logging, build_parser


@pytest.fixture(autouse=True)
def reset_pkg_logger():
    """Restore package logger state before and after each test."""
    pkg = logging.getLogger("es_ddl_converter")
    original_handlers = pkg.handlers[:]
    original_level = pkg.level
    original_propagate = pkg.propagate
    yield
    pkg.handlers[:] = original_handlers
    pkg.setLevel(original_level)
    pkg.propagate = original_propagate


# ---------------------------------------------------------------------------
# _CliFormatter
# ---------------------------------------------------------------------------

class TestCliFormatter:
    def _make_record(self, level: int, message: str) -> logging.LogRecord:
        return logging.LogRecord("test", level, "", 0, message, [], None)

    def test_info_is_plain_text(self):
        formatter = _CliFormatter()
        record = self._make_record(logging.INFO, "processing index logs-2024")
        assert formatter.format(record) == "processing index logs-2024"

    def test_error_has_level_prefix(self):
        formatter = _CliFormatter()
        record = self._make_record(logging.ERROR, "connection refused")
        assert formatter.format(record) == "ERROR: connection refused"

    def test_warning_has_level_prefix(self):
        formatter = _CliFormatter()
        record = self._make_record(logging.WARNING, "type degraded to VARIANT")
        assert formatter.format(record) == "WARNING: type degraded to VARIANT"

    def test_debug_has_level_prefix(self):
        formatter = _CliFormatter()
        record = self._make_record(logging.DEBUG, "fetching _mapping")
        assert formatter.format(record) == "DEBUG: fetching _mapping"

    def test_info_does_not_contain_level_word(self):
        formatter = _CliFormatter()
        record = self._make_record(logging.INFO, "done")
        result = formatter.format(record)
        assert "INFO" not in result
        assert result == "done"


# ---------------------------------------------------------------------------
# _setup_logging
# ---------------------------------------------------------------------------

class TestSetupLogging:
    def test_default_level_is_info(self):
        _setup_logging()
        assert logging.getLogger("es_ddl_converter").level == logging.INFO

    def test_verbose_sets_debug_level(self):
        _setup_logging(verbose=True)
        assert logging.getLogger("es_ddl_converter").level == logging.DEBUG

    def test_quiet_sets_error_level(self):
        _setup_logging(quiet=True)
        assert logging.getLogger("es_ddl_converter").level == logging.ERROR

    def test_adds_exactly_one_handler(self):
        _setup_logging()
        assert len(logging.getLogger("es_ddl_converter").handlers) == 1

    def test_repeated_calls_no_duplicate_handlers(self):
        _setup_logging()
        _setup_logging()
        _setup_logging()
        assert len(logging.getLogger("es_ddl_converter").handlers) == 1

    def test_does_not_propagate_to_root(self):
        _setup_logging()
        assert logging.getLogger("es_ddl_converter").propagate is False

    def test_handler_outputs_to_stderr(self):
        import sys
        _setup_logging()
        handler = logging.getLogger("es_ddl_converter").handlers[0]
        assert isinstance(handler, logging.StreamHandler)
        assert handler.stream is sys.stderr

    def test_handler_uses_cli_formatter(self):
        _setup_logging()
        handler = logging.getLogger("es_ddl_converter").handlers[0]
        assert isinstance(handler.formatter, _CliFormatter)

    def test_submodule_loggers_inherit_level(self):
        _setup_logging(verbose=True)
        # Child loggers use effective level from the package logger
        child = logging.getLogger("es_ddl_converter.cli")
        assert child.getEffectiveLevel() == logging.DEBUG


# ---------------------------------------------------------------------------
# --verbose / --quiet CLI flags
# ---------------------------------------------------------------------------

class TestVerbosityFlags:
    def test_verbose_flag_parsed(self):
        args = build_parser().parse_args(["--verbose", "-i", "x.json"])
        assert args.verbose is True
        assert args.quiet is False

    def test_quiet_flag_parsed(self):
        args = build_parser().parse_args(["--quiet", "-i", "x.json"])
        assert args.quiet is True
        assert args.verbose is False

    def test_short_v_flag(self):
        args = build_parser().parse_args(["-v", "-i", "x.json"])
        assert args.verbose is True

    def test_short_q_flag(self):
        args = build_parser().parse_args(["-q", "-i", "x.json"])
        assert args.quiet is True

    def test_verbose_and_quiet_are_mutually_exclusive(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["--verbose", "--quiet", "-i", "x.json"])

    def test_default_both_false(self):
        args = build_parser().parse_args(["-i", "x.json"])
        assert args.verbose is False
        assert args.quiet is False
