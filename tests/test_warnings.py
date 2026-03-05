"""Tests for warnings module."""

from es_ddl_converter.warnings import ConversionWarning, Severity, WarningCollector


def test_severity_values():
    assert Severity.ERROR.value == "ERROR"
    assert Severity.WARN.value == "WARN"
    assert Severity.INFO.value == "INFO"


def test_warning_format():
    w = ConversionWarning(Severity.WARN, "user.name", "some message", es_type="keyword")
    assert "[WARN]" in w.format()
    assert "user.name" in w.format()
    assert "some message" in w.format()


def test_warning_format_no_field():
    w = ConversionWarning(Severity.ERROR, "", "global error")
    formatted = w.format()
    assert "[ERROR]" in formatted


def test_collector_error(collector):
    collector.error("f1", "bad")
    assert collector.has_errors()
    assert len(collector.warnings) == 1
    assert collector.warnings[0].severity == Severity.ERROR


def test_collector_warn(collector):
    collector.warn("f1", "warning")
    assert not collector.has_errors()
    assert len(collector.warnings) == 1


def test_collector_info(collector):
    collector.info("f1", "info")
    assert not collector.has_errors()


def test_get_by_severity(collector):
    collector.error("f1", "err")
    collector.warn("f2", "wrn")
    collector.info("f3", "inf")
    assert len(collector.get_by_severity(Severity.ERROR)) == 1
    assert len(collector.get_by_severity(Severity.WARN)) == 1
    assert len(collector.get_by_severity(Severity.INFO)) == 1


def test_format_report_empty(collector):
    assert collector.format_report() == "No warnings."


def test_format_report_with_warnings(collector):
    collector.error("f1", "err msg")
    collector.warn("f2", "wrn msg")
    report = collector.format_report()
    assert "ERROR" in report
    assert "WARN" in report
    assert "err msg" in report
    assert "wrn msg" in report
