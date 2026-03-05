"""Warning collection system for conversion diagnostics."""

import enum
from dataclasses import dataclass, field
from typing import List, Optional


class Severity(enum.Enum):
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"


@dataclass
class ConversionWarning:
    severity: Severity
    field_path: str
    message: str
    es_type: Optional[str] = None

    def format(self):
        # type: () -> str
        prefix = "[{}]".format(self.severity.value)
        field_info = " field='{}':".format(self.field_path) if self.field_path else ":"
        return "{}{} {}".format(prefix, field_info, self.message)


@dataclass
class WarningCollector:
    """Collects warnings during the conversion process."""

    warnings: List[ConversionWarning] = field(default_factory=list)

    def error(self, field_path, message, es_type=None):
        # type: (str, str, Optional[str]) -> None
        self.warnings.append(ConversionWarning(
            severity=Severity.ERROR,
            field_path=field_path,
            message=message,
            es_type=es_type,
        ))

    def warn(self, field_path, message, es_type=None):
        # type: (str, str, Optional[str]) -> None
        self.warnings.append(ConversionWarning(
            severity=Severity.WARN,
            field_path=field_path,
            message=message,
            es_type=es_type,
        ))

    def info(self, field_path, message, es_type=None):
        # type: (str, str, Optional[str]) -> None
        self.warnings.append(ConversionWarning(
            severity=Severity.INFO,
            field_path=field_path,
            message=message,
            es_type=es_type,
        ))

    def has_errors(self):
        # type: () -> bool
        return any(w.severity == Severity.ERROR for w in self.warnings)

    def get_by_severity(self, severity):
        # type: (Severity) -> List[ConversionWarning]
        return [w for w in self.warnings if w.severity == severity]

    def format_report(self):
        # type: () -> str
        """Format all warnings as a human-readable report, grouped by severity."""
        lines = []  # type: List[str]
        for sev in (Severity.ERROR, Severity.WARN, Severity.INFO):
            group = self.get_by_severity(sev)
            if group:
                lines.append("")
                lines.append("--- {} ({}) ---".format(sev.value, len(group)))
                for w in group:
                    lines.append("  " + w.format())
        return "\n".join(lines) if lines else "No warnings."
