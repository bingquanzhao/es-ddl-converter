"""Validate job YAML (-f) and table config YAML (-c) files.

Validation collects ALL errors before raising, so users see the full list
in one pass rather than fixing one mistake at a time.
"""

from typing import Any, Dict, List


class ConfigValidationError(Exception):
    """Raised when a config file fails validation."""

    def __init__(self, errors):
        # type: (List[str]) -> None
        self.errors = errors
        super(ConfigValidationError, self).__init__(
            "Config validation failed:\n" + "\n".join("  - " + e for e in errors)
        )


# ---------------------------------------------------------------------------
# Allowed keys per section
# ---------------------------------------------------------------------------

_ALLOWED_JOB_TOP = {"source", "output", "doris", "table", "exclude", "fail_fast", "warnings_only"}

_ALLOWED_SOURCE = {"url", "user", "password", "index", "verify_ssl", "dir", "file"}

_ALLOWED_OUTPUT = {"dir", "file", "table_prefix", "table_name"}

_ALLOWED_DORIS = {"execute", "host", "port", "user", "password", "database"}

_ALLOWED_TABLE = {
    "model", "table_model", "include_id", "replication_num",
    "ip_type", "array_fields", "key_columns", "partition_field", "bucket_strategy",
    "compression",
}

_VALID_COMPRESSION = {"NO_COMPRESSION", "LZ4", "LZ4F", "ZLIB", "ZSTD", "SNAPPY"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unknown_keys(data, allowed, section):
    # type: (Dict[str, Any], set, str) -> List[str]
    return [
        "{}: unknown key '{}' (allowed: {})".format(
            section, k, ", ".join(sorted(allowed))
        )
        for k in data if k not in allowed
    ]


def _validate_table_section(table, section):
    # type: (Dict[str, Any], str) -> List[str]
    errors = _unknown_keys(table, _ALLOWED_TABLE, section)

    model = table.get("model") or table.get("table_model")
    if model is not None and model not in ("duplicate", "unique"):
        errors.append(
            "{}.model: must be 'duplicate' or 'unique', got '{}'".format(section, model)
        )

    if "replication_num" in table:
        v = table["replication_num"]
        if not isinstance(v, int) or v < 1:
            errors.append(
                "{}.replication_num: must be a positive integer, got '{}'".format(section, v)
            )

    if "ip_type" in table:
        v = table["ip_type"]
        if str(v).lower() not in ("ipv4", "ipv6"):
            errors.append(
                "{}.ip_type: must be 'ipv4' or 'ipv6', got '{}'".format(section, v)
            )

    if "array_fields" in table:
        v = table["array_fields"]
        if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
            errors.append("{}.array_fields: must be a list of strings".format(section))

    if "key_columns" in table:
        v = table["key_columns"]
        if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
            errors.append("{}.key_columns: must be a list of strings".format(section))

    if "include_id" in table:
        if not isinstance(table["include_id"], bool):
            errors.append("{}.include_id: must be true or false".format(section))

    if "compression" in table:
        v = str(table["compression"]).upper()
        if v not in _VALID_COMPRESSION:
            errors.append(
                "{}.compression: must be one of {}, got '{}'".format(
                    section, ", ".join(sorted(_VALID_COMPRESSION)), table["compression"]
                )
            )

    return errors


# ---------------------------------------------------------------------------
# Public validators
# ---------------------------------------------------------------------------

def validate_job_file(job):
    # type: (Dict[str, Any]) -> None
    """Validate a job YAML file (-f). Raises ConfigValidationError if invalid."""
    errors = _unknown_keys(job, _ALLOWED_JOB_TOP, "job")

    source = job.get("source", {})
    if isinstance(source, dict):
        errors.extend(_unknown_keys(source, _ALLOWED_SOURCE, "source"))
        defined = [k for k in ("url", "dir", "file") if k in source]
        if len(defined) > 1:
            errors.append(
                "source: only one of 'url', 'dir', 'file' may be specified, got: {}".format(
                    defined
                )
            )
    elif source is not None:
        errors.append("source: must be a mapping")

    output = job.get("output", {})
    if isinstance(output, dict):
        errors.extend(_unknown_keys(output, _ALLOWED_OUTPUT, "output"))
    elif output is not None:
        errors.append("output: must be a mapping")

    doris = job.get("doris", {})
    if isinstance(doris, dict):
        errors.extend(_unknown_keys(doris, _ALLOWED_DORIS, "doris"))
        if "port" in doris and not isinstance(doris["port"], int):
            errors.append(
                "doris.port: must be an integer, got '{}'".format(doris["port"])
            )
        if "execute" in doris and not isinstance(doris["execute"], bool):
            errors.append("doris.execute: must be true or false")
    elif doris is not None:
        errors.append("doris: must be a mapping")

    table = job.get("table", {})
    if isinstance(table, dict):
        errors.extend(_validate_table_section(table, "table"))
    elif table is not None:
        errors.append("table: must be a mapping")

    for bool_key in ("fail_fast", "warnings_only"):
        if bool_key in job and not isinstance(job[bool_key], bool):
            errors.append("{}: must be true or false".format(bool_key))

    if errors:
        raise ConfigValidationError(errors)


def validate_table_config(config):
    # type: (Dict[str, Any]) -> None
    """Validate a table config YAML (-c). Raises ConfigValidationError if invalid."""
    errors = _validate_table_section(config, "config")
    if errors:
        raise ConfigValidationError(errors)
