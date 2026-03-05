"""Batch conversion orchestrator: process multiple ES indexes in one run."""

import datetime
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Pattern

logger = logging.getLogger(__name__)

from .ddl_renderer import render_ddl
from .index_strategy import determine_indexes
from .mapping_parser import parse_mapping
from .table_builder import build_table
from .warnings import ConversionWarning, WarningCollector


@dataclass
class IndexResult:
    """Result of processing one index."""

    index_name: str
    table_name: str
    status: str  # "ok", "warning", "error", "skipped"
    ddl: Optional[str] = None
    output_path: Optional[str] = None
    warnings: List[ConversionWarning] = field(default_factory=list)
    error_message: Optional[str] = None
    executed: bool = False


@dataclass
class BatchResult:
    """Aggregated result of a batch run."""

    total: int = 0
    ok: int = 0
    warnings: int = 0
    errors: int = 0
    skipped: int = 0
    results: List[IndexResult] = field(default_factory=list)


def _sanitize_table_name(name):
    # type: (str) -> str
    """Convert an ES index name to a valid Doris table name.

    Doris backtick-quoted identifiers accept any character except backtick
    and NUL.  ES index names already forbid backtick, so a valid ES name
    is almost always a valid Doris identifier.  We only strip characters
    that Doris truly cannot handle.
    """
    name = name.replace("`", "_").replace("\x00", "")
    return name or "unnamed"


def convert_one_index(
    index_name,     # type: str
    mapping_body,   # type: Dict[str, Any]
    config,         # type: Dict[str, Any]
    table_name_prefix="",   # type: str
    table_model="duplicate",  # type: str
    include_id=False,  # type: bool
):
    # type: (...) -> IndexResult
    """Run the full conversion pipeline for a single index.

    Catches all exceptions and returns an :class:`IndexResult` with
    ``status="error"`` rather than propagating.
    """
    table_name = "{}{}".format(table_name_prefix, _sanitize_table_name(index_name))
    try:
        collector = WarningCollector()
        array_fields = set(config.get("array_fields", []))
        ip_type = config.get("ip_type", "ipv6")
        key_columns = config.get("key_columns")
        partition_field = config.get("partition_field")
        bucket_strategy = config.get("bucket_strategy", "random")
        replication_num = config.get("replication_num", 3)
        compression = config.get("compression", "ZSTD")

        # Wrap into single-index format for parse_mapping
        raw_json = {index_name: {"mappings": mapping_body}}

        parsed = parse_mapping(
            raw_json=raw_json,
            collector=collector,
            array_fields=array_fields,
            ip_type=ip_type,
            include_id=include_id,
        )

        indexes = determine_indexes(parsed.columns, collector)

        table_def = build_table(
            table_name=table_name,
            columns=parsed.columns,
            indexes=indexes,
            collector=collector,
            table_model=table_model,
            key_columns=key_columns,
            partition_field=partition_field,
            bucket_strategy=bucket_strategy,
            replication_num=replication_num,
            compression=compression,
        )

        ddl = render_ddl(table_def)

        status = "warning" if collector.warnings else "ok"
        if collector.has_errors():
            status = "error"

        return IndexResult(
            index_name=index_name,
            table_name=table_name,
            status=status,
            ddl=ddl,
            warnings=list(collector.warnings),
        )
    except Exception as e:
        return IndexResult(
            index_name=index_name,
            table_name=table_name,
            status="error",
            error_message=str(e),
        )


def run_batch(
    index_mapping_slices,  # type: Dict[str, Dict[str, Any]]
    config,                # type: Dict[str, Any]
    output_dir,            # type: str
    exclude_pattern=None,  # type: Optional[str]
    table_name_prefix="",  # type: str
    table_model="duplicate",  # type: str
    include_id=False,      # type: bool
    executor=None,         # type: Any  # Optional[DorisExecutor]
    fail_fast=False,       # type: bool
    warnings_only=False,   # type: bool
):
    # type: (...) -> BatchResult
    """Process all indexes in a batch.

    Args:
        index_mapping_slices: ``{index_name: mapping_body}`` from
            :func:`~es_ddl_converter.mapping_parser.extract_all_mappings`.
        config: YAML config dict (applied to all indexes).
        output_dir: Directory to write ``.sql`` files.
        exclude_pattern: Regex; indexes whose names match are skipped.
        table_name_prefix: Prefix prepended to all table names.
        table_model: ``"duplicate"`` or ``"unique"``.
        include_id: Whether to add ``_id`` column.
        executor: Optional :class:`DorisExecutor` for live execution.
        fail_fast: Stop on first error.
        warnings_only: Do not write files or execute.

    Returns:
        :class:`BatchResult` with per-index details.
    """
    exclude_re = re.compile(exclude_pattern) if exclude_pattern else None

    batch = BatchResult()

    for index_name in sorted(index_mapping_slices.keys()):
        batch.total += 1

        # --- Filter ---
        if exclude_re and exclude_re.search(index_name):
            batch.skipped += 1
            batch.results.append(IndexResult(
                index_name=index_name,
                table_name="",
                status="skipped",
            ))
            continue

        # --- Convert ---
        result = convert_one_index(
            index_name=index_name,
            mapping_body=index_mapping_slices[index_name],
            config=config,
            table_name_prefix=table_name_prefix,
            table_model=table_model,
            include_id=include_id,
        )

        # --- Write output ---
        if not warnings_only and result.ddl is not None and result.status != "error":
            os.makedirs(output_dir, exist_ok=True)
            out_path = os.path.join(output_dir, "{}.sql".format(result.table_name))
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(result.ddl)
                f.write("\n")
            result.output_path = out_path

        # --- Execute on Doris ---
        if executor is not None and result.ddl is not None and result.status != "error":
            try:
                executor.execute_ddl(result.ddl)
                result.executed = True
            except Exception as e:
                result.status = "error"
                result.error_message = "Doris execution failed: {}".format(e)

        # --- Accumulate ---
        if result.status == "ok":
            batch.ok += 1
        elif result.status == "warning":
            batch.warnings += 1
        elif result.status == "error":
            batch.errors += 1

        batch.results.append(result)

        if fail_fast and result.status == "error":
            # Count remaining as skipped
            remaining = len(index_mapping_slices) - batch.total
            batch.skipped += remaining
            batch.total += remaining
            break

    return batch


def format_batch_report(result, output_dir=None):
    # type: (BatchResult, Optional[str]) -> str
    """Format a human-readable batch summary report."""
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    success = result.ok + result.warnings
    lines = [
        "=" * 60,
        "Batch Conversion Summary  {}".format(now),
        "=" * 60,
        "Total:    {}".format(result.total),
        "  Success: {}  ({} with warnings)".format(success, result.warnings),
        "  Error:   {}".format(result.errors),
        "  Skip:    {}".format(result.skipped),
    ]
    if output_dir:
        lines.append("")
        lines.append("Output: {}".format(os.path.abspath(output_dir)))

    # Errors
    error_results = [r for r in result.results if r.status == "error"]
    if error_results:
        lines.append("")
        lines.append("--- ERRORS ({}) ---".format(len(error_results)))
        for r in error_results:
            msg = r.error_message or "conversion errors"
            lines.append("  [ERROR] index='{}': {}".format(r.index_name, msg))

    # Warnings
    warn_results = [r for r in result.results if r.status == "warning"]
    if warn_results:
        lines.append("")
        lines.append("--- WARNINGS ({} indexes) ---".format(len(warn_results)))
        for r in warn_results:
            lines.append("  index='{}': {} warning(s)".format(
                r.index_name, len(r.warnings)
            ))
            for w in r.warnings:
                lines.append("    [{}] field='{}': {}".format(
                    w.severity.name, w.field_path, w.message
                ))

    lines.append("=" * 60)
    return "\n".join(lines)
