"""CLI entry point for es-ddl-converter."""

import argparse
import contextlib
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

import yaml

from . import __version__
from .config_validator import ConfigValidationError, validate_job_file, validate_table_config
from .ddl_renderer import render_ddl
from .index_strategy import determine_indexes
from .mapping_parser import parse_mapping
from .table_builder import build_table
from .warnings import WarningCollector

logger = logging.getLogger(__name__)


class _CliFormatter(logging.Formatter):
    """CLI-friendly formatter: INFO is plain text, others show level prefix."""

    def format(self, record: logging.LogRecord) -> str:
        if record.levelno == logging.INFO:
            return record.getMessage()
        return "{}: {}".format(record.levelname, record.getMessage())


def _setup_logging(verbose: bool = False, quiet: bool = False) -> None:
    """Configure package-level logging for CLI use.

    Configures the 'es_ddl_converter' logger hierarchy only; does not affect
    other libraries. Safe to call multiple times (clears existing handlers).
    """
    if verbose:
        level = logging.DEBUG
    elif quiet:
        level = logging.ERROR
    else:
        level = logging.INFO

    pkg_logger = logging.getLogger("es_ddl_converter")
    pkg_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_CliFormatter())
    pkg_logger.addHandler(handler)
    pkg_logger.setLevel(level)
    pkg_logger.propagate = False


def _load_json(path):
    # type: (str) -> Dict[str, Any]
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_config(values):
    # type: (Optional[list]) -> Dict[str, Any]
    """Load and merge one or more --table-properties values (file paths or inline YAML)."""
    if not values:
        return {}
    merged = {}  # type: Dict[str, Any]
    for value in values:
        try:
            with open(value, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except (FileNotFoundError, OSError):
            data = yaml.safe_load(value) or {}
            if not isinstance(data, dict):
                raise ValueError(
                    "--table-properties value is neither a valid file path nor valid YAML: {!r}".format(value)
                )
        validate_table_config(data)
        merged.update(data)
    return merged


def _load_job_file(path):
    # type: (str) -> Dict[str, Any]
    """Load a job YAML file (-f)."""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    validate_job_file(data)
    return data


def _merge_job_into_args(args, job):
    # type: (argparse.Namespace, Dict[str, Any]) -> None
    """Merge job file values into *args*, only filling CLI-defaulted fields."""
    # Ensure batch-specific attributes exist with defaults
    _BATCH_DEFAULTS = {
        "es_url": None,
        "es_user": None,
        "es_password": None,
        "es_index": "*",
        "no_verify_ssl": False,
        "input_dir": None,
        "output_dir": None,
        "table_prefix": "",
        "exclude_index": None,
        "execute": False,
        "doris_host": "127.0.0.1",
        "doris_port": 9030,
        "doris_user": "root",
        "doris_password": "",
        "doris_db": None,
        "fail_fast": False,
    }
    for attr, default in _BATCH_DEFAULTS.items():
        if not hasattr(args, attr):
            setattr(args, attr, default)
        elif getattr(args, attr) is None and default is not None:
            # Top-level hidden args default to None; normalize to the
            # real default (e.g. table_prefix "" instead of None).
            setattr(args, attr, default)

    def _set(attr, value, default=None):
        """Set *attr* on args only when current value equals *default*.

        Also treats None as "not set" so that top-level hidden args
        (which default to None) are properly filled.
        """
        current = getattr(args, attr, default)
        if current is None or current == default:
            setattr(args, attr, value)

    source = job.get("source", {})
    output = job.get("output", {})
    doris = job.get("doris", {})
    table = job.get("table", {})

    # ── Source ──
    if "url" in source:
        _set("es_url", source["url"])
    if "user" in source:
        _set("es_user", source["user"])
    if "password" in source:
        _set("es_password", source["password"])
    if "index" in source:
        _set("es_index", source["index"], "*")
    if "verify_ssl" in source:
        _set("no_verify_ssl", not source["verify_ssl"], False)
    if "dir" in source:
        _set("input_dir", source["dir"])
    if "file" in source:
        _set("input", source["file"])

    # ── Exclude ──
    if "exclude" in job:
        _set("exclude_index", job["exclude"])

    # ── Output ──
    if "dir" in output:
        _set("output_dir", output["dir"])
    if "file" in output:
        _set("output", output["file"])
    if "table_prefix" in output:
        _set("table_prefix", output["table_prefix"], "")
    if "table_name" in output:
        _set("table_name", output["table_name"])

    # ── Doris ──
    if doris:
        if doris.get("execute", False):
            _set("execute", True, False)
        if "host" in doris:
            _set("doris_host", doris["host"], "127.0.0.1")
        if "port" in doris:
            _set("doris_port", doris["port"], 9030)
        if "user" in doris:
            _set("doris_user", doris["user"], "root")
        if "password" in doris:
            _set("doris_password", doris["password"], "")
        if "database" in doris:
            _set("doris_db", doris["database"])

    # ── Behavior ──
    if "fail_fast" in job:
        _set("fail_fast", job["fail_fast"], False)
    if "warnings_only" in job:
        _set("warnings_only", job["warnings_only"], False)

    # ── Table (model/include_id also exposed as CLI args) ──
    # Accept both 'model' and 'table_model' as key names
    if "model" in table:
        _set("model", table["model"])
    elif "table_model" in table:
        _set("model", table["table_model"])
    if "include_id" in table:
        _set("include_id", table["include_id"], False)

    # Build table config dict for the conversion pipeline.
    # Rename 'model' → 'table_model' to match existing config key convention.
    table_config = {}
    for key, value in table.items():
        if key == "model":
            table_config["table_model"] = value
        else:
            table_config[key] = value
    args._job_table_config = table_config


def _add_common_args(parser):
    # type: (argparse.ArgumentParser) -> None
    """Add arguments common to both convert and batch subcommands."""
    parser.add_argument(
        "-c", "--table-properties",
        action="append",
        default=None,
        dest="config",
        metavar="PROPS",
        help="YAML file path or inline YAML string (e.g. 'replication_num: 1'). Can be repeated.",
    )
    parser.add_argument(
        "--model",
        choices=["duplicate", "unique"],
        default=None,
        help="Table model (default: duplicate)",
    )
    parser.add_argument(
        "--include-id",
        action="store_true",
        default=False,
        help="Add _id VARCHAR(128) column",
    )
    parser.add_argument(
        "--warnings-only",
        action="store_true",
        default=False,
        help="Only print warnings, do not generate DDL",
    )


def build_parser():
    # type: () -> argparse.ArgumentParser
    parser = argparse.ArgumentParser(
        prog="es-ddl-converter",
        description="Convert Elasticsearch index mappings to Apache Doris CREATE TABLE DDL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Single file (legacy mode)
  es-ddl-converter -i mapping.json
  es-ddl-converter -i mapping.json --table-properties config.yaml -o create_table.sql

  # Single file (explicit subcommand)
  es-ddl-converter convert -i mapping.json --model unique

  # Batch from ES cluster
  es-ddl-converter batch --es-url http://localhost:9200 -o output_dir/

  # Batch from directory
  es-ddl-converter batch --input-dir mappings/ -o output_dir/
""",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s {}".format(__version__),
    )
    parser.add_argument(
        "-f", "--job-file",
        default=None,
        help="Path to job YAML file (all-in-one configuration)",
    )

    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "--verbose", "-v",
        action="store_true",
        default=False,
        help="Enable debug output",
    )
    verbosity.add_argument(
        "--quiet", "-q",
        action="store_true",
        default=False,
        help="Suppress all output except errors",
    )

    # --- Legacy top-level arguments (for backward compat) ---
    parser.add_argument("-i", "--input", default=None, help=argparse.SUPPRESS)
    parser.add_argument("-c", "--table-properties", dest="config", action="append", default=None, help=argparse.SUPPRESS)
    parser.add_argument("-o", "--output", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--table-name", default=None, help=argparse.SUPPRESS)
    parser.add_argument(
        "--model", choices=["duplicate", "unique"], default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--include-id", action="store_true", default=False,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--warnings-only", action="store_true", default=False,
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--exclude-index", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--table-prefix", default=None, help=argparse.SUPPRESS)
    parser.add_argument(
        "--fail-fast", action="store_true", default=False,
        help=argparse.SUPPRESS,
    )

    subparsers = parser.add_subparsers(dest="subcommand")

    # --- convert subcommand ---
    convert_p = subparsers.add_parser(
        "convert",
        help="Convert a single ES mapping file to Doris DDL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    convert_p.add_argument(
        "-i", "--input", required=True,
        help="Path to ES mapping JSON file",
    )
    convert_p.add_argument(
        "-o", "--output", default=None,
        help="Output file path (default: stdout)",
    )
    convert_p.add_argument(
        "--table-name", default=None,
        help="Override the table name (default: from ES index name)",
    )
    _add_common_args(convert_p)

    # --- batch subcommand ---
    batch_p = subparsers.add_parser(
        "batch",
        help="Batch convert multiple ES indexes to Doris DDL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Input source (mutually exclusive)
    input_group = batch_p.add_argument_group("input source (one required)")
    source = input_group.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--es-url",
        help="Elasticsearch cluster URL (e.g., http://localhost:9200)",
    )
    source.add_argument(
        "--input-dir",
        help="Directory containing ES mapping JSON files",
    )

    # ES options
    es_group = batch_p.add_argument_group("ES options")
    es_group.add_argument(
        "--es-index", default="*",
        help="Index pattern (default: *)",
    )
    es_group.add_argument("--es-user", default=None, help="ES HTTP Basic username")
    es_group.add_argument("--es-password", default=None, help="ES HTTP Basic password")
    es_group.add_argument(
        "--no-verify-ssl", action="store_true", default=False,
        help="Disable SSL certificate verification",
    )

    # Output
    batch_p.add_argument(
        "-o", "--output-dir", required=True,
        help="Output directory (one .sql per index)",
    )
    batch_p.add_argument(
        "--table-prefix", default="",
        help="Prefix prepended to all table names",
    )

    # Filter
    batch_p.add_argument(
        "--exclude-index", default=None,
        help="Regex pattern; matching index names are skipped",
    )

    # Common
    _add_common_args(batch_p)

    # Doris execution
    exec_group = batch_p.add_argument_group("Doris execution")
    exec_group.add_argument(
        "--execute", action="store_true", default=False,
        help="Execute generated DDL on Doris after generation",
    )
    exec_group.add_argument("--doris-host", default="127.0.0.1", help="Doris FE host")
    exec_group.add_argument("--doris-port", type=int, default=9030, help="Doris FE MySQL port")
    exec_group.add_argument("--doris-user", default="root", help="Doris user")
    exec_group.add_argument("--doris-password", default="", help="Doris password")
    exec_group.add_argument("--doris-db", default=None, help="Doris target database")

    # Behavior
    batch_p.add_argument(
        "--fail-fast", action="store_true", default=False,
        help="Stop on first error",
    )

    return parser


# ---------------------------------------------------------------------------
# convert (single file) mode
# ---------------------------------------------------------------------------

def _main_convert(args):
    # type: (argparse.Namespace) -> int
    """Run single-file conversion. Returns exit code."""
    # Load inputs
    try:
        raw_json = _load_json(args.input)
    except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
        logger.error("Error reading mapping file: %s", e)
        return 2

    config = getattr(args, "_job_table_config", {}).copy()
    try:
        file_config = _load_config(args.config)
    except ConfigValidationError as e:
        logger.error("%s", e)
        return 2
    except (FileNotFoundError, IOError) as e:
        logger.error("Error reading config file: %s", e)
        return 2
    if config and file_config:
        logger.info("Note: --table-properties overrides table settings from job file")
    config.update(file_config)  # -c overrides -f table section

    # Merge CLI args with config (CLI takes precedence)
    array_fields = set(config.get("array_fields", []))
    table_model = (args.model
                   or config.get("table_model")
                   or config.get("model", "duplicate"))
    key_columns = config.get("key_columns")
    partition_field = config.get("partition_field")
    bucket_strategy = config.get("bucket_strategy", "random")
    replication_num = config.get("replication_num", 3)
    compression = config.get("compression", "ZSTD")
    ip_type = config.get("ip_type", "ipv6")
    include_id = args.include_id or config.get("include_id", False)
    table_name_override = args.table_name or config.get("table_name_override")

    # Pipeline
    collector = WarningCollector()

    try:
        parsed = parse_mapping(
            raw_json=raw_json,
            collector=collector,
            array_fields=array_fields,
            ip_type=ip_type,
            include_id=include_id,
        )
    except ValueError as e:
        logger.error("Error parsing mapping: %s", e)
        return 2

    table_name = table_name_override or parsed.index_name

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

    # Output
    if not args.warnings_only:
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(ddl)
                f.write("\n")
            logger.info("DDL written to %s", args.output)
        else:
            sys.stdout.write(ddl)
            sys.stdout.write("\n")

    # Always emit conversion diagnostics
    if collector.warnings:
        if collector.has_errors():
            logger.error(collector.format_report())
        else:
            logger.warning(collector.format_report())

    if collector.has_errors():
        return 2
    if collector.warnings:
        return 1
    return 0


# ---------------------------------------------------------------------------
# batch mode
# ---------------------------------------------------------------------------

def _main_batch(args):
    # type: (argparse.Namespace) -> int
    """Run batch conversion. Returns exit code."""
    from .batch import format_batch_report, run_batch
    from .mapping_parser import extract_all_mappings

    config = getattr(args, "_job_table_config", {}).copy()
    try:
        file_config = _load_config(args.config)
    except ConfigValidationError as e:
        logger.error("%s", e)
        return 2
    except (FileNotFoundError, IOError) as e:
        logger.error("Error reading config file: %s", e)
        return 2
    if config and file_config:
        logger.info("Note: --table-properties overrides table settings from job file")
    config.update(file_config)  # -c overrides -f table section

    table_model = (args.model
                   or config.get("table_model")
                   or config.get("model", "duplicate"))
    include_id = args.include_id or config.get("include_id", False)

    # --- Gather index mappings ---
    if args.es_url:
        index_slices = _batch_from_es(args)
    else:
        index_slices = _batch_from_dir(args.input_dir)

    if index_slices is None:
        return 2  # error already printed

    if not index_slices:
        logger.error("No index mappings found.")
        return 2

    logger.info("Found %d index(es) to process.", len(index_slices))

    # --- Doris executor (optional) ---
    executor = None
    if args.execute:
        executor = _create_executor(args)
        if executor is None:
            return 2
        # Check replication_num against actual BE count to avoid build-time failure
        replication_num = config.get("replication_num", 3)
        try:
            be_count = executor.get_alive_be_count()
            if be_count > 0 and replication_num > be_count:
                logger.error(
                    "replication_num=%d exceeds alive BE count (%d). "
                    "Set replication_num <= %d via --table-properties \"replication_num: %d\".",
                    replication_num, be_count, be_count, be_count,
                )
                return 2
        except Exception as e:
            logger.warning("Could not verify BE count: %s", e)

    # --- Run batch ---
    with (executor if executor is not None else contextlib.nullcontext()):
        result = run_batch(
            index_mapping_slices=index_slices,
            config=config,
            output_dir=args.output_dir,
            exclude_pattern=args.exclude_index,
            table_name_prefix=args.table_prefix,
            table_model=table_model,
            include_id=include_id,
            executor=executor,
            fail_fast=args.fail_fast,
            warnings_only=args.warnings_only,
        )

    # --- Report ---
    report = format_batch_report(result, args.output_dir)
    if result.errors > 0:
        logger.error(report)
    else:
        logger.info(report)

    # Write report file
    if not args.warnings_only:
        os.makedirs(args.output_dir, exist_ok=True)
        report_path = os.path.join(args.output_dir, "_batch_report.txt")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report)
            f.write("\n")

    if result.errors > 0:
        return 2
    if result.warnings > 0:
        return 1
    return 0


def _batch_from_es(args):
    # type: (argparse.Namespace) -> Optional[Dict[str, Any]]
    """Fetch mappings from an ES cluster."""
    try:
        from .es_client import ESClient
    except ImportError:
        logger.error(
            "The 'requests' package is required for ES connection. "
            "Install with: pip install requests"
        )
        return None

    try:
        client = ESClient(
            base_url=args.es_url,
            username=args.es_user,
            password=args.es_password,
            verify_ssl=not args.no_verify_ssl,
        )
        info = client.get_cluster_info()
        logger.info(
            "Connected to ES cluster '%s' (version %s)",
            info.get("cluster_name", "?"),
            info.get("version", {}).get("number", "?"),
        )
    except Exception as e:
        logger.error("Error connecting to ES: %s", e)
        return None

    try:
        raw_mappings = client.get_all_mappings(args.es_index)
    except Exception as e:
        logger.error("Error fetching mappings: %s", e)
        return None

    from .mapping_parser import extract_all_mappings
    try:
        return extract_all_mappings(raw_mappings)
    except ValueError as e:
        logger.error("Error parsing mappings: %s", e)
        return None


def _batch_from_dir(input_dir):
    # type: (str) -> Optional[Dict[str, Any]]
    """Load mappings from a directory of JSON files."""
    from .mapping_parser import extract_all_mappings

    if not os.path.isdir(input_dir):
        logger.error("Input directory does not exist: %s", input_dir)
        return None

    all_slices = {}  # type: Dict[str, Any]
    json_files = sorted(f for f in os.listdir(input_dir) if f.endswith(".json"))

    if not json_files:
        logger.error("No .json files found in %s", input_dir)
        return None

    for fname in json_files:
        fpath = os.path.join(input_dir, fname)
        try:
            raw = _load_json(fpath)
            slices = extract_all_mappings(raw)
            all_slices.update(slices)
        except Exception as e:
            logger.warning("Skipping %s: %s", fname, e)

    return all_slices


def _create_executor(args):
    # type: (argparse.Namespace) -> Any
    """Create a DorisExecutor and test the connection."""
    try:
        from .doris_executor import DorisExecutor
    except ImportError:
        logger.error(
            "The 'pymysql' package is required for Doris execution. "
            "Install with: pip install pymysql"
        )
        return None

    executor = DorisExecutor(
        host=args.doris_host,
        port=args.doris_port,
        user=args.doris_user,
        password=args.doris_password,
        database=args.doris_db,
    )

    try:
        executor.test_connection()
        logger.info("Connected to Doris at %s:%d", args.doris_host, args.doris_port)
    except Exception as e:
        logger.error("Error connecting to Doris: %s", e)
        return None

    if args.doris_db:
        try:
            executor.ensure_database()
            logger.info("Database '%s' ready.", args.doris_db)
        except Exception as e:
            logger.error("Error creating database: %s", e)
            return None

    return executor


# ---------------------------------------------------------------------------
# main entry point
# ---------------------------------------------------------------------------

def main(argv=None):
    # type: (Optional[List[str]]) -> int
    """Main entry point. Returns exit code (0=ok, 1=warnings, 2=errors)."""
    parser = build_parser()
    args = parser.parse_args(argv)

    _setup_logging(
        verbose=getattr(args, "verbose", False),
        quiet=getattr(args, "quiet", False),
    )

    # ── Job file (-f) ──
    if args.job_file:
        try:
            job = _load_job_file(args.job_file)
        except ConfigValidationError as e:
            logger.error("%s", e)
            return 2
        except (FileNotFoundError, IOError) as e:
            logger.error("Error reading job file: %s", e)
            return 2
        _merge_job_into_args(args, job)
        # Auto-infer mode when no subcommand is given
        if args.subcommand is None and not args.input:
            if not args.es_url and not args.input_dir:
                logger.error(
                    "Job file must specify source.url, source.dir, or source.file"
                )
                return 2
            if not args.output_dir:
                logger.error("Job file must specify output.dir for batch mode")
                return 2
            args.subcommand = "batch"

    # Backward compat: no subcommand but -i is present → legacy convert mode
    if args.subcommand is None:
        if args.input:
            return _main_convert(args)
        else:
            parser.print_help()
            return 2

    if args.subcommand == "convert":
        return _main_convert(args)

    if args.subcommand == "batch":
        return _main_batch(args)

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
