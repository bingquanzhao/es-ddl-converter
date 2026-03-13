"""Assemble Doris table structure: model, keys, partition, bucket."""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

from .index_strategy import IndexDef
from .type_mapping import DorisColumn
from .warnings import WarningCollector


@dataclass
class TableDef:
    """Complete Doris table definition ready for DDL rendering."""

    table_name: str
    columns: List[DorisColumn]
    indexes: List[IndexDef]
    table_model: str
    key_columns: List[str]
    partition_expr: Optional[str]
    distribution_expr: str
    properties: Dict[str, str] = field(default_factory=dict)


# Common time field names (ordered by priority)
TIME_FIELD_CANDIDATES = (
    "@timestamp", "timestamp", "created_at", "updated_at",
    "event_time", "log_time", "time", "date", "datetime",
    "ingest_time", "received_at", "processed_at",
)

PARTITIONABLE_TYPES = ("DATE", "DATETIME(0)", "DATETIME(3)", "DATETIME(6)")


def _can_be_key(col):
    # type: (DorisColumn) -> bool
    """Check if a column can be a Key column in Doris."""
    base = col.doris_type.upper().split("(")[0]
    non_key_types = {"STRING", "TEXT", "JSON", "VARIANT", "HLL", "BITMAP", "MAP", "STRUCT"}
    if base in non_key_types:
        return False
    if col.doris_type.upper().startswith("ARRAY"):
        return False
    return True


def _find_time_column(columns):
    # type: (List[DorisColumn]) -> Optional[DorisColumn]
    """Find the best time column for partitioning."""
    by_name = {c.name: c for c in columns}

    for candidate in TIME_FIELD_CANDIDATES:
        col_name = candidate.replace(".", "_")
        if col_name in by_name:
            col = by_name[col_name]
            if col.doris_type in PARTITIONABLE_TYPES:
                return col

    for col in columns:
        if col.doris_type in PARTITIONABLE_TYPES:
            return col

    return None


def _auto_select_keys(columns, model):
    # type: (List[DorisColumn], str) -> List[str]
    """Auto-select key columns when not specified by user."""
    if model == "UNIQUE KEY":
        for col in columns:
            if col.name == "_id":
                return ["_id"]
        for col in columns:
            if _can_be_key(col):
                return [col.name]
        return []

    # DUPLICATE KEY: time column only; keyword columns require explicit user config
    keys = []  # type: List[str]

    time_col = _find_time_column(columns)
    if time_col is not None:
        keys.append(time_col.name)

    if not keys:
        for col in columns:
            if _can_be_key(col):
                keys.append(col.name)
                break

    return keys


def _build_partition_expr(field_name, columns, collector):
    # type: (str, List[DorisColumn], WarningCollector) -> Optional[str]
    col = None  # type: Optional[DorisColumn]
    for c in columns:
        if c.name == field_name:
            col = c
            break
    if col is None:
        collector.warn("", "Partition field '{}' not found in columns.".format(field_name))
        return None
    if col.doris_type not in PARTITIONABLE_TYPES:
        collector.warn(
            field_name,
            "Partition field '{}' type {} may not support range partition.".format(
                field_name, col.doris_type),
        )
        return None
    return "AUTO PARTITION BY RANGE(date_trunc(`{}`, 'day'))".format(field_name)


VALID_COMPRESSION = {"NO_COMPRESSION", "LZ4", "LZ4F", "ZLIB", "ZSTD", "SNAPPY"}


def build_table(
    table_name,         # type: str
    columns,            # type: List[DorisColumn]
    indexes,            # type: List[IndexDef]
    collector,          # type: WarningCollector
    table_model="duplicate",    # type: str
    key_columns=None,           # type: Optional[List[str]]
    partition_field=None,       # type: Optional[str]
    bucket_strategy=None,       # type: Optional[str]
    replication_num=3,          # type: int
    compression="ZSTD",         # type: str
):
    # type: (...) -> TableDef
    """Build a complete TableDef from columns, indexes, and config."""

    # --- Table model ---
    if table_model.lower() == "unique":
        model_str = "UNIQUE KEY"
    else:
        model_str = "DUPLICATE KEY"

    # --- Key columns ---
    if key_columns:
        col_names = {c.name for c in columns}
        valid_keys = []  # type: List[str]
        for k in key_columns:
            if k not in col_names:
                collector.warn("", "Key column '{}' not found. Skipped.".format(k))
            else:
                col_obj = next(c for c in columns if c.name == k)
                if not _can_be_key(col_obj):
                    collector.warn(
                        k,
                        "Column '{}' (type {}) cannot be a Key column. Skipped.".format(
                            k, col_obj.doris_type),
                    )
                else:
                    valid_keys.append(k)
        resolved_keys = valid_keys if valid_keys else _auto_select_keys(columns, model_str)
    else:
        resolved_keys = _auto_select_keys(columns, model_str)

    if not resolved_keys:
        collector.error(
            "",
            "No suitable key column found. All columns have types that cannot be Doris key columns "
            "(e.g. ARRAY, VARIANT, TEXT, STRING). "
            "Add a keyable column or use --include-id to add an _id key column.",
        )

    # Reorder: key columns first, then rest in original order
    key_set = set(resolved_keys)
    key_order = {name: i for i, name in enumerate(resolved_keys)}
    key_cols = sorted(
        [c for c in columns if c.name in key_set],
        key=lambda c: key_order.get(c.name, 999),
    )
    non_key_cols = [c for c in columns if c.name not in key_set]
    ordered_columns = key_cols + non_key_cols

    # Mark key columns as NOT NULL
    for col in ordered_columns:
        if col.name in key_set:
            col.nullable = False

    # --- Partition ---
    partition_expr = None  # type: Optional[str]
    if partition_field:
        # For UNIQUE KEY, partition column must be a KEY column
        if model_str == "UNIQUE KEY" and partition_field not in key_set:
            collector.warn(
                partition_field,
                "Partition field '{}' is not a KEY column in UNIQUE KEY table. "
                "Skipping partition.".format(partition_field),
            )
        else:
            partition_expr = _build_partition_expr(partition_field, columns, collector)
    else:
        time_col = _find_time_column(columns)
        if time_col is not None:
            # For UNIQUE KEY, only partition if time column is in KEY set
            if model_str == "UNIQUE KEY" and time_col.name not in key_set:
                pass  # skip auto-partition for UNIQUE KEY when time col is not a key
            else:
                partition_expr = _build_partition_expr(time_col.name, columns, collector)

    # --- Bucket ---
    if bucket_strategy and bucket_strategy.lower().startswith("hash("):
        field_in = bucket_strategy[5:].rstrip(")")
        distribution_expr = "DISTRIBUTED BY HASH(`{}`) BUCKETS AUTO".format(field_in)
    elif model_str == "UNIQUE KEY" and resolved_keys:
        # UNIQUE KEY tables cannot use RANDOM distribution; default to HASH on first key
        distribution_expr = "DISTRIBUTED BY HASH(`{}`) BUCKETS AUTO".format(resolved_keys[0])
    else:
        distribution_expr = "DISTRIBUTED BY RANDOM BUCKETS AUTO"

    # --- Properties ---
    props = {
        "replication_num": str(replication_num),
        "compression": compression.upper(),
        "inverted_index_storage_format": "V3",
    }
    if partition_expr is not None:
        props["compaction_policy"] = "time_series"
    props["enable_single_replica_compaction"] = "true"

    return TableDef(
        table_name=table_name,
        columns=ordered_columns,
        indexes=indexes,
        table_model=model_str,
        key_columns=resolved_keys,
        partition_expr=partition_expr,
        distribution_expr=distribution_expr,
        properties=props,
    )
