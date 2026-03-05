"""ES type to Doris type mapping rules.

All mapping rules are data-driven via a handler registry.

To add support for a new ES type:
  1. Write a handler function matching the TypeHandler signature.
  2. Decorate it with @_register("es_type_name").

No business logic about table structure belongs here — only type conversion.
"""

import logging
import math
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .warnings import WarningCollector

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class DorisColumn:
    """Represents a single Doris column derived from an ES field."""

    name: str
    doris_type: str
    nullable: bool = True
    default_value: Optional[str] = None
    comment: str = ""
    es_type: str = ""
    es_field_path: str = ""
    analyzer: Optional[str] = None
    index_disabled: bool = False
    is_array: bool = False
    has_keyword_subfield: bool = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DIRECT_TYPE_MAP: Dict[str, str] = {
    "byte": "TINYINT",
    "short": "SMALLINT",
    "integer": "INT",
    "long": "BIGINT",
    "unsigned_long": "LARGEINT",
    "float": "FLOAT",
    "half_float": "FLOAT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "binary": "STRING",
    "token_count": "INT",
    "version": "VARCHAR(64)",
    "rank_feature": "DOUBLE",
    "rank_features": "VARIANT",
    "histogram": "VARIANT",
    "sparse_vector": "VARIANT",
    "geo_shape": "VARIANT",
    "shape": "VARIANT",
}

SKIP_TYPES = {"alias", "runtime"}

UNSUPPORTED_TYPES = {
    "join": (
        "Parent-child join relationships are not supported in Doris. "
        "Consider denormalization or application-level joins."
    ),
    "percolator": "Percolator (reverse matching) queries have no equivalent in Doris.",
}

RANGE_BASE_TYPES: Dict[str, str] = {
    "integer_range": "INT",
    "long_range": "BIGINT",
    "float_range": "FLOAT",
    "double_range": "DOUBLE",
    "date_range": "DATETIME(3)",
    "ip_range": "VARCHAR(64)",
}

# Keyword field name heuristics for STRING vs VARCHAR
LONG_VALUE_PATTERNS = (
    "url", "path", "description", "message", "body",
    "content", "text", "summary", "comment", "note",
    "payload", "raw", "original", "full_text",
)

ANALYZER_TO_PARSER: Dict[str, Optional[str]] = {
    "standard": "unicode",
    "simple": "unicode",
    "whitespace": "english",
    "english": "english",
    "ik_max_word": "chinese",
    "ik_smart": "chinese",
    "smartcn": "chinese",
    "cjk": "unicode",
    "pattern": "unicode",
    "keyword": None,
}


# ---------------------------------------------------------------------------
# Public resolve helpers (also used by index_strategy)
# ---------------------------------------------------------------------------

def resolve_analyzer_parser(analyzer, field_path, collector):
    # type: (Optional[str], str, WarningCollector) -> Optional[str]
    """Map an ES analyzer name to a Doris inverted index parser name."""
    if analyzer is None:
        return "unicode"
    if analyzer in ANALYZER_TO_PARSER:
        return ANALYZER_TO_PARSER[analyzer]
    collector.warn(
        field_path,
        "Custom analyzer '{}' has no direct Doris equivalent. "
        "Falling back to 'unicode' parser. Manual review recommended.".format(analyzer),
        es_type="text",
    )
    return "unicode"


def resolve_date_type(format_str):
    # type: (Optional[str]) -> str
    """Determine the Doris date/datetime type based on ES date format string."""
    if format_str is None:
        return "DATETIME(3)"

    formats = [f.strip() for f in format_str.split("||")]

    has_date_only = False
    has_epoch_second = False
    has_epoch_millis = False
    has_time_component = False

    for fmt in formats:
        lower = fmt.lower()
        if lower in ("date", "strict_date", "basic_date", "yyyy-mm-dd"):
            has_date_only = True
        elif "epoch_second" in lower:
            has_epoch_second = True
        elif "epoch_millis" in lower:
            has_epoch_millis = True
        else:
            if any(c in fmt for c in ("H", "h", "m", "s", "S", "T", "Z")):
                has_time_component = True
            else:
                has_date_only = True

    if has_epoch_millis or has_time_component:
        return "DATETIME(3)"
    if has_epoch_second:
        return "DATETIME(0)"
    if has_date_only and not has_epoch_millis and not has_time_component:
        return "DATE"
    return "DATETIME(3)"


def resolve_scaled_float(scaling_factor):
    # type: (Optional[float]) -> str
    """Compute DECIMAL(38, S) from ES scaling_factor."""
    if scaling_factor is None or scaling_factor <= 0:
        return "DECIMAL(38, 2)"
    s = int(math.ceil(math.log10(scaling_factor)))
    s = max(0, min(s, 38))
    return "DECIMAL(38, {})".format(s)


def resolve_keyword_type(field_name, ignore_above):
    # type: (str, Optional[int]) -> str
    """Determine VARCHAR(N) or STRING for a keyword field."""
    if ignore_above is not None:
        if ignore_above > 65533:
            return "STRING"
        return "VARCHAR({})".format(ignore_above)
    lower_name = field_name.lower()
    for pattern in LONG_VALUE_PATTERNS:
        if pattern in lower_name:
            return "STRING"
    return "VARCHAR(256)"


# ---------------------------------------------------------------------------
# Handler registry
# ---------------------------------------------------------------------------

# TypeHandler: the signature every handler must implement.
TypeHandler = Callable[
    [str, str, Dict[str, Any], WarningCollector, str],
    List[DorisColumn],
]

_TYPE_HANDLERS: Dict[str, TypeHandler] = {}


def _register(*es_types: str):
    """Decorator: register a function as the handler for one or more ES types."""
    def decorator(fn: TypeHandler) -> TypeHandler:
        for t in es_types:
            _TYPE_HANDLERS[t] = fn
        return fn
    return decorator


def _common_field_props(field_def: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Extract index_disabled and default_value from a field definition."""
    index_disabled = field_def.get("index") is False
    null_value = field_def.get("null_value")
    default_str = str(null_value) if null_value is not None else None
    return index_disabled, default_str


# ---------------------------------------------------------------------------
# Handler implementations
# ---------------------------------------------------------------------------

@_register("keyword", "constant_keyword")
def _handle_keyword(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    index_disabled, default_str = _common_field_props(field_def)
    ignore_above = field_def.get("ignore_above")
    dtype = resolve_keyword_type(field_name, ignore_above)
    comment = field_def.get("type", "keyword")
    if ignore_above is not None:
        comment += ", ignore_above={}".format(ignore_above)
    return [DorisColumn(
        name=field_name,
        doris_type=dtype,
        comment=comment,
        es_type=field_def.get("type", "keyword"),
        es_field_path=field_path,
        index_disabled=index_disabled,
        default_value=default_str,
    )]


@_register("wildcard")
def _handle_wildcard(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    index_disabled, default_str = _common_field_props(field_def)
    return [DorisColumn(
        name=field_name,
        doris_type="STRING",
        comment="wildcard",
        es_type="wildcard",
        es_field_path=field_path,
        index_disabled=index_disabled,
        default_value=default_str,
    )]


@_register("text", "match_only_text")
def _handle_text(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    index_disabled, default_str = _common_field_props(field_def)
    es_type = field_def.get("type", "text")
    analyzer = field_def.get("analyzer")
    has_kw = "keyword" in field_def.get("fields", {})
    comment = es_type
    if analyzer:
        comment += ", analyzer={}".format(analyzer)
    return [DorisColumn(
        name=field_name,
        doris_type="TEXT",
        comment=comment,
        es_type=es_type,
        es_field_path=field_path,
        analyzer=analyzer,
        has_keyword_subfield=has_kw,
        index_disabled=index_disabled,
        default_value=default_str,
    )]


@_register("completion")
def _handle_completion(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    index_disabled, _ = _common_field_props(field_def)
    return [DorisColumn(
        name=field_name,
        doris_type="STRING",
        comment="completion",
        es_type="completion",
        es_field_path=field_path,
        index_disabled=index_disabled,
    )]


@_register("search_as_you_type")
def _handle_search_as_you_type(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    index_disabled, _ = _common_field_props(field_def)
    analyzer = field_def.get("analyzer")
    return [DorisColumn(
        name=field_name,
        doris_type="TEXT",
        comment="search_as_you_type",
        es_type="search_as_you_type",
        es_field_path=field_path,
        analyzer=analyzer,
        index_disabled=index_disabled,
    )]


@_register("scaled_float")
def _handle_scaled_float(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    index_disabled, default_str = _common_field_props(field_def)
    sf = field_def.get("scaling_factor")
    dtype = resolve_scaled_float(sf)
    return [DorisColumn(
        name=field_name,
        doris_type=dtype,
        comment="scaled_float, scaling_factor={}".format(sf),
        es_type="scaled_float",
        es_field_path=field_path,
        index_disabled=index_disabled,
        default_value=default_str,
    )]


@_register("date")
def _handle_date(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    index_disabled, default_str = _common_field_props(field_def)
    fmt = field_def.get("format")
    dtype = resolve_date_type(fmt)
    comment = "date"
    if fmt:
        comment += ", format={}".format(fmt)
    return [DorisColumn(
        name=field_name,
        doris_type=dtype,
        comment=comment,
        es_type="date",
        es_field_path=field_path,
        index_disabled=index_disabled,
        default_value=default_str,
    )]


@_register("date_nanos")
def _handle_date_nanos(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    index_disabled, _ = _common_field_props(field_def)
    collector.warn(
        field_path,
        "date_nanos converted to DATETIME(6). Nanosecond precision will be "
        "truncated to microseconds.",
        es_type="date_nanos",
    )
    return [DorisColumn(
        name=field_name,
        doris_type="DATETIME(6)",
        comment="date_nanos (precision loss: ns->us)",
        es_type="date_nanos",
        es_field_path=field_path,
        index_disabled=index_disabled,
    )]


@_register("ip")
def _handle_ip(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    index_disabled, _ = _common_field_props(field_def)
    dtype = "IPv4" if ip_type.upper() == "IPV4" else "IPv6"
    return [DorisColumn(
        name=field_name,
        doris_type=dtype,
        comment="ip",
        es_type="ip",
        es_field_path=field_path,
        index_disabled=index_disabled,
    )]


@_register("dense_vector")
def _handle_dense_vector(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    dims = field_def.get("dims", "?")
    collector.warn(
        field_path,
        "dense_vector converted to ARRAY<FLOAT>. "
        "Doris has no native ANN retrieval. dims={}".format(dims),
        es_type="dense_vector",
    )
    return [DorisColumn(
        name=field_name,
        doris_type="ARRAY<FLOAT>",
        comment="dense_vector, dims={}".format(dims),
        es_type="dense_vector",
        es_field_path=field_path,
        index_disabled=True,
    )]


@_register("geo_point")
def _handle_geo_point(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    collector.warn(
        field_path,
        "geo_point mapped to VARIANT. Original {lat, lon} structure is preserved. "
        "Doris has no native geospatial query support.",
        es_type="geo_point",
    )
    return [DorisColumn(
        name=field_name,
        doris_type="VARIANT",
        comment="geo_point",
        es_type="geo_point",
        es_field_path=field_path,
    )]


@_register("point")
def _handle_point(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    collector.warn(
        field_path,
        "point mapped to VARIANT. Original {x, y} structure is preserved. "
        "Doris has no native geospatial query support.",
        es_type="point",
    )
    return [DorisColumn(
        name=field_name,
        doris_type="VARIANT",
        comment="point",
        es_type="point",
        es_field_path=field_path,
    )]


@_register("aggregate_metric_double")
def _handle_aggregate_metric_double(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    metrics = field_def.get("metrics", ["min", "max", "sum", "value_count"])
    metric_type_map = {
        "min": "DOUBLE",
        "max": "DOUBLE",
        "sum": "DOUBLE",
        "value_count": "BIGINT",
        "avg": "DOUBLE",
    }
    columns = []  # type: List[DorisColumn]
    for metric in metrics:
        suffix = "_count" if metric == "value_count" else "_{}".format(metric)
        dtype = metric_type_map.get(metric, "DOUBLE")
        columns.append(DorisColumn(
            name="{}{}".format(field_name, suffix),
            doris_type=dtype,
            comment="aggregate_metric_double.{}".format(metric),
            es_type="aggregate_metric_double",
            es_field_path=field_path,
        ))
    return columns


@_register("nested")
def _handle_nested(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    collector.warn(
        field_path,
        "nested type converted to VARIANT. Nested object correlation semantics "
        "are not preserved in Doris. VARIANT will auto-extract sub-columns "
        "for efficient columnar access.",
        es_type="nested",
    )
    return [DorisColumn(
        name=field_name,
        doris_type="VARIANT",
        comment="nested",
        es_type="nested",
        es_field_path=field_path,
    )]


@_register("flattened")
def _handle_flattened(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    return [DorisColumn(
        name=field_name,
        doris_type="VARIANT",
        comment="flattened",
        es_type="flattened",
        es_field_path=field_path,
    )]


# ---------------------------------------------------------------------------
# Programmatically register the remaining type groups
# ---------------------------------------------------------------------------

def _make_direct_handler(doris_type: str) -> TypeHandler:
    """Factory: create a handler that maps an ES type 1:1 to a Doris type."""
    def handler(field_name, field_path, field_def, collector, ip_type):
        # type: (...) -> List[DorisColumn]
        index_disabled, default_str = _common_field_props(field_def)
        return [DorisColumn(
            name=field_name,
            doris_type=doris_type,
            comment=field_def.get("type", ""),
            es_type=field_def.get("type", ""),
            es_field_path=field_path,
            index_disabled=index_disabled,
            default_value=default_str,
        )]
    return handler


def _make_range_handler(base_type: str) -> TypeHandler:
    """Factory: create a handler that expands a range type into _gte/_lte columns."""
    def handler(field_name, field_path, field_def, collector, ip_type):
        # type: (...) -> List[DorisColumn]
        es_type = field_def.get("type", "")
        return [
            DorisColumn(
                name="{}_gte".format(field_name),
                doris_type=base_type,
                comment="{} lower bound".format(es_type),
                es_type=es_type,
                es_field_path=field_path,
            ),
            DorisColumn(
                name="{}_lte".format(field_name),
                doris_type=base_type,
                comment="{} upper bound".format(es_type),
                es_type=es_type,
                es_field_path=field_path,
            ),
        ]
    return handler


def _handle_skip(field_name, field_path, field_def, collector, ip_type):
    # type: (...) -> List[DorisColumn]
    """Silently skip alias and runtime fields (virtual, no storage needed)."""
    return []


def _make_unsupported_handler(message: str) -> TypeHandler:
    """Factory: create a handler that records an error for an unsupported ES type."""
    def handler(field_name, field_path, field_def, collector, ip_type):
        # type: (...) -> List[DorisColumn]
        collector.error(field_path, message, es_type=field_def.get("type", ""))
        return []
    return handler


for _es_type, _doris_type in DIRECT_TYPE_MAP.items():
    _TYPE_HANDLERS[_es_type] = _make_direct_handler(_doris_type)

for _es_type, _base_type in RANGE_BASE_TYPES.items():
    _TYPE_HANDLERS[_es_type] = _make_range_handler(_base_type)

for _es_type in SKIP_TYPES:
    _TYPE_HANDLERS[_es_type] = _handle_skip

for _es_type, _msg in UNSUPPORTED_TYPES.items():
    _TYPE_HANDLERS[_es_type] = _make_unsupported_handler(_msg)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def map_es_field(field_name, field_path, field_def, collector, ip_type="IPv6"):
    # type: (str, str, Dict[str, Any], WarningCollector, str) -> List[DorisColumn]
    """Map a single ES field definition to one or more DorisColumn objects.

    Dispatches to a registered handler based on the ES type. Returns an empty
    list for skipped types. Objects with properties are NOT handled here —
    they are handled by mapping_parser via recursion.
    """
    es_type = field_def.get("type", "object")
    handler = _TYPE_HANDLERS.get(es_type)
    if handler is not None:
        return handler(field_name, field_path, field_def, collector, ip_type)

    # Unknown type: emit a user-visible warning and fall back to VARIANT
    logger.debug("Unknown ES type '%s' at '%s', mapping to VARIANT", es_type, field_path)
    collector.warn(
        field_path,
        "Unknown ES type '{}'. Mapped to VARIANT as fallback.".format(es_type),
        es_type=es_type,
    )
    return [DorisColumn(
        name=field_name,
        doris_type="VARIANT",
        comment="unknown type: {}".format(es_type),
        es_type=es_type,
        es_field_path=field_path,
    )]
