"""Parse ES mapping JSON into a flat list of DorisColumns.

Handles:
- Three JSON input formats (ES 7+ API, ES 6.x, simplified)
- Recursive object flattening
- Multi-field collapsing (text + .keyword -> single column)
- Array field annotation from config
- copy_to target detection
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

from .type_mapping import DorisColumn, map_es_field
from .warnings import WarningCollector


@dataclass
class ParsedMapping:
    """Result of parsing an ES mapping."""

    index_name: str
    columns: List[DorisColumn]
    dynamic: Optional[str]
    has_routing: bool
    copy_to_targets: Set[str] = field(default_factory=set)


def extract_mapping(raw_json):
    # type: (Dict[str, Any]) -> Tuple[str, Dict[str, Any]]
    """Extract the mapping body and index name from various JSON formats.

    Supported formats:
    1. {"index_name": {"mappings": {"properties": {...}}}}           -- ES 7+ API
    2. {"index_name": {"mappings": {"doc": {"properties": {...}}}}}  -- ES 6.x
    3. {"mappings": {"properties": {...}}}                           -- Simplified

    Returns:
        (index_name, mapping_body) where mapping_body has "properties" at top level.
    """
    # Format 3: simplified (no index name wrapper)
    if "mappings" in raw_json and isinstance(raw_json.get("mappings"), dict):
        mappings = raw_json["mappings"]
        if "properties" in mappings:
            return ("unnamed_index", mappings)
        # Check for type wrapper: {"mappings": {"doc": {"properties": ...}}}
        for key, val in mappings.items():
            if isinstance(val, dict) and "properties" in val:
                return ("unnamed_index", val)

    # Format 1 or 2: index_name at top level
    keys = [k for k in raw_json.keys() if not k.startswith("_")]
    if len(keys) == 1:
        index_name = keys[0]
        index_body = raw_json[index_name]
        if isinstance(index_body, dict) and "mappings" in index_body:
            mappings = index_body["mappings"]
            # Format 1: mappings.properties directly
            if "properties" in mappings:
                return (index_name, mappings)
            # Format 2: mappings.<type_name>.properties
            for type_name, type_body in mappings.items():
                if isinstance(type_body, dict) and "properties" in type_body:
                    return (index_name, type_body)

    raise ValueError(
        "Unrecognized ES mapping JSON format. Expected one of: "
        "(1) {index: {mappings: {properties: ...}}}, "
        "(2) {index: {mappings: {type: {properties: ...}}}}, "
        "(3) {mappings: {properties: ...}}"
    )


def extract_all_mappings(raw_json):
    # type: (Dict[str, Any]) -> Dict[str, Dict[str, Any]]
    """Extract all index mappings from a multi-index ES API response.

    The ES ``GET /_mapping`` response may contain multiple indexes::

        {"idx1": {"mappings": {...}}, "idx2": {"mappings": {...}}}

    System indexes (names starting with ``'.'``) are skipped automatically.

    Also handles single-index formats by delegating to :func:`extract_mapping`.

    Returns:
        dict mapping index_name -> mapping_body (with ``"properties"`` at top).
    """
    # Format 3: simplified (has "mappings" directly at top level)
    if "mappings" in raw_json and isinstance(raw_json.get("mappings"), dict):
        index_name, mapping_body = extract_mapping(raw_json)
        return {index_name: mapping_body}

    results = {}  # type: Dict[str, Dict[str, Any]]
    for key, value in raw_json.items():
        if key.startswith("_") or key.startswith("."):
            continue
        if not isinstance(value, dict) or "mappings" not in value:
            continue
        mappings = value["mappings"]
        # Format 1: mappings.properties directly
        if isinstance(mappings, dict) and "properties" in mappings:
            results[key] = mappings
            continue
        # Format 2: mappings.<type_name>.properties (ES 6.x)
        if isinstance(mappings, dict):
            for type_name, type_body in mappings.items():
                if isinstance(type_body, dict) and "properties" in type_body:
                    results[key] = type_body
                    break

    if not results:
        raise ValueError(
            "No valid index mappings found in the provided JSON. "
            "Expected ES _mapping API response format."
        )
    return results


def parse_mapping(
    raw_json,           # type: Dict[str, Any]
    collector,          # type: WarningCollector
    array_fields=None,  # type: Optional[Set[str]]
    flatten_fields=None,  # type: Optional[Set[str]]
    ip_type="IPv6",     # type: str
    include_id=False,   # type: bool
):
    # type: (...) -> ParsedMapping
    """Parse a full ES mapping JSON into a ParsedMapping.

    By default, ``object`` fields with sub-properties are mapped to VARIANT.
    Specify ``flatten_fields`` (a set of ES dot-paths) to opt-in to flattening
    specific object fields (e.g. ``{"user", "host.geo"}``).
    """
    if array_fields is None:
        array_fields = set()
    if flatten_fields is None:
        flatten_fields = set()

    index_name, mapping_body = extract_mapping(raw_json)
    properties = mapping_body.get("properties", {})
    dynamic = mapping_body.get("dynamic")
    copy_to_targets = set()  # type: Set[str]
    used_names = set()  # type: Set[str]

    columns = []  # type: List[DorisColumn]

    # Optionally add _id column
    if include_id:
        columns.append(DorisColumn(
            name="_id",
            doris_type="VARCHAR(128)",
            nullable=False,
            comment="ES document _id",
            es_type="_id",
            es_field_path="_id",
        ))
        used_names.add("_id")

    # Walk properties; objects default to VARIANT unless in flatten_fields
    _flatten_properties(
        properties=properties,
        prefix="",
        path_prefix="",
        columns=columns,
        collector=collector,
        array_fields=array_fields,
        flatten_fields=flatten_fields,
        copy_to_targets=copy_to_targets,
        used_names=used_names,
        ip_type=ip_type,
    )

    # Handle copy_to targets not already present as columns
    existing_names = {c.name for c in columns}
    for target in copy_to_targets:
        col_name = target.replace(".", "_")
        if col_name not in existing_names:
            collector.info(
                target,
                "copy_to target '{}' not in mapping. Creating as TEXT column.".format(target),
            )
            columns.append(DorisColumn(
                name=col_name,
                doris_type="TEXT",
                comment="auto-created copy_to target",
                es_type="text",
                es_field_path=target,
            ))

    has_routing = False
    routing_meta = mapping_body.get("_routing", {})
    if isinstance(routing_meta, dict) and routing_meta.get("required"):
        has_routing = True
        collector.info(
            "_routing",
            "Routing is required. Consider DISTRIBUTED BY HASH on the routing field.",
        )

    return ParsedMapping(
        index_name=index_name,
        columns=columns,
        dynamic=dynamic,
        has_routing=has_routing,
        copy_to_targets=copy_to_targets,
    )


def _resolve_unique_name(name, used_names):
    # type: (str, Set[str]) -> str
    """Resolve a unique column name, appending suffix on conflict."""
    if name not in used_names:
        used_names.add(name)
        return name
    counter = 2
    while True:
        candidate = "{}_{}".format(name, counter)
        if candidate not in used_names:
            used_names.add(candidate)
            return candidate
        counter += 1


def _flatten_properties(
    properties,       # type: Dict[str, Any]
    prefix,           # type: str
    path_prefix,      # type: str
    columns,          # type: List[DorisColumn]
    collector,        # type: WarningCollector
    array_fields,     # type: Set[str]
    flatten_fields,   # type: Set[str]
    copy_to_targets,  # type: Set[str]
    used_names,       # type: Set[str]
    ip_type,          # type: str
    depth=0,          # type: int
):
    # type: (...) -> None
    """Walk ES properties and map to DorisColumns.

    Objects default to VARIANT. Only paths listed in ``flatten_fields``
    are recursively expanded into individual columns.
    """
    for field_name, field_def in properties.items():
        if not isinstance(field_def, dict):
            continue

        doris_name = "{}{}".format(prefix, field_name)
        es_path = "{}{}".format(path_prefix, field_name)
        es_type = field_def.get("type")

        # Collect copy_to targets
        copy_to = field_def.get("copy_to")
        if copy_to:
            if isinstance(copy_to, str):
                copy_to_targets.add(copy_to)
            elif isinstance(copy_to, list):
                copy_to_targets.update(copy_to)

        # --- Object without properties (or enabled:false) ---
        has_properties = "properties" in field_def and isinstance(field_def.get("properties"), dict)
        is_object = es_type == "object" or es_type is None

        if is_object and not has_properties:
            # Object with no sub-properties or enabled:false -> VARIANT
            unique_name = _resolve_unique_name(doris_name, used_names)
            comment = "object, enabled=false" if field_def.get("enabled") is False else "object, no properties defined"
            columns.append(DorisColumn(
                name=unique_name,
                doris_type="VARIANT",
                comment=comment,
                es_type="object",
                es_field_path=es_path,
            ))
            continue

        # --- Object with properties ---
        if has_properties and is_object:
            # enabled:false always -> VARIANT
            if field_def.get("enabled") is False:
                unique_name = _resolve_unique_name(doris_name, used_names)
                columns.append(DorisColumn(
                    name=unique_name,
                    doris_type="VARIANT",
                    comment="object, enabled=false",
                    es_type="object",
                    es_field_path=es_path,
                ))
                continue

            # Default: VARIANT unless this path is in flatten_fields
            if es_path not in flatten_fields:
                unique_name = _resolve_unique_name(doris_name, used_names)
                columns.append(DorisColumn(
                    name=unique_name,
                    doris_type="VARIANT",
                    comment="object",
                    es_type="object",
                    es_field_path=es_path,
                ))
                continue

            # Depth limit protection (only relevant when flattening)
            if depth >= 10:
                collector.warn(
                    es_path,
                    "Object nesting depth exceeds 10. Mapping as VARIANT.",
                    es_type="object",
                )
                unique_name = _resolve_unique_name(doris_name, used_names)
                columns.append(DorisColumn(
                    name=unique_name,
                    doris_type="VARIANT",
                    comment="object, depth limit exceeded",
                    es_type="object",
                    es_field_path=es_path,
                ))
                continue

            # Recurse into sub-properties (flatten_fields opt-in)
            sub_props = field_def["properties"]
            _flatten_properties(
                properties=sub_props,
                prefix="{}_".format(doris_name),
                path_prefix="{}.".format(es_path),
                columns=columns,
                collector=collector,
                array_fields=array_fields,
                flatten_fields=flatten_fields,
                copy_to_targets=copy_to_targets,
                used_names=used_names,
                ip_type=ip_type,
                depth=depth + 1,
            )
            continue

        # --- Leaf field with explicit type: delegate to type_mapping ---
        if es_type is not None:
            mapped_cols = map_es_field(
                field_name=doris_name,
                field_path=es_path,
                field_def=field_def,
                collector=collector,
                ip_type=ip_type,
            )

            # Annotate array fields
            is_array = es_path in array_fields
            for col in mapped_cols:
                col.name = _resolve_unique_name(col.name, used_names)
                if is_array:
                    col.is_array = True
                    col.doris_type = "ARRAY<{}>".format(col.doris_type)
                    col.comment += ", multi-value"

            columns.extend(mapped_cols)
            continue

        # --- No type, no properties: treat as VARIANT ---
        unique_name = _resolve_unique_name(doris_name, used_names)
        columns.append(DorisColumn(
            name=unique_name,
            doris_type="VARIANT",
            comment="object, no properties defined",
            es_type="object",
            es_field_path=es_path,
        ))
