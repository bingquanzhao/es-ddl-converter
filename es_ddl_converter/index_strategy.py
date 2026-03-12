"""Determine Doris index strategy for each column."""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

from .type_mapping import DorisColumn, resolve_analyzer_parser
from .warnings import WarningCollector


@dataclass
class IndexDef:
    """Represents a Doris index definition."""

    index_name: str
    column_name: str
    index_type: str  # "INVERTED" or "NGRAM_BF"
    properties: Dict[str, str] = field(default_factory=dict)
    comment: str = ""

    def format_properties(self):
        # type: () -> str
        if not self.properties:
            return ""
        pairs = ", ".join(
            '"{}"="{}"'.format(k, v) for k, v in sorted(self.properties.items())
        )
        return " PROPERTIES({})".format(pairs)


# Types where inverted index is not supported
NO_INVERTED_INDEX_TYPES = {"FLOAT", "DOUBLE"}


def _base_doris_type(doris_type):
    # type: (str) -> str
    """Extract base type from a possibly parameterized type like ARRAY<VARCHAR(256)>."""
    t = doris_type.upper().strip()
    if t.startswith("ARRAY<"):
        inner = t[6:-1].strip()
        return inner.split("(")[0]
    return t.split("(")[0]


def _make_index_name(column_name, seen_names, suffix=""):
    # type: (str, set, str) -> str
    """Generate a unique index name."""
    base = "idx_{}".format(column_name.replace("@", "").replace(".", "_"))
    if suffix:
        base = "{}_{}".format(base, suffix)
    name = base
    counter = 2
    while name in seen_names:
        name = "{}_{}".format(base, counter)
        counter += 1
    seen_names.add(name)
    return name


def determine_indexes(columns, collector):
    # type: (List[DorisColumn], WarningCollector) -> List[IndexDef]
    """Determine index definitions for all columns.

    Rules:
    - keyword/constant_keyword: INVERTED (no parser)
    - text/match_only_text/search_as_you_type: INVERTED with parser
    - wildcard: INVERTED + NGRAM_BF
    - VARIANT: INVERTED (indexes sub-columns for efficient filtering)
    - ARRAY fields (non-float): INVERTED for array_contains()
    - FLOAT/DOUBLE: skip (Doris limitation)
    - index:false: skip
    """
    indexes = []  # type: List[IndexDef]
    seen_names = set()  # type: set

    for col in columns:
        if col.index_disabled:
            continue

        base_type = _base_doris_type(col.doris_type)

        if base_type in NO_INVERTED_INDEX_TYPES:
            continue

        # --- VARIANT: inverted index on sub-columns ---
        if base_type == "VARIANT":
            idx_name = _make_index_name(col.name, seen_names)
            indexes.append(IndexDef(
                index_name=idx_name,
                column_name=col.name,
                index_type="INVERTED",
                properties={},
                comment="accelerate sub-column filtering",
            ))
            continue

        # --- Array fields: inverted index for array_contains (check first) ---
        if col.is_array and base_type not in NO_INVERTED_INDEX_TYPES:
            idx_name = _make_index_name(col.name, seen_names)
            indexes.append(IndexDef(
                index_name=idx_name,
                column_name=col.name,
                index_type="INVERTED",
                properties={},
                comment="array contains",
            ))
            continue

        # --- Text fields with parser ---
        if col.es_type in ("text", "match_only_text", "search_as_you_type"):
            parser = resolve_analyzer_parser(col.analyzer, col.es_field_path, collector)
            props = {}  # type: Dict[str, str]
            if parser is not None:
                props["parser"] = parser
                # match_only_text: no position info → no phrase query support
                props["support_phrase"] = "false" if col.es_type == "match_only_text" else "true"
            idx_name = _make_index_name(col.name, seen_names)
            indexes.append(IndexDef(
                index_name=idx_name,
                column_name=col.name,
                index_type="INVERTED",
                properties=props,
                comment="full-text search",
            ))
            continue

        # --- Keyword fields ---
        if col.es_type in ("keyword", "constant_keyword"):
            idx_name = _make_index_name(col.name, seen_names)
            indexes.append(IndexDef(
                index_name=idx_name,
                column_name=col.name,
                index_type="INVERTED",
                properties={},
                comment="keyword exact match",
            ))
            continue

        # --- Wildcard: INVERTED + NGRAM_BF ---
        if col.es_type == "wildcard":
            idx_name = _make_index_name(col.name, seen_names)
            indexes.append(IndexDef(
                index_name=idx_name,
                column_name=col.name,
                index_type="INVERTED",
                properties={},
                comment="wildcard search",
            ))
            ngram_name = _make_index_name(col.name, seen_names, suffix="ngram")
            indexes.append(IndexDef(
                index_name=ngram_name,
                column_name=col.name,
                index_type="NGRAM_BF",
                properties={"gram_size": "3", "bf_size": "1024"},
                comment="wildcard LIKE pattern",
            ))
            continue

    return indexes
