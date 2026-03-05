"""Render a TableDef into a SQL DDL string using Jinja2."""

import os
from typing import List, Set

import jinja2

from .table_builder import TableDef
from .type_mapping import DorisColumn


def _get_template_dir():
    # type: () -> str
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")


def render_ddl(table):
    # type: (TableDef) -> str
    """Render a TableDef into a CREATE TABLE DDL string."""
    template_dir = _get_template_dir()
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(template_dir),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=False,
        undefined=jinja2.StrictUndefined,
    )

    key_set = set(table.key_columns)  # type: Set[str]
    key_columns_list = []  # type: List[DorisColumn]
    key_order = {name: i for i, name in enumerate(table.key_columns)}
    for col in table.columns:
        if col.name in key_set:
            key_columns_list.append(col)
    key_columns_list.sort(key=lambda c: key_order.get(c.name, 999))

    value_columns = [c for c in table.columns if c.name not in key_set]

    # Compute alignment widths
    all_names = [c.name for c in table.columns]
    all_types = [c.doris_type for c in table.columns]
    max_name_len = max((len(n) for n in all_names), default=0)
    max_type_len = max((len(t) for t in all_types), default=0)

    key_columns_str = ", ".join("`{}`".format(k) for k in table.key_columns)
    properties_list = list(table.properties.items())

    def pad_name(name):
        # type: (str) -> int
        return max_name_len - len(name)

    def pad_type(dtype):
        # type: (str) -> int
        return max_type_len - len(dtype)

    template = env.get_template("create_table.sql.j2")
    rendered = template.render(
        table=table,
        key_set=key_set,
        key_columns_list=key_columns_list,
        value_columns=value_columns,
        key_columns_str=key_columns_str,
        properties_list=properties_list,
        pad_name=pad_name,
        pad_type=pad_type,
    )

    # Clean up multiple consecutive blank lines
    lines = rendered.split("\n")
    cleaned = []  # type: List[str]
    prev_blank = False
    for line in lines:
        is_blank = line.strip() == ""
        if is_blank and prev_blank:
            continue
        cleaned.append(line)
        prev_blank = is_blank

    # Remove trailing blank lines before closing paren
    result = "\n".join(cleaned).strip()
    return result
