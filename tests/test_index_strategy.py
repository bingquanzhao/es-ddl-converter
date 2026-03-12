"""Tests for index_strategy module."""

import pytest

from es_ddl_converter.index_strategy import IndexDef, determine_indexes
from es_ddl_converter.type_mapping import DorisColumn
from es_ddl_converter.warnings import WarningCollector


@pytest.fixture
def collector():
    return WarningCollector()


def _col(name, doris_type, es_type, analyzer=None, index_disabled=False, is_array=False):
    return DorisColumn(
        name=name,
        doris_type=doris_type,
        es_type=es_type,
        es_field_path=name,
        analyzer=analyzer,
        index_disabled=index_disabled,
        is_array=is_array,
    )


def test_keyword_gets_inverted(collector):
    cols = [_col("status", "VARCHAR(256)", "keyword")]
    indexes = determine_indexes(cols, collector)
    assert len(indexes) == 1
    assert indexes[0].index_type == "INVERTED"
    assert indexes[0].properties == {}


def test_text_gets_inverted_with_parser(collector):
    cols = [_col("message", "TEXT", "text", analyzer="standard")]
    indexes = determine_indexes(cols, collector)
    assert len(indexes) == 1
    assert indexes[0].index_type == "INVERTED"
    assert "parser" in indexes[0].properties
    assert indexes[0].properties["parser"] == "unicode"


def test_text_ik_analyzer(collector):
    cols = [_col("content", "TEXT", "text", analyzer="ik_max_word")]
    indexes = determine_indexes(cols, collector)
    assert indexes[0].properties["parser"] == "ik"


def test_wildcard_gets_two_indexes(collector):
    cols = [_col("pattern", "STRING", "wildcard")]
    indexes = determine_indexes(cols, collector)
    assert len(indexes) == 2
    types = {idx.index_type for idx in indexes}
    assert "INVERTED" in types
    assert "NGRAM_BF" in types


def test_float_no_index(collector):
    cols = [_col("score", "FLOAT", "float")]
    indexes = determine_indexes(cols, collector)
    assert len(indexes) == 0


def test_double_no_index(collector):
    cols = [_col("value", "DOUBLE", "double")]
    indexes = determine_indexes(cols, collector)
    assert len(indexes) == 0


def test_index_disabled_skip(collector):
    cols = [_col("f", "VARCHAR(256)", "keyword", index_disabled=True)]
    indexes = determine_indexes(cols, collector)
    assert len(indexes) == 0


def test_json_no_index(collector):
    cols = [_col("data", "JSON", "nested")]
    indexes = determine_indexes(cols, collector)
    assert len(indexes) == 0


def test_array_gets_inverted(collector):
    cols = [_col("tags", "ARRAY<VARCHAR(256)>", "keyword", is_array=True)]
    indexes = determine_indexes(cols, collector)
    assert len(indexes) == 1
    assert indexes[0].comment == "array contains"


def test_array_float_no_index(collector):
    cols = [_col("vec", "ARRAY<FLOAT>", "dense_vector", is_array=True)]
    indexes = determine_indexes(cols, collector)
    assert len(indexes) == 0


def test_unique_index_names(collector):
    cols = [
        _col("level", "VARCHAR(256)", "keyword"),
        _col("level_2", "VARCHAR(256)", "keyword"),
    ]
    indexes = determine_indexes(cols, collector)
    names = [idx.index_name for idx in indexes]
    assert len(names) == len(set(names))


def test_format_properties():
    idx = IndexDef(
        index_name="idx_msg",
        column_name="msg",
        index_type="INVERTED",
        properties={"parser": "unicode", "support_phrase": "true"},
    )
    formatted = idx.format_properties()
    assert '"parser"="unicode"' in formatted
    assert '"support_phrase"="true"' in formatted


def test_format_properties_empty():
    idx = IndexDef(
        index_name="idx_f",
        column_name="f",
        index_type="INVERTED",
    )
    assert idx.format_properties() == ""
