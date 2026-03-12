"""Smoke tests against real-world ES mappings.

Each fixture is run through the full conversion pipeline
(extract_all_mappings -> convert_one_index) and verified for basic structural
correctness: no crashes, valid DDL output, reasonable column counts, etc.

Fixtures are downloaded from open-source projects by
``scripts/fetch_realworld_fixtures.py``.
"""

import json
import os

import pytest

from es_ddl_converter.batch import convert_one_index
from es_ddl_converter.mapping_parser import extract_all_mappings

REALWORLD_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "realworld")

# (fixture filename, minimum expected column count)
# Column counts reflect the VARIANT-by-default behavior:
# object fields with sub-properties become a single VARIANT column rather than
# being recursively flattened. Use flatten_fields to opt-in to flattening.
FIXTURES = [
    ("ecs_template.json", 40),
    ("wazuh_template.json", 30),
    ("rally_geonames.json", 10),
    ("rally_nyc_taxis.json", 15),
    ("rally_nested.json", 5),
    ("rally_dense_vector.json", 1),
    ("jaeger_span.json", 10),
    ("kibana_sample_ecommerce.json", 20),
    ("kibana_sample_flights.json", 20),
    ("kibana_sample_logs.json", 15),
]


def _load_and_convert(fixture_name):
    """Load a realworld fixture and run the full pipeline."""
    path = os.path.join(REALWORLD_DIR, fixture_name)
    with open(path, "r", encoding="utf-8") as f:
        raw_json = json.load(f)

    all_mappings = extract_all_mappings(raw_json)
    # Each fixture has exactly one index
    index_name = next(iter(all_mappings))
    mapping_body = all_mappings[index_name]

    result = convert_one_index(
        index_name=index_name,
        mapping_body=mapping_body,
        config={},
    )
    return result


@pytest.fixture(params=FIXTURES, ids=[f[0] for f in FIXTURES])
def realworld_result(request):
    """Parametrized fixture: loads and converts each realworld mapping."""
    fixture_name, min_columns = request.param
    result = _load_and_convert(fixture_name)
    return result, min_columns, fixture_name


class TestPipelineNoCrash:
    """Full pipeline should not raise exceptions for any real-world mapping."""

    @pytest.mark.parametrize("fixture_name,min_columns", FIXTURES, ids=[f[0] for f in FIXTURES])
    def test_pipeline_no_crash(self, fixture_name, min_columns):
        result = _load_and_convert(fixture_name)
        # Pipeline must return a valid status (no unhandled exception).
        # "error" is acceptable — it means a conversion issue was detected and
        # reported cleanly (e.g. no suitable key column, unsupported type).
        assert result.status in ("ok", "warning", "error")


class TestDdlContainsCreateTable:
    """Output DDL must contain CREATE TABLE."""

    @pytest.mark.parametrize("fixture_name,min_columns", FIXTURES, ids=[f[0] for f in FIXTURES])
    def test_ddl_contains_create_table(self, fixture_name, min_columns):
        result = _load_and_convert(fixture_name)
        assert result.ddl is not None, "DDL is None for {}".format(fixture_name)
        assert "CREATE TABLE" in result.ddl


class TestDdlStructuralValidity:
    """DDL must end with semicolon and have balanced parentheses."""

    @pytest.mark.parametrize("fixture_name,min_columns", FIXTURES, ids=[f[0] for f in FIXTURES])
    def test_ddl_structural_validity(self, fixture_name, min_columns):
        result = _load_and_convert(fixture_name)
        assert result.ddl is not None
        ddl = result.ddl.strip()
        assert ddl.endswith(";"), "DDL does not end with semicolon for {}".format(fixture_name)
        assert ddl.count("(") == ddl.count(")"), (
            "Unbalanced parentheses in DDL for {}: ({} open, {} close)".format(
                fixture_name, ddl.count("("), ddl.count(")")
            )
        )


class TestColumnCountMinimum:
    """DDL must have at least the expected minimum number of columns."""

    @pytest.mark.parametrize("fixture_name,min_columns", FIXTURES, ids=[f[0] for f in FIXTURES])
    def test_column_count_minimum(self, fixture_name, min_columns):
        result = _load_and_convert(fixture_name)
        assert result.ddl is not None
        # Count column definitions: lines that contain a type keyword inside the
        # CREATE TABLE block. Use a simple heuristic: count non-empty lines
        # between the first ( and the matching ) that look like column defs.
        lines = result.ddl.split("\n")
        col_count = 0
        in_columns = False
        for line in lines:
            stripped = line.strip()
            if "CREATE TABLE" in stripped:
                in_columns = True
                continue
            if in_columns and stripped.startswith(")"):
                break
            if in_columns and stripped and not stripped.startswith("--"):
                col_count += 1
        assert col_count >= min_columns, (
            "Expected >= {} columns for {}, got {}".format(min_columns, fixture_name, col_count)
        )


class TestDdlHasDistribution:
    """DDL must contain DISTRIBUTED BY clause."""

    @pytest.mark.parametrize("fixture_name,min_columns", FIXTURES, ids=[f[0] for f in FIXTURES])
    def test_ddl_has_distribution(self, fixture_name, min_columns):
        result = _load_and_convert(fixture_name)
        assert result.ddl is not None
        assert "DISTRIBUTED BY" in result.ddl, (
            "Missing DISTRIBUTED BY for {}".format(fixture_name)
        )


class TestDdlHasPropertiesBlock:
    """DDL must contain PROPERTIES block."""

    @pytest.mark.parametrize("fixture_name,min_columns", FIXTURES, ids=[f[0] for f in FIXTURES])
    def test_ddl_has_properties_block(self, fixture_name, min_columns):
        result = _load_and_convert(fixture_name)
        assert result.ddl is not None
        assert "PROPERTIES" in result.ddl, (
            "Missing PROPERTIES block for {}".format(fixture_name)
        )
