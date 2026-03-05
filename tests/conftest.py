"""Shared test fixtures."""

import json
import os

import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def load_fixture(name):
    path = os.path.join(FIXTURES_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def full_example_mapping():
    return load_fixture("full_example_mapping.json")


@pytest.fixture
def es6_mapping():
    return load_fixture("es6_mapping.json")


@pytest.fixture
def simple_mapping():
    return load_fixture("simple_mapping.json")


@pytest.fixture
def collector():
    from es_ddl_converter.warnings import WarningCollector
    return WarningCollector()
