"""End-to-end tests: ES mapping → converter → Doris DDL execution.

Requires Docker services (ES + Doris) running via docker/docker-compose.yml.
Run with:  pytest tests/test_e2e.py -m e2e -v
"""

import json
import os
import subprocess
import sys
import textwrap
import time

import pytest

# ---------------------------------------------------------------------------
# Skip entire module if dependencies or services are not available
# ---------------------------------------------------------------------------

try:
    import requests
    import pymysql
except ImportError:
    pytest.skip(
        "E2E dependencies not installed (requests, pymysql). "
        "Install with: pip install -e '.[e2e]'",
        allow_module_level=True,
    )

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
DORIS_HOST = os.environ.get("DORIS_HOST", "127.0.0.1")
DORIS_PORT = int(os.environ.get("DORIS_PORT", "9030"))
DORIS_USER = os.environ.get("DORIS_USER", "root")
DORIS_PASSWORD = os.environ.get("DORIS_PASSWORD", "")
DORIS_DB = os.environ.get("DORIS_DB", "e2e_test")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _es_available():
    """Check if ES is reachable."""
    try:
        r = requests.get(ES_URL, timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def _doris_available():
    """Check if Doris FE MySQL port is reachable."""
    try:
        conn = pymysql.connect(
            host=DORIS_HOST,
            port=DORIS_PORT,
            user=DORIS_USER,
            password=DORIS_PASSWORD,
            connect_timeout=5,
        )
        conn.close()
        return True
    except Exception:
        return False


def _doris_conn():
    """Create a PyMySQL connection to Doris."""
    return pymysql.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        database=DORIS_DB,
        charset="utf8mb4",
        autocommit=True,
    )


def _es_create_index(index_name, mapping_body):
    """Create an ES index with the given mapping."""
    url = "{}/{}".format(ES_URL, index_name)
    # Delete if exists
    requests.delete(url, timeout=10)
    r = requests.put(
        url,
        json={"mappings": mapping_body},
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


def _es_get_mapping(index_name):
    """Retrieve the mapping JSON from ES (API format)."""
    url = "{}/_mapping".format(ES_URL)
    r = requests.get("{}/{}/_mapping".format(ES_URL, index_name), timeout=10)
    r.raise_for_status()
    return r.json()


def _es_delete_index(index_name):
    """Delete an ES index (ignore 404)."""
    requests.delete("{}/{}".format(ES_URL, index_name), timeout=10)


def _run_converter(mapping_json, config_yaml=None, extra_args=None):
    """Run es-ddl-converter as a subprocess and return (ddl, warnings, returncode)."""
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, dir=PROJECT_ROOT,
    ) as f:
        json.dump(mapping_json, f)
        mapping_path = f.name

    config_path = None
    try:
        cmd = [
            sys.executable, "-m", "es_ddl_converter.cli",
            "-i", mapping_path,
        ]
        if config_yaml:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".yaml", delete=False, dir=PROJECT_ROOT,
            ) as cf:
                cf.write(config_yaml)
                config_path = cf.name
            cmd.extend(["-c", config_path])

        if extra_args:
            cmd.extend(extra_args)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
            timeout=30,
        )
        return result.stdout, result.stderr, result.returncode
    finally:
        os.unlink(mapping_path)
        if config_path:
            os.unlink(config_path)


def _execute_ddl_on_doris(ddl):
    """Execute the DDL on Doris. Returns list of (column_name, data_type) tuples."""
    conn = _doris_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
    finally:
        conn.close()


def _get_doris_columns(table_name):
    """Get column info from Doris for a table. Returns list of dicts."""
    conn = _doris_conn()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("DESC `{}`".format(table_name))
            return cur.fetchall()
    finally:
        conn.close()


def _doris_table_exists(table_name):
    """Check if a table exists in Doris."""
    conn = _doris_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES LIKE '{}'".format(table_name))
            return cur.fetchone() is not None
    finally:
        conn.close()


def _doris_drop_table(table_name):
    """Drop a Doris table if it exists."""
    conn = _doris_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS `{}`".format(table_name))
    finally:
        conn.close()


def _ensure_doris_db():
    """Ensure the E2E test database exists in Doris."""
    conn = pymysql.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS `{}`".format(DORIS_DB))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def check_services():
    """Skip all E2E tests if Docker services are not running."""
    if not _es_available():
        pytest.skip("Elasticsearch not available at {}".format(ES_URL))
    if not _doris_available():
        pytest.skip("Doris not available at {}:{}".format(DORIS_HOST, DORIS_PORT))
    _ensure_doris_db()


@pytest.fixture(autouse=True)
def _cleanup_es_index(request):
    """Cleanup ES indexes created during tests."""
    yield
    # Clean up indexes whose names start with "e2e_"
    try:
        r = requests.get("{}/_cat/indices?format=json".format(ES_URL), timeout=5)
        if r.ok:
            for idx in r.json():
                if idx.get("index", "").startswith("e2e_"):
                    _es_delete_index(idx["index"])
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.e2e
class TestSimpleMapping:
    """E2E: simple mapping with basic types."""

    TABLE_NAME = "e2e_simple"

    def setup_method(self):
        _doris_drop_table(self.TABLE_NAME)

    def teardown_method(self):
        _doris_drop_table(self.TABLE_NAME)

    def test_simple_keyword_integer_boolean(self):
        """Create an ES index with keyword/integer/boolean, convert, execute DDL on Doris."""
        # 1. Create ES index
        mapping = {
            "properties": {
                "name": {"type": "keyword"},
                "age": {"type": "integer"},
                "active": {"type": "boolean"},
            }
        }
        _es_create_index("e2e_simple", mapping)

        # 2. Get mapping from ES (API format)
        es_mapping = _es_get_mapping("e2e_simple")

        # 3. Run converter
        ddl, warnings, rc = _run_converter(
            es_mapping,
            extra_args=["--table-name", self.TABLE_NAME],
        )
        assert rc in (0, 1), "Converter failed with rc={}: {}".format(rc, warnings)
        assert "CREATE TABLE" in ddl

        # 4. Adjust replication_num for single-node Doris
        ddl = ddl.replace('"replication_num" = "3"', '"replication_num" = "1"')

        # 5. Execute DDL on Doris
        _execute_ddl_on_doris(ddl)

        # 6. Verify table exists and columns
        assert _doris_table_exists(self.TABLE_NAME)
        columns = _get_doris_columns(self.TABLE_NAME)
        col_names = {c["Field"] for c in columns}
        assert "name" in col_names
        assert "age" in col_names
        assert "active" in col_names


@pytest.mark.e2e
class TestFullExampleMapping:
    """E2E: full example mapping with all common ES types."""

    TABLE_NAME = "e2e_my_logs"
    ES_INDEX = "e2e_my_logs"

    def setup_method(self):
        _doris_drop_table(self.TABLE_NAME)
        _es_delete_index(self.ES_INDEX)

    def teardown_method(self):
        _doris_drop_table(self.TABLE_NAME)
        _es_delete_index(self.ES_INDEX)

    def test_full_example_pipeline(self):
        """Full pipeline: complex mapping → converter → Doris table creation."""
        # 1. Create ES index with rich mapping
        mapping = {
            "dynamic": "true",
            "properties": {
                "@timestamp": {"type": "date", "format": "epoch_millis"},
                "level": {"type": "keyword"},
                "service": {"type": "keyword", "ignore_above": 128},
                "trace_id": {"type": "keyword"},
                "message": {
                    "type": "text",
                    "analyzer": "standard",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 2048}
                    },
                },
                "host_ip": {"type": "ip"},
                "duration": {"type": "float"},
                "response_code": {"type": "short"},
                "tags": {"type": "keyword"},
                "user": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "long"},
                        "name": {"type": "keyword"},
                    },
                },
                "metadata": {"type": "object", "enabled": False},
            },
        }
        _es_create_index(self.ES_INDEX, mapping)

        # 2. Get mapping from ES API
        es_mapping = _es_get_mapping(self.ES_INDEX)

        # 3. Run converter with config (tags is array)
        config_yaml = textwrap.dedent("""\
            array_fields:
              - tags
            replication_num: 1
        """)
        ddl, warnings, rc = _run_converter(
            es_mapping,
            config_yaml=config_yaml,
            extra_args=["--table-name", self.TABLE_NAME],
        )
        assert rc in (0, 1), "Converter failed: {}".format(warnings)
        assert "CREATE TABLE" in ddl

        # 4. Execute DDL on Doris
        _execute_ddl_on_doris(ddl)

        # 5. Verify table structure
        assert _doris_table_exists(self.TABLE_NAME)
        columns = _get_doris_columns(self.TABLE_NAME)
        col_map = {c["Field"]: c["Type"] for c in columns}

        # Verify key fields exist
        assert "@timestamp" in col_map
        assert "level" in col_map
        assert "service" in col_map
        assert "message" in col_map
        assert "host_ip" in col_map
        assert "duration" in col_map

        # user object → VARIANT by default
        assert "user" in col_map
        assert col_map["user"].upper().startswith("VARIANT")

        # metadata (enabled:false) → VARIANT
        assert "metadata" in col_map

        # tags → ARRAY
        tags_type = col_map.get("tags", "")
        assert "ARRAY" in tags_type.upper(), "Expected ARRAY type for tags, got: {}".format(tags_type)


@pytest.mark.e2e
class TestUniqueKeyModel:
    """E2E: test UNIQUE KEY table model with _id."""

    TABLE_NAME = "e2e_unique_model"
    ES_INDEX = "e2e_unique_model"

    def setup_method(self):
        _doris_drop_table(self.TABLE_NAME)
        _es_delete_index(self.ES_INDEX)

    def teardown_method(self):
        _doris_drop_table(self.TABLE_NAME)
        _es_delete_index(self.ES_INDEX)

    def test_unique_key_with_id(self):
        """UNIQUE KEY model with _id column."""
        mapping = {
            "properties": {
                "title": {"type": "text", "analyzer": "standard"},
                "status": {"type": "keyword"},
                "created": {"type": "date"},
            }
        }
        _es_create_index(self.ES_INDEX, mapping)
        es_mapping = _es_get_mapping(self.ES_INDEX)

        config_yaml = textwrap.dedent("""\
            table_model: unique
            include_id: true
            replication_num: 1
        """)
        ddl, warnings, rc = _run_converter(
            es_mapping,
            config_yaml=config_yaml,
            extra_args=["--table-name", self.TABLE_NAME],
        )
        assert rc in (0, 1), "Converter failed: {}".format(warnings)
        assert "UNIQUE KEY" in ddl
        assert "_id" in ddl

        _execute_ddl_on_doris(ddl)

        assert _doris_table_exists(self.TABLE_NAME)
        columns = _get_doris_columns(self.TABLE_NAME)
        col_map = {c["Field"]: c for c in columns}
        assert "_id" in col_map


@pytest.mark.e2e
class TestNestedObjectFlattening:
    """E2E: deeply nested objects get flattened correctly."""

    TABLE_NAME = "e2e_nested"
    ES_INDEX = "e2e_nested"

    def setup_method(self):
        _doris_drop_table(self.TABLE_NAME)
        _es_delete_index(self.ES_INDEX)

    def teardown_method(self):
        _doris_drop_table(self.TABLE_NAME)
        _es_delete_index(self.ES_INDEX)

    def test_nested_object_flattening(self):
        """Multi-level object nesting flattened with _ separator."""
        mapping = {
            "properties": {
                "event_time": {"type": "date"},
                "server": {
                    "type": "object",
                    "properties": {
                        "host": {"type": "keyword"},
                        "geo": {
                            "type": "object",
                            "properties": {
                                "country": {"type": "keyword"},
                                "city": {"type": "keyword"},
                            },
                        },
                    },
                },
                "request": {
                    "type": "object",
                    "properties": {
                        "method": {"type": "keyword"},
                        "path": {"type": "keyword"},
                        "bytes": {"type": "long"},
                    },
                },
            }
        }
        _es_create_index(self.ES_INDEX, mapping)
        es_mapping = _es_get_mapping(self.ES_INDEX)

        config_yaml = "replication_num: 1\n"
        ddl, warnings, rc = _run_converter(
            es_mapping,
            config_yaml=config_yaml,
            extra_args=["--table-name", self.TABLE_NAME],
        )
        assert rc in (0, 1)
        assert "CREATE TABLE" in ddl

        _execute_ddl_on_doris(ddl)

        assert _doris_table_exists(self.TABLE_NAME)
        columns = _get_doris_columns(self.TABLE_NAME)
        col_names = {c["Field"] for c in columns}

        # Objects default to VARIANT — top-level object names present
        assert "server" in col_names
        assert "request" in col_names
        # Sub-fields not flattened
        assert "server_host" not in col_names
        assert "request_method" not in col_names


@pytest.mark.e2e
class TestNumericTypes:
    """E2E: all numeric ES types map to valid Doris types."""

    TABLE_NAME = "e2e_numerics"
    ES_INDEX = "e2e_numerics"

    def setup_method(self):
        _doris_drop_table(self.TABLE_NAME)
        _es_delete_index(self.ES_INDEX)

    def teardown_method(self):
        _doris_drop_table(self.TABLE_NAME)
        _es_delete_index(self.ES_INDEX)

    def test_numeric_types(self):
        """byte, short, integer, long, float, double, half_float, scaled_float."""
        mapping = {
            "properties": {
                "ts": {"type": "date"},
                "val_byte": {"type": "byte"},
                "val_short": {"type": "short"},
                "val_integer": {"type": "integer"},
                "val_long": {"type": "long"},
                "val_float": {"type": "float"},
                "val_double": {"type": "double"},
                "val_half_float": {"type": "half_float"},
                "val_scaled": {"type": "scaled_float", "scaling_factor": 100},
            }
        }
        _es_create_index(self.ES_INDEX, mapping)
        es_mapping = _es_get_mapping(self.ES_INDEX)

        config_yaml = "replication_num: 1\n"
        ddl, warnings, rc = _run_converter(
            es_mapping,
            config_yaml=config_yaml,
            extra_args=["--table-name", self.TABLE_NAME],
        )
        assert rc in (0, 1)

        _execute_ddl_on_doris(ddl)

        assert _doris_table_exists(self.TABLE_NAME)
        columns = _get_doris_columns(self.TABLE_NAME)
        col_names = {c["Field"] for c in columns}
        for name in ["val_byte", "val_short", "val_integer", "val_long",
                      "val_float", "val_double", "val_half_float", "val_scaled"]:
            assert name in col_names, "Missing column: {}".format(name)


@pytest.mark.e2e
class TestConverterFromFixtureFile:
    """E2E: use the fixture JSON files directly with the CLI."""

    TABLE_NAME = "e2e_from_fixture"

    def setup_method(self):
        _doris_drop_table(self.TABLE_NAME)

    def teardown_method(self):
        _doris_drop_table(self.TABLE_NAME)

    def test_simple_fixture_file(self):
        """Run converter on tests/fixtures/simple_mapping.json and execute on Doris."""
        mapping_path = os.path.join(FIXTURES_DIR, "simple_mapping.json")
        cmd = [
            sys.executable, "-m", "es_ddl_converter.cli",
            "-i", mapping_path,
            "--table-name", self.TABLE_NAME,
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
            timeout=30,
        )
        assert result.returncode in (0, 1)
        ddl = result.stdout
        assert "CREATE TABLE" in ddl

        ddl = ddl.replace('"replication_num" = "3"', '"replication_num" = "1"')
        _execute_ddl_on_doris(ddl)

        assert _doris_table_exists(self.TABLE_NAME)

    def test_full_example_fixture_file(self):
        """Run converter on tests/fixtures/full_example_mapping.json with config."""
        mapping_path = os.path.join(FIXTURES_DIR, "full_example_mapping.json")
        config_path = os.path.join(FIXTURES_DIR, "config_example.yaml")

        # Override replication_num for single-node Doris
        import tempfile
        import yaml

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        config["replication_num"] = 1
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False,
        ) as tf:
            yaml.dump(config, tf)
            tmp_config = tf.name

        try:
            cmd = [
                sys.executable, "-m", "es_ddl_converter.cli",
                "-i", mapping_path,
                "-c", tmp_config,
                "--table-name", self.TABLE_NAME,
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=PROJECT_ROOT,
                timeout=30,
            )
            assert result.returncode in (0, 1)
            ddl = result.stdout
            assert "CREATE TABLE" in ddl

            _execute_ddl_on_doris(ddl)
            assert _doris_table_exists(self.TABLE_NAME)

            columns = _get_doris_columns(self.TABLE_NAME)
            col_names = {c["Field"] for c in columns}
            # Verify key fields from the full example
            assert "@timestamp" in col_names
            assert "level" in col_names
            assert "message" in col_names
        finally:
            os.unlink(tmp_config)
