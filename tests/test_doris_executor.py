"""Tests for doris_executor.py using mocked pymysql."""

from unittest.mock import MagicMock, patch, call

import pytest

from es_ddl_converter.doris_executor import (
    DorisExecutor,
    DorisExecutorError,
    _validate_identifier,
)


@pytest.fixture
def mock_pymysql():
    mock = MagicMock()
    mock.err.OperationalError = type("OperationalError", (Exception,), {})
    with patch("es_ddl_converter.doris_executor.pymysql", mock):
        yield mock


class TestDorisExecutor:

    def test_init(self, mock_pymysql):
        executor = DorisExecutor(
            host="doris.local",
            port=9030,
            user="admin",
            password="secret",
            database="mydb",
        )
        assert executor._host == "doris.local"
        assert executor._database == "mydb"

    def test_test_connection_success(self, mock_pymysql):
        conn = MagicMock()
        mock_pymysql.connect.return_value = conn

        executor = DorisExecutor()
        executor.test_connection()
        mock_pymysql.connect.assert_called_once()
        conn.close.assert_called_once()

    def test_test_connection_failure(self, mock_pymysql):
        mock_pymysql.connect.side_effect = Exception("refused")

        executor = DorisExecutor()
        with pytest.raises(DorisExecutorError, match="Cannot connect"):
            executor.test_connection()

    def test_ensure_database(self, mock_pymysql):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pymysql.connect.return_value = conn

        executor = DorisExecutor(database="test_db")
        executor.ensure_database()
        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert "CREATE DATABASE" in sql
        assert "test_db" in sql

    def test_ensure_database_no_db(self, mock_pymysql):
        executor = DorisExecutor(database=None)
        executor.ensure_database()
        # Should be a no-op, no connect call
        mock_pymysql.connect.assert_not_called()

    def test_execute_ddl(self, mock_pymysql):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pymysql.connect.return_value = conn

        executor = DorisExecutor(database="mydb")
        executor.execute_ddl("CREATE TABLE test (id INT);")
        cursor.execute.assert_called_once_with("CREATE TABLE test (id INT);")
        conn.close.assert_called_once()

    def test_execute_ddl_failure(self, mock_pymysql):
        mock_pymysql.connect.side_effect = Exception("table exists")

        executor = DorisExecutor(database="mydb")
        with pytest.raises(DorisExecutorError, match="DDL execution failed"):
            executor.execute_ddl("CREATE TABLE ...")


# ---------------------------------------------------------------------------
# _validate_identifier — SQL injection prevention
# ---------------------------------------------------------------------------

class TestValidateIdentifier:

    def test_valid_simple_name(self):
        _validate_identifier("mydb", "database name")  # should not raise

    def test_valid_with_underscore(self):
        _validate_identifier("my_db_01", "database name")

    def test_valid_with_hyphen(self):
        _validate_identifier("my-db", "database name")

    def test_valid_uppercase(self):
        _validate_identifier("MyDB", "database name")

    def test_rejects_backtick(self):
        with pytest.raises(DorisExecutorError, match="Invalid database name"):
            _validate_identifier("test`; DROP DATABASE foo; --", "database name")

    def test_rejects_semicolon(self):
        with pytest.raises(DorisExecutorError, match="Invalid database name"):
            _validate_identifier("db; DROP TABLE t", "database name")

    def test_rejects_space(self):
        with pytest.raises(DorisExecutorError, match="Invalid database name"):
            _validate_identifier("my db", "database name")

    def test_rejects_empty_string(self):
        with pytest.raises(DorisExecutorError, match="Invalid database name"):
            _validate_identifier("", "database name")

    def test_rejects_dot(self):
        with pytest.raises(DorisExecutorError, match="Invalid database name"):
            _validate_identifier("db.schema", "database name")


class TestGetAliveBeCount:

    def _make_cursor(self, mock_pymysql, description, rows):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.description = description
        cursor.fetchall.return_value = rows
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pymysql.connect.return_value = conn
        return conn, cursor

    def test_counts_only_alive_backends(self, mock_pymysql):
        """Returns count of rows where Alive == 'true'."""
        self._make_cursor(
            mock_pymysql,
            description=[("BackendId",), ("Alive",)],
            rows=[(1, "true"), (2, "true"), (3, "false")],
        )
        executor = DorisExecutor()
        assert executor.get_alive_be_count() == 2

    def test_case_insensitive_alive_column(self, mock_pymysql):
        """Alive column name matching is case-insensitive."""
        self._make_cursor(
            mock_pymysql,
            description=[("BackendId",), ("alive",)],
            rows=[(1, "true"), (2, "false")],
        )
        executor = DorisExecutor()
        assert executor.get_alive_be_count() == 1

    def test_fallback_to_row_count_when_no_alive_column(self, mock_pymysql):
        """Falls back to total row count when 'Alive' column is absent."""
        self._make_cursor(
            mock_pymysql,
            description=[("BackendId",), ("Host",)],
            rows=[(1, "host1"), (2, "host2")],
        )
        executor = DorisExecutor()
        assert executor.get_alive_be_count() == 2

    def test_raises_on_connection_failure(self, mock_pymysql):
        mock_pymysql.connect.side_effect = Exception("connection refused")
        executor = DorisExecutor()
        with pytest.raises(DorisExecutorError, match="Failed to query BE count"):
            executor.get_alive_be_count()


class TestEnsureDatabaseValidation:

    def test_invalid_database_name_raises_before_connect(self, mock_pymysql):
        executor = DorisExecutor(database="bad`name")
        with pytest.raises(DorisExecutorError, match="Invalid database name"):
            executor.ensure_database()
        # Must not have attempted a DB connection
        mock_pymysql.connect.assert_not_called()
