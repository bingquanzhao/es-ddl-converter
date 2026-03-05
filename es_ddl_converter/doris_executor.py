"""Execute Doris DDL statements via MySQL protocol."""

import logging
import re
from typing import Any, Optional

import pymysql
import pymysql.err

logger = logging.getLogger(__name__)

_UNSET = object()  # sentinel: "use self._database"

# Doris identifier: letters, digits, underscores, hyphens; must not be empty.
_IDENTIFIER_RE = re.compile(r'^[A-Za-z0-9_][A-Za-z0-9_\-]*$')


def _validate_identifier(value, label):
    # type: (str, str) -> None
    """Raise DorisExecutorError if *value* is not a safe Doris identifier."""
    if not _IDENTIFIER_RE.match(value):
        raise DorisExecutorError(
            "Invalid {}: '{}'. Only letters, digits, underscores, and hyphens are allowed.".format(
                label, value
            )
        )


class DorisExecutorError(Exception):
    """Raised on Doris DDL execution errors."""


class DorisExecutor:
    """Executes CREATE TABLE DDL on Apache Doris via MySQL protocol."""

    def __init__(
        self,
        host="127.0.0.1",  # type: str
        port=9030,          # type: int
        user="root",        # type: str
        password="",        # type: str
        database=None,      # type: Optional[str]
        timeout=30,         # type: int
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._timeout = timeout
        self._conn = None   # persistent connection; set by connect()

    def _connect(self, database=_UNSET):
        """Open and return a new raw connection.

        Pass ``database=None`` to connect without selecting any database
        (e.g. for CREATE DATABASE). Omit the argument to use ``self._database``.
        """
        db = self._database if database is _UNSET else database
        return pymysql.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=db,
            charset="utf8mb4",
            autocommit=True,
            connect_timeout=self._timeout,
        )

    def connect(self):
        # type: () -> None
        """Establish a persistent connection for batch operations.

        Call once before a batch; use as a context manager (``with executor:``)
        to ensure the connection is closed on exit.
        """
        self._conn = self._connect()

    def close(self):
        # type: () -> None
        """Close the persistent connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def test_connection(self):
        # type: () -> None
        """Verify that Doris is reachable."""
        try:
            conn = self._connect(database=None)
            conn.close()
        except Exception as e:
            raise DorisExecutorError(
                "Cannot connect to Doris at {}:{}: {}".format(
                    self._host, self._port, e
                )
            )

    def ensure_database(self):
        # type: () -> None
        """Create the target database if it does not exist."""
        if not self._database:
            return
        _validate_identifier(self._database, "database name")
        try:
            conn = self._connect(database=None)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "CREATE DATABASE IF NOT EXISTS `{}`".format(self._database)
                    )
            finally:
                conn.close()
        except DorisExecutorError:
            raise
        except Exception as e:
            raise DorisExecutorError(
                "Failed to create database '{}': {}".format(self._database, e)
            )

    def _run(self, fn, one_off=False):
        # type: (Any, bool) -> Any
        """Execute fn(cursor) using the persistent connection when available.

        If ``one_off=True``, always use a temporary connection (useful for
        operations that need a different database context, e.g. SHOW BACKENDS).

        On ``OperationalError`` (broken pipe, server gone away) the persistent
        connection is re-established and the call is retried once.
        """
        if one_off or self._conn is None:
            conn = self._connect(database=None) if one_off else self._connect()
            try:
                with conn.cursor() as cur:
                    return fn(cur)
            finally:
                conn.close()
        else:
            try:
                with self._conn.cursor() as cur:
                    return fn(cur)
            except pymysql.err.OperationalError:
                logger.debug("Persistent connection lost, reconnecting...")
                self._conn = self._connect()
                with self._conn.cursor() as cur:
                    return fn(cur)

    def get_alive_be_count(self):
        # type: () -> int
        """Return the number of alive Backend nodes reported by Doris."""
        def _query(cur):
            cur.execute("SHOW BACKENDS")
            rows = cur.fetchall()
            try:
                headers = [d[0].lower() for d in cur.description]
                alive_idx = headers.index("alive")
                return sum(1 for r in rows if str(r[alive_idx]).lower() == "true")
            except (ValueError, TypeError):
                return len(rows)

        try:
            return self._run(_query, one_off=True)
        except DorisExecutorError:
            raise
        except Exception as e:
            raise DorisExecutorError("Failed to query BE count: {}".format(e))

    def execute_ddl(self, ddl):
        # type: (str) -> None
        """Execute a single DDL statement on Doris."""
        try:
            self._run(lambda cur: cur.execute(ddl))
        except pymysql.err.OperationalError as e:
            raise DorisExecutorError("DDL execution failed: {}".format(e))
        except DorisExecutorError:
            raise
        except Exception as e:
            raise DorisExecutorError("DDL execution failed: {}".format(e))
