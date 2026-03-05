"""Fetch ES index mappings from a live Elasticsearch cluster via HTTP.

Requires ``requests`` (optional dependency at module level; imported on use).
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ESClientError(Exception):
    """Raised on ES communication errors."""


class ESClient:
    """HTTP client for Elasticsearch mapping retrieval."""

    def __init__(
        self,
        base_url,       # type: str
        username=None,  # type: Optional[str]
        password=None,  # type: Optional[str]
        verify_ssl=True,  # type: bool
        timeout=30,     # type: int
    ):
        try:
            import requests as _requests
        except ImportError:
            raise ImportError(
                "The 'requests' package is required for ES connection. "
                "Install with: pip install requests"
            )
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = _requests.Session()
        self._session.verify = verify_ssl
        if username and password:
            self._session.auth = (username, password)

    def get_all_mappings(self, index_pattern="*"):
        # type: (str) -> Dict[str, Any]
        """Fetch mappings for indexes matching the given pattern.

        Args:
            index_pattern: ES index pattern (e.g. ``"*"``, ``"logs-*"``).

        Returns:
            Raw JSON dict from ``GET /{pattern}/_mapping``.
        """
        url = "{}/_mapping".format(self._base_url)
        if index_pattern and index_pattern != "*":
            url = "{}/{}/_mapping".format(self._base_url, index_pattern)
        try:
            resp = self._session.get(url, timeout=self._timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            raise ESClientError("Failed to fetch mappings from {}: {}".format(url, e))

    def list_indexes(self, pattern="*", include_system=False):
        # type: (str, bool) -> List[str]
        """List index names matching the given pattern.

        Args:
            pattern: ES index pattern.
            include_system: If False, skip indexes starting with ``'.'``.

        Returns:
            Sorted list of index names.
        """
        url = "{}/_cat/indices/{}?format=json&h=index".format(
            self._base_url, pattern
        )
        try:
            resp = self._session.get(url, timeout=self._timeout)
            resp.raise_for_status()
            indexes = [item["index"] for item in resp.json()]
            if not include_system:
                indexes = [i for i in indexes if not i.startswith(".")]
            return sorted(indexes)
        except Exception as e:
            raise ESClientError("Failed to list indexes from {}: {}".format(url, e))

    def get_cluster_info(self):
        # type: () -> Dict[str, Any]
        """Get basic cluster info (used for connectivity check)."""
        try:
            resp = self._session.get(self._base_url, timeout=self._timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            raise ESClientError(
                "Cannot connect to ES at {}: {}".format(self._base_url, e)
            )
