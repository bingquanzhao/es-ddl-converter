"""Tests for es_client.py using mocked requests."""

from unittest.mock import MagicMock, patch

import pytest

from es_ddl_converter.es_client import ESClient, ESClientError


@pytest.fixture
def mock_session():
    with patch("requests.Session") as MockSession:
        session = MagicMock()
        MockSession.return_value = session
        yield session


class TestESClient:

    def test_init_sets_base_url(self, mock_session):
        client = ESClient("http://localhost:9200")
        assert client._base_url == "http://localhost:9200"

    def test_init_strips_trailing_slash(self, mock_session):
        client = ESClient("http://localhost:9200/")
        assert client._base_url == "http://localhost:9200"

    def test_init_sets_auth(self, mock_session):
        client = ESClient("http://localhost:9200", username="user", password="pass")
        assert mock_session.auth == ("user", "pass")

    def test_get_all_mappings(self, mock_session):
        resp = MagicMock()
        resp.json.return_value = {
            "idx": {"mappings": {"properties": {"f": {"type": "keyword"}}}}
        }
        resp.raise_for_status = MagicMock()
        mock_session.get.return_value = resp

        client = ESClient("http://localhost:9200")
        result = client.get_all_mappings()
        assert "idx" in result
        mock_session.get.assert_called_once()
        call_url = mock_session.get.call_args[0][0]
        assert "/_mapping" in call_url

    def test_get_all_mappings_with_pattern(self, mock_session):
        resp = MagicMock()
        resp.json.return_value = {}
        resp.raise_for_status = MagicMock()
        mock_session.get.return_value = resp

        client = ESClient("http://localhost:9200")
        client.get_all_mappings("logs-*")
        call_url = mock_session.get.call_args[0][0]
        assert "logs-*/_mapping" in call_url

    def test_get_all_mappings_error(self, mock_session):
        mock_session.get.side_effect = Exception("connection refused")

        client = ESClient("http://localhost:9200")
        with pytest.raises(ESClientError, match="Failed to fetch"):
            client.get_all_mappings()

    def test_list_indexes(self, mock_session):
        resp = MagicMock()
        resp.json.return_value = [
            {"index": "logs"},
            {"index": "users"},
            {"index": ".kibana"},
        ]
        resp.raise_for_status = MagicMock()
        mock_session.get.return_value = resp

        client = ESClient("http://localhost:9200")
        indexes = client.list_indexes()
        assert indexes == ["logs", "users"]
        assert ".kibana" not in indexes

    def test_list_indexes_include_system(self, mock_session):
        resp = MagicMock()
        resp.json.return_value = [
            {"index": "logs"},
            {"index": ".kibana"},
        ]
        resp.raise_for_status = MagicMock()
        mock_session.get.return_value = resp

        client = ESClient("http://localhost:9200")
        indexes = client.list_indexes(include_system=True)
        assert ".kibana" in indexes

    def test_get_cluster_info(self, mock_session):
        resp = MagicMock()
        resp.json.return_value = {"cluster_name": "test", "version": {"number": "7.17.25"}}
        resp.raise_for_status = MagicMock()
        mock_session.get.return_value = resp

        client = ESClient("http://localhost:9200")
        info = client.get_cluster_info()
        assert info["cluster_name"] == "test"

    def test_get_cluster_info_error(self, mock_session):
        mock_session.get.side_effect = Exception("timeout")

        client = ESClient("http://localhost:9200")
        with pytest.raises(ESClientError, match="Cannot connect"):
            client.get_cluster_info()
