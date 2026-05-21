"""Tests for ``ingest_osm_restaurants`` Overpass API call."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.connectors.delivery_platforms import _UA
from app.ingest.restaurant_pois import ingest_osm_restaurants


def test_ingest_osm_restaurants_sends_ua_and_accept_headers():
    fake_response = MagicMock()
    fake_response.json.return_value = {"elements": []}
    fake_response.raise_for_status.return_value = None

    with patch("httpx.post", return_value=fake_response) as mock_post:
        ingest_osm_restaurants(db=MagicMock())

    assert mock_post.call_count == 1
    _, kwargs = mock_post.call_args
    headers = kwargs["headers"]
    assert headers["User-Agent"] == _UA
    assert headers["Accept"] == "application/json"
