from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app


def test_api_paths_bypass_spa_static() -> None:
    client = TestClient(app)

    openapi = client.get("/openapi.json")
    assert openapi.status_code == 200
    assert openapi.headers["content-type"].startswith("application/json")

    tiles = client.get("/v1/tiles/parcels/0/0/0.pbf")
    assert tiles.status_code != 404

    health = client.get("/health")
    assert health.status_code == 200

    health_v1 = client.get("/v1/health")
    assert health_v1.status_code == 200


def test_root_path_returns_spa_or_not_500() -> None:
    if not Path("frontend/dist/index.html").is_file():
        pytest.skip("frontend/dist/index.html not present")

    client = TestClient(app)

    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")


def test_non_api_404_routes_fall_back_to_spa_index() -> None:
    if not Path("frontend/dist/index.html").is_file():
        pytest.skip("frontend/dist/index.html not present")

    client = TestClient(app)

    response = client.get("/dashboard/summary")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")


def test_static_assets_are_served_as_files_not_spa_html() -> None:
    if not Path("frontend/dist/index.html").is_file():
        pytest.skip("frontend/dist/index.html not present")

    assets_dir = Path("frontend/dist/assets")
    asset_path = next(assets_dir.glob("*.js"), None)
    if asset_path is None:
        pytest.skip("frontend/dist/assets/*.js not present")

    client = TestClient(app)
    response = client.get(f"/assets/{asset_path.name}")

    assert response.status_code == 200
    content_type = response.headers.get("content-type", "").lower()
    assert "javascript" in content_type or not response.text.lower().startswith(
        "<!doctype html"
    )


def test_api_404_still_returns_json_not_spa_html() -> None:
    client = TestClient(app)

    api_response = client.get("/v1/does-not-exist")
    assert api_response.status_code == 404
    assert api_response.headers["content-type"].startswith("application/json")
