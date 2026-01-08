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
