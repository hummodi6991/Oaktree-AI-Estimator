from fastapi.testclient import TestClient

from app.main import app


def test_health() -> None:
    client = TestClient(app)
    response = client.get("/health")
    v1_response = client.get("/v1/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert v1_response.status_code == 200
    assert v1_response.json()["status"] == "ok"
