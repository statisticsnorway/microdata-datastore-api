from fastapi.testclient import TestClient

from datastore_api.main import app

client = TestClient(app)


def test_alive():
    response = client.get("/health/alive")
    assert response.status_code == 200
    assert response.text == '"I\'m alive!"'


def test_ready():
    response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.text == '"I\'m ready!"'
