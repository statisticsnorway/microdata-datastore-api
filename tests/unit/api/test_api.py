from fastapi import testclient


def test_client_sends_x_request_id(test_app: testclient.TestClient):
    response = test_app.get("/health/alive", headers={"X-Request-ID": "abc123"})
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "abc123"


def test_client_does_not_send_x_request_id(test_app: testclient.TestClient):
    response = test_app.get("/health/alive")
    assert response.status_code == 200
    assert response.headers["X-Request-ID"]


def test_not_found(test_app: testclient.TestClient):
    response = test_app.get("/no/such/path")
    assert response.status_code == 400
    assert response.json()["code"] == 103
    assert response.json()["service"] == "datastore-api"
    assert response.json()["type"] == "PATH_NOT_FOUND"
    assert "Error: " in response.json()["message"]
    assert response.headers["X-Request-ID"]
