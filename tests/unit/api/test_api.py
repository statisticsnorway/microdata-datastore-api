from fastapi import testclient

from datastore_api.adapter.auth.dependencies import authorize_user
from datastore_api.main import app


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
    assert response.status_code == 404
    assert response.json() == {
        "type": "PATH_NOT_FOUND",
        "code": 103,
        "message": "The url in the request points to a path that don't exist.",
    }
    assert response.headers["X-Request-ID"]


def test_unhandled_exception_logs_stack_trace(caplog):
    def _raise_unhandled() -> None:
        raise RuntimeError("unhandled")

    app.dependency_overrides[authorize_user] = _raise_unhandled
    client = testclient.TestClient(app, raise_server_exceptions=False)
    response = client.post(
        "/datastores/no.ssb.test/data/event/stream",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME",
            "startDate": 0,
            "stopDate": 0,
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 500
    error_records = [r for r in caplog.records if r.levelname == "ERROR"]
    assert error_records[-1].exc_info[1].args == ("unhandled",)
