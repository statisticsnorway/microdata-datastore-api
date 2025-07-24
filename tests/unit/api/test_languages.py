from fastapi import testclient
from httpx import Response

from datastore_api.domain import languages

MOCKED_LANGUAGES = [
    {"code": "no", "label": "Norsk"},
    {"code": "en", "label": "English"},
]


def test_get_languages(test_app: testclient.TestClient, mocker):
    spy = mocker.patch.object(
        languages, "find_languages", return_value=MOCKED_LANGUAGES
    )
    response: Response = test_app.get(
        "/languages",
        headers={"X-Request-ID": "test-123"},
    )
    spy.assert_called()
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == MOCKED_LANGUAGES
