from unittest.mock import Mock

import pytest

from datastore_api.adapter.auth import MicrodataAuthClient
from datastore_api.adapter.db.models import UserInfo
from datastore_api.common.exceptions import AuthError

USER_INFO_ALLOWED_USER = UserInfo(
    user_id="123-123-125", first_name="Data", last_name="Admin"
)
USER_INFO_DISALLOWED_USER = UserInfo(
    user_id="111-111-111", first_name="Data", last_name="Admin"
)


@pytest.fixture
def auth_client():
    return MicrodataAuthClient(
        valid_aud_jobs="test-aud",
        valid_aud_data="test-aud",
    )


def test_authorize_datastore_modification_ok(auth_client):
    auth_client.authorize_data_administrator = Mock(
        return_value=USER_INFO_ALLOWED_USER
    )
    user = auth_client.authorize_datastore_modification(
        authorization_cookie=None,
        user_info_cookie=None,
    )
    assert user.user_id == "123-123-125"


def test_authorize_datastore_modification_forbidden(auth_client):
    auth_client.authorize_data_administrator = Mock(
        return_value=USER_INFO_DISALLOWED_USER
    )
    with pytest.raises(AuthError):
        auth_client.authorize_datastore_modification(
            authorization_cookie=None, user_info_cookie=None
        )
