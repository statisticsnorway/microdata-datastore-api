from unittest.mock import Mock

import pytest

from datastore_api.adapter.auth import MicrodataAuthClient
from datastore_api.adapter.auth.dependencies import (
    authorize_datastore_provisioner,
)
from datastore_api.adapter.db.models import UserInfo
from datastore_api.common.exceptions import AuthError

ALLOWED_USER = UserInfo(
    user_id="123-123-125", first_name="Test", last_name="Testerson"
)

DISALLOWED_USER = UserInfo(
    user_id="111-111-111", first_name="Test", last_name="Testerson"
)


@pytest.fixture
def auth_client():
    return MicrodataAuthClient()


def test_authorize_datastore_provisioner_ok(auth_client):
    auth_client.authorize_jwt = Mock(return_value=ALLOWED_USER)
    result = authorize_datastore_provisioner(
        authorization="token",
        user_info="user-info-token",
        auth_client=auth_client,
    )

    assert result == ALLOWED_USER
    auth_client.authorize_jwt.assert_called_once()


def test_authorize_datastore_provisioner_forbidden(auth_client):
    auth_client.authorize_jwt = Mock(return_value=DISALLOWED_USER)
    with pytest.raises(AuthError):
        authorize_datastore_provisioner(
            authorization=None, user_info=None, auth_client=auth_client
        )
