from unittest.mock import Mock

import pytest

from datastore_api.adapter.auth import AuthContext, MicrodataAuthClient
from datastore_api.common.exceptions import AuthError

AUTH_CONTEXT_ALLOWED_USER = AuthContext(user_id="123-123-125", signing_key="")

AUTH_CONTEXT_DISALLOWED_USER = AuthContext(
    user_id="111-111-111", signing_key=""
)


@pytest.fixture
def auth_client():
    return MicrodataAuthClient(
        valid_aud_jobs="test-aud",
        valid_aud_data="test-aud",
        valid_aud_provision="test-aud",
    )


def test_authorize_datastore_provisioner_ok(auth_client):
    auth_client.authorize = Mock(return_value=AUTH_CONTEXT_ALLOWED_USER)
    auth_client.authorize_datastore_provisioner(
        authorization_cookie=None,
    )


def test_authorize_datastore_provisioner_forbidden(auth_client):
    auth_client.authorize_datastore_user = Mock(
        return_value=AUTH_CONTEXT_DISALLOWED_USER
    )
    with pytest.raises(AuthError):
        auth_client.authorize_datastore_provisioner(authorization_cookie=None)
