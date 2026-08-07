from datetime import datetime, timedelta

import pytest

from datastore_api.adapter.auth import (
    AuthClient,
    MicrodataAuthClient,
    _decode_jwt,
)
from datastore_api.adapter.auth.dependencies import (
    ACCREDITATION_TOKEN_POLICY,
    DATA_ADMINISTRATOR_ROLE,
    valid_aud_jobs,
)
from datastore_api.common.exceptions import AuthError
from tests.resources import test_resources
from tests.utils.util import encode_jwt_payload, generate_rsa_key_pairs

JWT_PRIVATE_KEY, JWT_PUBLIC_KEY = generate_rsa_key_pairs()


@pytest.fixture
def auth_client() -> AuthClient:
    auth_client = MicrodataAuthClient()
    auth_client._get_signing_key = lambda jwt_token: JWT_PUBLIC_KEY.decode(
        "utf-8"
    )  # type: ignore
    return auth_client


def test_auth_valid_token(auth_client):
    token = encode_jwt_payload(
        test_resources.valid_jwt_payload, JWT_PRIVATE_KEY
    )
    auth_client.authorize_jwt(
        required_aud=valid_aud_jobs,
        decode_policy=ACCREDITATION_TOKEN_POLICY,
        required_role=DATA_ADMINISTRATOR_ROLE,
        authorization_token=token,
        rdn="no.ssb.fdb",
    )


def test_auth_valid_token_root_aud(auth_client):
    payload = {
        **test_resources.valid_jwt_payload,
        "aud": ["no", "datastore-api-jobs"],
    }
    token = encode_jwt_payload(payload, JWT_PRIVATE_KEY)
    auth_client.authorize_jwt(
        required_aud=valid_aud_jobs,
        decode_policy=ACCREDITATION_TOKEN_POLICY,
        required_role=DATA_ADMINISTRATOR_ROLE,
        authorization_token=token,
        rdn="no.testdatastore",
    )


def test_auth_root_aud_does_not_match_unrelated_rdn(auth_client):
    payload = {
        **test_resources.valid_jwt_payload,
        "aud": ["no", "datastore-api-jobs"],
    }
    token = encode_jwt_payload(payload, JWT_PRIVATE_KEY)
    with pytest.raises(AuthError) as e:
        auth_client.authorize_jwt(
            required_aud=valid_aud_jobs,
            decode_policy=ACCREDITATION_TOKEN_POLICY,
            required_role=DATA_ADMINISTRATOR_ROLE,
            authorization_token=token,
            rdn="nope.evil.datastore",
        )
    assert "Not authorized to access datastore: nope.evil.datastore" in str(e)


def test_auth_no_datastore_in_aud(auth_client):
    payload = {
        **test_resources.valid_jwt_payload,
        "aud": ["datastore-api-jobs"],
    }
    token = encode_jwt_payload(payload, JWT_PRIVATE_KEY)
    with pytest.raises(AuthError) as e:
        auth_client.authorize_jwt(
            required_aud=valid_aud_jobs,
            decode_policy=ACCREDITATION_TOKEN_POLICY,
            required_role=DATA_ADMINISTRATOR_ROLE,
            authorization_token=token,
            rdn="no.ssb.fdb",
        )
    assert "Not authorized to access datastore: no.ssb.fdb" in str(e)


def test_auth_datastore_not_in_audience(auth_client):
    payload = {
        **test_resources.valid_jwt_payload,
        "aud": ["no.ssb.fdb", "datastore-api-jobs"],
    }
    token = encode_jwt_payload(payload, JWT_PRIVATE_KEY)
    with pytest.raises(AuthError) as e:
        auth_client.authorize_jwt(
            required_aud=valid_aud_jobs,
            decode_policy=ACCREDITATION_TOKEN_POLICY,
            required_role=DATA_ADMINISTRATOR_ROLE,
            authorization_token=token,
            rdn="no.provider1.test",
        )
    assert "Not authorized to access datastore: no.provider1.test" in str(e)


def test_auth_no_audience(auth_client):
    payload = {
        **test_resources.valid_jwt_payload,
        "aud": [],
    }
    token = encode_jwt_payload(payload, JWT_PRIVATE_KEY)
    with pytest.raises(AuthError) as e:
        auth_client.authorize_jwt(
            required_aud=valid_aud_jobs,
            decode_policy=ACCREDITATION_TOKEN_POLICY,
            required_role=DATA_ADMINISTRATOR_ROLE,
            authorization_token=token,
            rdn="no.ssb.fdb",
        )
    assert 'Invalid token: Token is missing the "aud" claim' in str(e)


def test_auth_no_accreditation_role(auth_client):
    token = encode_jwt_payload(
        test_resources.jwt_payload_no_accreditation_role, JWT_PRIVATE_KEY
    )
    with pytest.raises(AuthError) as e:
        auth_client.authorize_jwt(
            required_aud=valid_aud_jobs,
            decode_policy=ACCREDITATION_TOKEN_POLICY,
            required_role=DATA_ADMINISTRATOR_ROLE,
            authorization_token=token,
            rdn="no.ssb.fdb",
        )
    assert (
        'Invalid token: Token is missing the "accreditation/role" claim'
        in str(e)
    )


def test_auth_wrong_role(auth_client):
    payload = {
        **test_resources.valid_jwt_payload,
        "accreditation/role": "role/invalidrole",
    }
    token = encode_jwt_payload(payload, JWT_PRIVATE_KEY)
    with pytest.raises(AuthError) as e:
        auth_client.authorize_jwt(
            required_aud=valid_aud_jobs,
            decode_policy=ACCREDITATION_TOKEN_POLICY,
            required_role=DATA_ADMINISTRATOR_ROLE,
            authorization_token=token,
            rdn="no.ssb.fdb",
        )
    assert "Unauthorized with role: role/invalidrole" in str(e)


def test_auth_wrong_audience(auth_client):
    payload = {
        **test_resources.valid_jwt_payload,
        "aud": ["wrong-audience"],
    }
    token = encode_jwt_payload(payload, JWT_PRIVATE_KEY)
    with pytest.raises(AuthError) as e:
        auth_client.authorize_jwt(
            required_aud=valid_aud_jobs,
            decode_policy=ACCREDITATION_TOKEN_POLICY,
            required_role=DATA_ADMINISTRATOR_ROLE,
            authorization_token=token,
            rdn="no.ssb.fdb",
        )
    assert "Invalid token: Audience doesn't match" in str(e)


def test_auth_expired_token(auth_client):
    payload = {
        **test_resources.valid_jwt_payload,
        "exp": (datetime.now() - timedelta(hours=1)).timestamp(),
    }
    token = encode_jwt_payload(payload, JWT_PRIVATE_KEY)
    with pytest.raises(AuthError) as e:
        auth_client.authorize_jwt(
            required_aud=valid_aud_jobs,
            decode_policy=ACCREDITATION_TOKEN_POLICY,
            required_role=DATA_ADMINISTRATOR_ROLE,
            authorization_token=token,
            rdn="no.ssb.fdb",
        )
    assert "Invalid token: Signature has expired" in str(e)


def test_auth_invalid_signature(auth_client):
    second_private_key, _ = generate_rsa_key_pairs()
    token = encode_jwt_payload(
        test_resources.valid_jwt_payload, second_private_key
    )
    with pytest.raises(AuthError) as e:
        auth_client.authorize_jwt(
            required_aud=valid_aud_jobs,
            decode_policy=ACCREDITATION_TOKEN_POLICY,
            required_role=DATA_ADMINISTRATOR_ROLE,
            authorization_token=token,
            rdn="no.ssb.fdb",
        )
    assert "Invalid token: Signature verification failed" in str(e)


def test_auth_missing_token(auth_client):
    with pytest.raises(AuthError) as e:
        auth_client.authorize_jwt(
            required_aud=valid_aud_jobs,
            decode_policy=ACCREDITATION_TOKEN_POLICY,
            required_role=DATA_ADMINISTRATOR_ROLE,
            authorization_token=None,
        )
    assert "Unauthorized. No token was provided" in str(e)


def test_decoding_signing_key():
    token = encode_jwt_payload(
        test_resources.valid_jwt_payload, JWT_PRIVATE_KEY
    )
    # Signing key can not be None if we are verifying signature
    with pytest.raises(AuthError) as e:
        _decode_jwt(
            jwt_token=token,
            policy=ACCREDITATION_TOKEN_POLICY,
            verify_signature=True,
            signing_key=None,
        )
    assert "Signing key required when verify_signature=True" in str(e)
    # Signing key can be None if we are not verifying signature
    _decode_jwt(
        jwt_token=token,
        policy=ACCREDITATION_TOKEN_POLICY,
        verify_signature=False,
        signing_key=None,
        audience="no.ssb.fdb",
    )
    # Signing key must be something if we are verifying signature
    _decode_jwt(
        jwt_token=token,
        policy=ACCREDITATION_TOKEN_POLICY,
        verify_signature=True,
        signing_key=JWT_PUBLIC_KEY.decode("utf-8"),  # pyright: ignore
        audience="no.ssb.fdb",
    )
