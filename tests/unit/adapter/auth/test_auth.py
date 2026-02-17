import pytest

from datastore_api.adapter.auth import (
    TokenPolicy,
    _decode_jwt,
)
from datastore_api.common.exceptions import AuthError
from tests.resources import test_resources
from tests.utils.util import encode_jwt_payload, generate_rsa_key_pairs

JWT_PRIVATE_KEY, JWT_PUBLIC_KEY = generate_rsa_key_pairs()


ACCREDITATION_TOKEN_POLICY = TokenPolicy(
    user_id_claim="user/uuid",
    required_claims=[
        "aud",
        "sub",
        "accreditation/role",
        "user/uuid",
    ],
)
decode_policy = (ACCREDITATION_TOKEN_POLICY,)


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
