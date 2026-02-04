import logging
from dataclasses import dataclass
from typing import Any, Protocol

import jwt
from fastapi import HTTPException, status
from jwt import MissingRequiredClaimError, PyJWK, PyJWKClient
from jwt.exceptions import (
    DecodeError,
    ExpiredSignatureError,
    InvalidAudienceError,
    InvalidSignatureError,
)

from datastore_api.adapter.db.models import UserInfo
from datastore_api.common.exceptions import AuthError, InternalServerError
from datastore_api.config import environment, secrets

logger = logging.getLogger()

USER_FIRST_NAME_KEY = "user/firstName"
USER_LAST_NAME_KEY = "user/lastName"
USER_ID_KEY = "user/uuid"
ACCREDITATION_ROLE_KEY = "accreditation/role"


@dataclass(frozen=True)
class TokenPolicy:
    user_id_claim: str
    required_claims: list[str] | None = None
    verify_aud: bool = True

    def to_pyjwk_options(self, verify_signature: bool = True) -> dict[str, Any]:
        options = {}
        options["verify_aud"] = self.verify_aud
        options["verify_signature"] = verify_signature
        if self.required_claims:
            options["require"] = self.required_claims
        return options


USER_INFO_TOKEN_POLICY = TokenPolicy(
    user_id_claim=USER_ID_KEY,
    required_claims=[
        USER_ID_KEY,
        USER_FIRST_NAME_KEY,
        USER_LAST_NAME_KEY,
    ],
    verify_aud=False,
)


def _decode_jwt(
    jwt_token: str,
    policy: TokenPolicy,
    signing_key: PyJWK | None,
    audience: str | None = None,
    verify_signature: bool = True,
) -> dict[str, Any]:
    if verify_signature and signing_key is None:
        raise InternalServerError(
            "Signing key required when verify_signature=True"
        )
    signing_key_for_decode: Any = signing_key if verify_signature else None
    try:
        decoded_jwt = jwt.decode(
            jwt_token,
            signing_key_for_decode,
            algorithms=["RS256", "RS512"],
            audience=audience if policy.verify_aud else None,
            options=policy.to_pyjwk_options(verify_signature),
        )
        return decoded_jwt
    except (
        InvalidSignatureError,
        ExpiredSignatureError,
        InvalidAudienceError,
        MissingRequiredClaimError,
        DecodeError,
    ) as e:
        raise AuthError(f"Invalid token: {e}") from e
    except Exception as e:
        raise InternalServerError(f"Internal Server Error {e}") from e


def _validate_role(required_role: str, decoded_jwt: dict[str, Any]) -> None:
    # Ensure that token contains the expected accreditation role
    role = decoded_jwt.get(ACCREDITATION_ROLE_KEY)
    if role != required_role:
        raise AuthError(f"Unauthorized with role: {role}")


def _validate_rdn_in_aud(rdn: str, decoded_jwt: dict[str, Any]) -> None:
    # Ensure the requested datastore RDN is covered by at least one audience
    aud = decoded_jwt.get("aud")
    if not aud:
        raise AuthError("Missing audience")
    aud_list = [aud] if isinstance(aud, str) else aud
    if not any(rdn.startswith(a) for a in aud_list):
        raise AuthError(f"Not authorized to access datastore: {rdn}")


def _validate_and_parse_user_info_from_token(
    user_info_token: str,
    signing_key: PyJWK | None,
    decoded_jwt: dict[str, Any],
    verify_signature: bool = True,
) -> UserInfo:
    # Decode user-info token and verify it belongs to same user as auth token
    decoded_user_info = _decode_jwt(
        jwt_token=user_info_token,
        policy=USER_INFO_TOKEN_POLICY,
        signing_key=signing_key,
        verify_signature=verify_signature,
    )
    user_id = decoded_user_info.get(USER_ID_KEY)
    if decoded_jwt.get(USER_ID_KEY) != user_id:
        raise AuthError("Token mismatch")
    return UserInfo(
        user_id=str(user_id),
        first_name=str(decoded_user_info.get(USER_FIRST_NAME_KEY)),
        last_name=str(decoded_user_info.get(USER_LAST_NAME_KEY)),
    )


def _validate_user_id(decoded_jwt: dict[str, Any]) -> None:
    # Ensure token contains a non-empty subject identifier
    user_id = decoded_jwt.get("sub")
    if user_id in [None, ""]:
        raise AuthError("No valid user_id")


class AuthClient(Protocol):
    def authorize_jwt(
        self,
        required_aud: str,
        decode_policy: TokenPolicy,
        required_role: str | None = None,
        authorization_token: str | None = None,
        user_info_token: str | None = None,
        rdn: str | None = None,
    ) -> UserInfo | None: ...
    def authorize_api_key(self, x_api_key: str) -> None: ...


class MicrodataAuthClient:
    jwks_client: PyJWKClient

    def __init__(self) -> None:
        self.jwks_client = PyJWKClient(environment.jwks_url, lifespan=3000)

    def _get_signing_key(self, jwt_token: str) -> PyJWK:
        return self.jwks_client.get_signing_key_from_jwt(jwt_token).key

    def authorize_jwt(
        self,
        required_aud: str,
        decode_policy: TokenPolicy,
        required_role: str | None = None,
        authorization_token: str | None = None,
        user_info_token: str | None = None,
        rdn: str | None = None,
    ) -> UserInfo | None:
        if authorization_token is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unauthorized. No token was provided",
            )
        try:
            signing_key = self._get_signing_key(authorization_token)
            decoded_jwt = _decode_jwt(
                jwt_token=authorization_token,
                policy=decode_policy,
                signing_key=signing_key,
                audience=required_aud,
                verify_signature=True,
            )
            if required_role:
                _validate_role(required_role, decoded_jwt)
            if rdn:
                _validate_rdn_in_aud(rdn, decoded_jwt)
            if user_info_token:
                return _validate_and_parse_user_info_from_token(
                    user_info_token, signing_key, decoded_jwt, True
                )
            if decode_policy.user_id_claim == "sub":
                _validate_user_id(decoded_jwt)

        except AuthError as e:
            raise e

    def authorize_api_key(self, x_api_key: str) -> None:
        if x_api_key != secrets.datastore_api_service_key:
            raise AuthError("Invalid API key")


class DisabledAuthClient:
    def authorize_jwt(
        self,
        required_aud: str,
        decode_policy: TokenPolicy,
        required_role: str | None = None,
        authorization_token: str | None = None,
        user_info_token: str | None = None,
        rdn: str | None = None,
    ) -> UserInfo | None:
        logger.error("JWT_AUTH is turned off. Returning default UserInfo")
        return UserInfo(
            user_id="1234-1234-1234-1234",
            first_name="Test",
            last_name="User",
        )

    def authorize_api_key(self, x_api_key: str) -> None:
        logger.error("JWT_AUTH is turned off. Not checking API key")


class SkipSignatureAuthClient:
    """
    This auth client reads JWT claims WITHOUT signature validation.
    Only use this in development/testing environments.
    """

    def authorize_jwt(
        self,
        required_aud: str,
        decode_policy: TokenPolicy,
        required_role: str | None = None,
        authorization_token: str | None = None,
        user_info_token: str | None = None,
        rdn: str | None = None,
    ) -> UserInfo | None:
        logger.error(
            "This auth client reads JWT claims WITHOUT signature validation!"
        )
        if authorization_token is None:
            raise AuthError("Unauthorized. No authorization token was provided")

        try:
            decoded_jwt = _decode_jwt(
                jwt_token=authorization_token,
                policy=decode_policy,
                signing_key=None,
                audience=required_aud,
                verify_signature=False,
            )
            if required_role:
                _validate_role(required_role, decoded_jwt)
            if rdn:
                _validate_rdn_in_aud(rdn, decoded_jwt)
            if user_info_token:
                return _validate_and_parse_user_info_from_token(
                    user_info_token, None, decoded_jwt, False
                )
            if decode_policy.user_id_claim == "sub":
                _validate_user_id(decoded_jwt)

        except AuthError as e:
            raise e

    def authorize_api_key(self, x_api_key: str) -> None:
        if x_api_key != secrets.datastore_api_service_key:
            raise AuthError("Invalid API key")


def get_auth_client() -> AuthClient:
    match environment.jwt_auth:
        case "FULL":
            return MicrodataAuthClient()
        case "SKIP_SIGNATURE":
            logger.error(
                "SKIP_SIGNATURE is enabled. "
                "JWT tokens will be read WITHOUT signature validation. "
                "This should only be used in development/testing environments."
            )
            return SkipSignatureAuthClient()
        case "OFF":
            logger.error("Auth toggled off. Returning default UserInfo.")
            return DisabledAuthClient()
        case _:
            raise InternalServerError(
                f"Unrecognized auth client: {environment.jwt_auth}"
            )
