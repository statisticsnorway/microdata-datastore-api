import logging
from typing import Protocol

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
from datastore_api.config import environment

logger = logging.getLogger()

USER_FIRST_NAME_KEY = "user/firstName"
USER_LAST_NAME_KEY = "user/lastName"
USER_ID_KEY = "user/uuid"
ACCREDITATION_ROLE_KEY = "accreditation/role"


class AuthClient(Protocol):
    def authorize_user(self, authorization_header: str | None) -> str: ...
    def authorize_data_administrator(
        self, authorization_cookie: str | None, user_info_cookie: str | None
    ) -> UserInfo: ...


class MicrodataAuthClient:
    jwks_client: PyJWKClient
    valid_aud_jobs: str
    valid_aud_data: str

    def __init__(self, valid_aud_jobs: str, valid_aud_data: str) -> None:
        self.jwks_client = PyJWKClient(environment.jwks_url, lifespan=3000)
        self.valid_aud_jobs = valid_aud_jobs
        self.valid_aud_data = valid_aud_data

    def _get_signing_key(self, jwt_token: str) -> PyJWK:
        return self.jwks_client.get_signing_key_from_jwt(jwt_token).key

    def authorize_user(self, authorization_header: str | None) -> str:
        if authorization_header is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unauthorized. No token was provided",
            )

        try:
            jwt_token = authorization_header.removeprefix("Bearer ")
            signing_key = self._get_signing_key(jwt_token)

            decoded_jwt = jwt.decode(
                jwt_token,
                signing_key,
                algorithms=["RS256", "RS512"],
                audience=self.valid_aud_data,
            )
            user_id = decoded_jwt.get("sub")
            if user_id in [None, ""]:
                raise AuthError("No valid user_id")
            return user_id
        except (
            InvalidSignatureError,
            ExpiredSignatureError,
            InvalidAudienceError,
            AuthError,
            DecodeError,
            ValueError,
            AttributeError,
        ) as e:
            raise AuthError("Unauthorized: Invalid token") from e
        except Exception as e:
            raise InternalServerError(f"Internal Server Error: {e}") from e

    def authorize_data_administrator(
        self, authorization_cookie: str | None, user_info_cookie: str | None
    ) -> UserInfo:
        if authorization_cookie is None:
            raise AuthError("Unauthorized. No authorization token was provided")
        if user_info_cookie is None:
            raise AuthError("Unauthorized. No user info token was provided")
        try:
            signing_key = self._get_signing_key(authorization_cookie)
            decoded_authorization = jwt.decode(
                authorization_cookie,
                signing_key,
                algorithms=["RS256", "RS512"],
                audience=self.valid_aud_jobs,
                options={
                    "require": [
                        "aud",
                        "sub",
                        ACCREDITATION_ROLE_KEY,
                        USER_ID_KEY,
                    ]
                },
            )
            role = decoded_authorization.get(ACCREDITATION_ROLE_KEY)
            if role != "role/dataadministrator":
                raise AuthError(f"Can't start job with role: {role}")
            decoded_user_info = jwt.decode(
                user_info_cookie,
                signing_key,
                algorithms=["RS256", "RS512"],
                options={
                    "require": [
                        USER_ID_KEY,
                        USER_FIRST_NAME_KEY,
                        USER_LAST_NAME_KEY,
                    ],
                    "verify_aud": False,
                },
            )
            if decoded_authorization.get(USER_ID_KEY) != decoded_user_info.get(
                USER_ID_KEY
            ):
                raise AuthError("Token mismatch")
            user_id = decoded_user_info.get(USER_ID_KEY)
            first_name = decoded_user_info.get(USER_FIRST_NAME_KEY)
            last_name = decoded_user_info.get(USER_LAST_NAME_KEY)
            return UserInfo(
                user_id=str(user_id),
                first_name=str(first_name),
                last_name=str(last_name),
            )
        except AuthError as e:
            raise e
        except (
            InvalidSignatureError,
            ExpiredSignatureError,
            InvalidAudienceError,
            MissingRequiredClaimError,
            DecodeError,
            ValueError,
            AttributeError,
            KeyError,
        ) as e:
            raise AuthError(f"Unauthorized: {e}") from e
        except Exception as e:
            raise InternalServerError(f"Internal Server Error {e}") from e


class DisabledAuthClient:
    def authorize_user(
        self,
        authorization_header: str | None,  # NOSONAR(S1172)
    ) -> str:
        logger.error('Auth toggled off. Returning "default" as user_id.')
        return "default"

    def authorize_data_administrator(
        self,
        authorization_cookie: str | None,  # NOSONAR(S1172)
        user_info_cookie: str | None,  # NOSONAR(S1172)
    ) -> UserInfo:
        logger.error("JWT_AUTH is turned off. Returning default UserInfo")
        return UserInfo(
            user_id="1234-1234-1234-1234",
            first_name="Test",
            last_name="User",
        )


class SkipSignatureAuthClient:
    """
    This auth client reads JWT claims WITHOUT signature validation.
    Only use this in development/testing environments.
    """

    valid_aud: str

    def __init__(self) -> None:
        self.valid_aud = (
            "datastore-qa" if environment.stack == "qa" else "datastore"
        )

    def authorize_user(self, authorization_header: str | None) -> str:
        logger.error(
            "This auth client reads JWT claims WITHOUT signature validation!"
        )
        if authorization_header is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unauthorized. No token was provided",
            )

        try:
            jwt_token = authorization_header.removeprefix("Bearer ")
            decoded_jwt = jwt.decode(
                jwt_token,
                audience=self.valid_aud,
                options={  # NOSONAR(S5659)
                    "verify_signature": False,
                    "verify_exp": True,
                    "verify_aud": True,
                },
            )
            user_id = decoded_jwt.get("sub")
            if user_id in [None, ""]:
                raise AuthError("No valid user_id in token")
            return user_id
        except (
            ExpiredSignatureError,
            InvalidAudienceError,
            AuthError,
            DecodeError,
            ValueError,
            AttributeError,
        ) as e:
            raise AuthError("Unauthorized: Invalid token") from e
        except Exception as e:
            raise InternalServerError(f"Internal Server Error: {e}") from e

    def authorize_data_administrator(
        self, authorization_cookie: str | None, user_info_cookie: str | None
    ) -> UserInfo:
        logger.error(
            "This auth client reads JWT claims WITHOUT signature validation!"
        )
        if authorization_cookie is None:
            raise AuthError("Unauthorized. No authorization token was provided")
        if user_info_cookie is None:
            raise AuthError("Unauthorized. No user info token was provided")

        try:
            decoded_authorization = jwt.decode(
                authorization_cookie,
                audience=self.valid_aud,
                options={  # NOSONAR(S5659)
                    "verify_signature": False,
                    "verify_exp": True,
                    "verify_aud": True,
                    "require": [
                        "aud",
                        "sub",
                        ACCREDITATION_ROLE_KEY,
                        USER_ID_KEY,
                    ],
                },
            )
            role = decoded_authorization.get(ACCREDITATION_ROLE_KEY)
            if role != "role/dataadministrator":
                raise AuthError(f"Can't start job with role: {role}")
            decoded_user_info = jwt.decode(
                user_info_cookie,
                options={  # NOSONAR(S5659)
                    "verify_signature": False,
                    "verify_exp": False,
                    "verify_aud": False,
                    "require": [
                        USER_ID_KEY,
                        USER_FIRST_NAME_KEY,
                        USER_LAST_NAME_KEY,
                    ],
                },
            )
            if decoded_authorization.get(USER_ID_KEY) != decoded_user_info.get(
                USER_ID_KEY
            ):
                raise AuthError("Token mismatch")
            user_id = decoded_user_info.get(USER_ID_KEY)
            first_name = decoded_user_info.get(USER_FIRST_NAME_KEY)
            last_name = decoded_user_info.get(USER_LAST_NAME_KEY)
            return UserInfo(
                user_id=str(user_id),
                first_name=str(first_name),
                last_name=str(last_name),
            )
        except AuthError as e:
            raise e
        except (
            ExpiredSignatureError,
            InvalidAudienceError,
            MissingRequiredClaimError,
            DecodeError,
            ValueError,
            AttributeError,
            KeyError,
        ) as e:
            raise AuthError(f"Unauthorized: {e}") from e
        except Exception as e:
            raise InternalServerError(f"Internal Server Error {e}") from e


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
            logger.error('Auth toggled off. Returning "default" as user_id.')
            return DisabledAuthClient()
        case _:
            raise InternalServerError(
                f"Unrecognized auth client: {environment.jwt_auth}"
            )
