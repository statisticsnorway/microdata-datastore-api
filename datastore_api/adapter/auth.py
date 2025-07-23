import logging

from fastapi import HTTPException, status
import jwt
from jwt import MissingRequiredClaimError, PyJWKClient
from jwt.exceptions import (
    InvalidSignatureError,
    ExpiredSignatureError,
    InvalidAudienceError,
    DecodeError,
)

from datastore_api.config import environment
from datastore_api.common.exceptions import AuthError, InternalServerError
from datastore_api.adapter.db.models import UserInfo


logger = logging.getLogger()

USER_FIRST_NAME_KEY = "user/firstName"
USER_LAST_NAME_KEY = "user/lastName"
USER_ID_KEY = "user/uuid"


class AuthClient:
    valid_aud: str
    jwks_client: PyJWKClient

    def __init__(self):
        self.valid_aud = (
            "datastore-qa" if environment.get("STACK") == "qa" else "datastore"
        )
        self.jwks_client = PyJWKClient(
            environment.get("JWKS_URL"), lifespan=3000
        )

    def _get_signing_key(self, jwt_token: str):
        return self.jwks_client.get_signing_key_from_jwt(jwt_token).key

    def authorize_user(self, authorization_header: str | None) -> str:
        if environment.get("JWT_AUTH") == "false":
            logger.info('Auth toggled off. Returning "default" as user_id.')
            return "default"
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
                audience=self.valid_aud,
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
        except (KeyError, Exception) as e:
            raise InternalServerError(f"Internal Server Error: {e}") from e

    def authorize_data_administrator(
        self, authorization_cookie: str | None, user_info_cookie: str | None
    ) -> UserInfo:
        if environment.get("JWT_AUTH") == "false":
            logger.warning("JWT_AUTH is turned off.")
            return UserInfo(
                user_id="1234-1234-1234-1234",
                first_name="Test",
                last_name="User",
            )
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
                audience=self.valid_aud,
                options={
                    "require": [
                        "aud",
                        "sub",
                        "accreditation/role",
                        USER_ID_KEY,
                    ]
                },
            )
            role = decoded_authorization.get("accreditation/role")
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


def get_auth_client() -> AuthClient:
    return AuthClient()
