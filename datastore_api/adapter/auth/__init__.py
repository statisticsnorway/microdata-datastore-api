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
from datastore_api.config import environment, secrets

logger = logging.getLogger()

USER_FIRST_NAME_KEY = "user/firstName"
USER_LAST_NAME_KEY = "user/lastName"
USER_ID_KEY = "user/uuid"
ACCREDITATION_ROLE_KEY = "accreditation/role"
DATA_ADMINISTRATOR_ROLE = "role/dataadministrator"
DATASTORE_PROVISIONER_ROLE = "role/dataadministrator"
# TODO: Update once new role is available


class AuthClient(Protocol):
    def authorize_user(self, authorization_header: str | None) -> str: ...
    def authorize_data_administrator(
        self,
        rdn: str,
        authorization_cookie: str | None,
        user_info_cookie: str | None,
    ) -> UserInfo: ...
    def authorize_datastore_provisioner(
        self, authorization_cookie: str | None, user_info_cookie: str | None
    ) -> UserInfo: ...
    def check_api_key(self, x_api_key: str) -> None: ...


class MicrodataAuthClient:
    jwks_client: PyJWKClient
    valid_aud_jobs: str
    valid_aud_data: str
    valid_aud_provision: str

    def __init__(
        self, valid_aud_jobs: str, valid_aud_data: str, valid_aud_provision: str
    ) -> None:
        self.jwks_client = PyJWKClient(environment.jwks_url, lifespan=3000)
        self.valid_aud_jobs = valid_aud_jobs
        self.valid_aud_data = valid_aud_data
        self.valid_aud_provision = valid_aud_provision

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

    def authorize_datastore_user(
        self,
        required_role: str,
        authorization_cookie: str | None,
        user_info_cookie: str | None,
        rdn: str | None = None,
    ) -> UserInfo:
        if authorization_cookie is None:
            raise AuthError("Unauthorized. No authorization token was provided")
        if user_info_cookie is None:
            raise AuthError("Unauthorized. No user info token was provided")
        required_aud = (
            self.valid_aud_jobs
            if required_role == DATA_ADMINISTRATOR_ROLE
            else self.valid_aud_provision
        )
        try:
            signing_key = self._get_signing_key(authorization_cookie)
            decoded_authorization = jwt.decode(
                authorization_cookie,
                signing_key,
                algorithms=["RS256", "RS512"],
                audience=required_aud,
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
            if role != required_role:
                raise AuthError(f"Unauthorized with role {role}")
            if rdn:
                aud = decoded_authorization.get("aud")
                aud_list = [aud] if isinstance(aud, str) else aud
                if not any(rdn.startswith(a) for a in aud_list):
                    raise AuthError(
                        f"Not authorized to access datastore: {rdn}"
                    )
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

    def authorize_datastore_provisioner(
        self, authorization_cookie: str | None, user_info_cookie: str | None
    ) -> UserInfo:
        parsed_user_info = self.authorize_datastore_user(
            DATASTORE_PROVISIONER_ROLE, authorization_cookie, user_info_cookie
        )
        if parsed_user_info.user_id not in secrets.datastore_provisioners:
            raise AuthError("Forbidden: Not allowed to modify datastore")
        return parsed_user_info

    def authorize_data_administrator(
        self,
        rdn: str,
        authorization_cookie: str | None,
        user_info_cookie: str | None,
    ) -> UserInfo:
        parsed_user_info = self.authorize_datastore_user(
            DATA_ADMINISTRATOR_ROLE, authorization_cookie, user_info_cookie, rdn
        )
        return parsed_user_info

    def check_api_key(self, x_api_key: str) -> None:
        if x_api_key != secrets.datastore_api_service_key:
            raise AuthError("Invalid API key")


class DisabledAuthClient:
    def authorize_user(
        self,
        authorization_header: str | None,  # NOSONAR(S1172)
    ) -> str:
        logger.error('Auth toggled off. Returning "default" as user_id.')
        return "default"

    def authorize_data_administrator(
        self,
        rdn: str | None,
        authorization_cookie: str | None,  # NOSONAR(S1172)
        user_info_cookie: str | None,  # NOSONAR(S1172)
    ) -> UserInfo:
        logger.error("JWT_AUTH is turned off. Returning default UserInfo")
        return UserInfo(
            user_id="1234-1234-1234-1234",
            first_name="Test",
            last_name="User",
        )

    def authorize_datastore_provisioner(
        self, authorization_cookie: str | None, user_info_cookie: str | None
    ) -> UserInfo:
        logger.error("JWT_AUTH is turned off. Returning default UserInfo")
        return UserInfo(
            user_id="1234-1234-1234-1234",
            first_name="Test",
            last_name="User",
        )

    def check_api_key(self, x_api_key: str) -> None:
        logger.error("JWT_AUTH is turned off. Not checking API key")


class SkipSignatureAuthClient:
    """
    This auth client reads JWT claims WITHOUT signature validation.
    Only use this in development/testing environments.
    """

    valid_aud_jobs: str
    valid_aud_data: str
    valid_aud_provision: str

    def __init__(
        self, valid_aud_jobs: str, valid_aud_data: str, valid_aud_provision: str
    ) -> None:
        self.valid_aud_jobs = valid_aud_jobs
        self.valid_aud_data = valid_aud_data
        self.valid_aud_provision = valid_aud_provision

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
                audience=self.valid_aud_data,
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

    def authorize_datastore_user(
        self,
        required_role: str,
        authorization_cookie: str | None,
        user_info_cookie: str | None,
        rdn: str | None = None,
    ) -> UserInfo:
        logger.error(
            "This auth client reads JWT claims WITHOUT signature validation!"
        )
        if authorization_cookie is None:
            raise AuthError("Unauthorized. No authorization token was provided")
        if user_info_cookie is None:
            raise AuthError("Unauthorized. No user info token was provided")
        required_aud = (
            self.valid_aud_jobs
            if required_role == DATA_ADMINISTRATOR_ROLE
            else self.valid_aud_provision
        )
        try:
            decoded_authorization = jwt.decode(
                authorization_cookie,
                audience=required_aud,
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
            if role != required_role:
                raise AuthError(f"Unauthorized with role: {role}")
            if rdn:
                aud = decoded_authorization.get("aud")
                aud_list = [aud] if isinstance(aud, str) else aud
                if not any(rdn.startswith(a) for a in aud_list):
                    raise AuthError(
                        f"Not authorized to access datastore: {rdn}"
                    )
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

    def authorize_datastore_provisioner(
        self, authorization_cookie: str | None, user_info_cookie: str | None
    ) -> UserInfo:
        parsed_user_info = self.authorize_datastore_user(
            DATASTORE_PROVISIONER_ROLE,
            authorization_cookie,
            user_info_cookie,
        )
        if parsed_user_info.user_id not in secrets.datastore_provisioners:
            raise AuthError("Forbidden: Not allowed to modify datastore")
        return parsed_user_info

    def authorize_data_administrator(
        self,
        rdn: str,
        authorization_cookie: str | None,
        user_info_cookie: str | None,
    ) -> UserInfo:
        parsed_user_info = self.authorize_datastore_user(
            DATA_ADMINISTRATOR_ROLE, authorization_cookie, user_info_cookie, rdn
        )
        return parsed_user_info

    def check_api_key(self, x_api_key: str) -> None:
        if x_api_key != secrets.datastore_api_service_key:
            raise AuthError("Invalid API key")


def get_auth_client() -> AuthClient:
    stack = environment.stack
    valid_aud_jobs = (
        "datastore-api-jobs-qa" if stack == "qa" else "datastore-api-jobs"
    )
    valid_aud_data = (
        "datastore-api-data-qa" if stack == "qa" else "datastore-api-data"
    )
    valid_aud_provision = (
        "datastore-api-jobs-qa" if stack == "qa" else "datastore-api-jobs"
    )  # TODO: Update once new aud (datastore-api-provision) is available

    match environment.jwt_auth:
        case "FULL":
            return MicrodataAuthClient(
                valid_aud_jobs=valid_aud_jobs,
                valid_aud_data=valid_aud_data,
                valid_aud_provision=valid_aud_provision,
            )
        case "SKIP_SIGNATURE":
            logger.error(
                "SKIP_SIGNATURE is enabled. "
                "JWT tokens will be read WITHOUT signature validation. "
                "This should only be used in development/testing environments."
            )
            return SkipSignatureAuthClient(
                valid_aud_jobs=valid_aud_jobs,
                valid_aud_data=valid_aud_data,
                valid_aud_provision=valid_aud_provision,
            )
        case "OFF":
            logger.error('Auth toggled off. Returning "default" as user_id.')
            return DisabledAuthClient()
        case _:
            raise InternalServerError(
                f"Unrecognized auth client: {environment.jwt_auth}"
            )
