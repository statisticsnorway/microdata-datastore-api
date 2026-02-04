from fastapi import Cookie, Depends, Header

from datastore_api.adapter import auth
from datastore_api.adapter.auth import TokenPolicy
from datastore_api.adapter.db.models import UserInfo
from datastore_api.common.exceptions import AuthError
from datastore_api.config import environment, secrets

DATA_ADMINISTRATOR_ROLE = "role/dataadministrator"
DATASTORE_PROVISIONER_ROLE = "role/dataadministrator"
# TODO: Update once new role is available

USER_TOKEN_POLICY = TokenPolicy(user_id_claim="sub", required_claims=None)
ACCREDITATION_TOKEN_POLICY = TokenPolicy(
    user_id_claim="user/uuid",
    required_claims=[
        "aud",
        "sub",
        "accreditation/role",
        "user/uuid",
    ],
)

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


def authorize_api_key(
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
    api_key: str = Header("X-API-KEY", alias="x-api-key"),
) -> None:
    auth_client.authorize_api_key(api_key)


def authorize_datastore_provisioner(
    authorization: str | None = Cookie(None),
    user_info: str | None = Cookie(None, alias="user-info"),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> UserInfo:
    parsed_user_info = auth_client.authorize_jwt(
        required_role=DATASTORE_PROVISIONER_ROLE,
        required_aud=valid_aud_provision,
        decode_policy=ACCREDITATION_TOKEN_POLICY,
        authorization_token=authorization,
        user_info_token=user_info,
    )
    assert parsed_user_info
    if parsed_user_info.user_id not in secrets.datastore_provisioners:
        raise AuthError("Forbidden: Not allowed to modify datastore")

    return parsed_user_info


def authorize_data_administrator(
    datastore_rdn: str,
    authorization: str | None = Cookie(None),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> None:
    auth_client.authorize_jwt(
        required_role=DATA_ADMINISTRATOR_ROLE,
        required_aud=valid_aud_jobs,
        decode_policy=ACCREDITATION_TOKEN_POLICY,
        authorization_token=authorization,
        rdn=datastore_rdn,
    )


def authorize_data_administrator_with_user_info(
    datastore_rdn: str,
    user_info: str | None = Cookie(None, alias="user-info"),
    authorization: str | None = Cookie(None),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> UserInfo:
    parsed_user_info = auth_client.authorize_jwt(
        required_role=DATA_ADMINISTRATOR_ROLE,
        required_aud=valid_aud_jobs,
        decode_policy=ACCREDITATION_TOKEN_POLICY,
        authorization_token=authorization,
        user_info_token=user_info,
        rdn=datastore_rdn,
    )
    assert parsed_user_info
    return parsed_user_info


def authorize_user(
    authorization_header: str | None = Header(None),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> None:
    authorizaton_token: str | None = (
        authorization_header.removeprefix("Bearer ")
        if authorization_header
        else None
    )
    auth_client.authorize_jwt(
        required_aud=valid_aud_data,
        decode_policy=USER_TOKEN_POLICY,
        authorization_token=authorizaton_token,
    )
