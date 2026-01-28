from fastapi import Cookie, Depends, Header

from datastore_api.adapter import auth
from datastore_api.adapter.db.models import UserInfo


def require_data_administrator(
    datastore_rdn: str,
    authorization: str | None = Cookie(None),
    user_info: str | None = Cookie(None, alias="user-info"),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> UserInfo:
    return auth_client.authorize_data_administrator(
        datastore_rdn, authorization, user_info
    )


def require_datastore_provisioner(
    authorization: str | None = Cookie(None),
    user_info: str | None = Cookie(None, alias="user-info"),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> UserInfo:
    return auth_client.authorize_datastore_provisioner(authorization, user_info)


def require_api_key(
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
    api_key: str = Header("X-API-KEY", alias="x-api-key"),
) -> None:
    auth_client.check_api_key(api_key)
