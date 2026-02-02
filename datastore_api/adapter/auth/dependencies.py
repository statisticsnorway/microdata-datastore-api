from fastapi import Cookie, Depends, Header

from datastore_api.adapter import auth
from datastore_api.adapter.db.models import UserInfo


def require_datastore_provisioner(
    authorization: str | None = Cookie(None),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> None:
    auth_client.authorize_datastore_provisioner(authorization)


def require_api_key(
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
    api_key: str = Header("X-API-KEY", alias="x-api-key"),
) -> None:
    auth_client.check_api_key(api_key)


def require_data_administrator(
    datastore_rdn: str,
    authorization: str | None = Cookie(None),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> None:
    auth_client.authorize_data_administrator(datastore_rdn, authorization)




def require_data_administrator_with_user_info(
    datastore_rdn: str,
    user_info: str | None = Cookie(None, alias="user-info"),
    authorization: str | None = Cookie(None),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> UserInfo:
    auth_context = auth_client.authorize_data_administrator(
        datastore_rdn, authorization
    )
    return auth_client.parse_user_info(auth_context, user_info)
