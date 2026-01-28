from fastapi import Cookie, Depends

from datastore_api.adapter import auth
from datastore_api.adapter.db.models import UserInfo


def require_data_administrator(
    authorization: str | None = Cookie(None),
    user_info: str | None = Cookie(None, alias="user-info"),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> UserInfo:
    return auth_client.authorize_data_administrator(authorization, user_info)


def require_datastore_provisioner(
    authorization: str | None = Cookie(None),
    user_info: str | None = Cookie(None, alias="user-info"),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
) -> UserInfo:
    return auth_client.authorize_datastore_modification(
        authorization, user_info
    )

