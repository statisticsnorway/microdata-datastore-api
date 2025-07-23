from datastore_api.common.models import CamelModel


class UserInfo(CamelModel, extra="forbid"):
    user_id: str
    first_name: str
    last_name: str
