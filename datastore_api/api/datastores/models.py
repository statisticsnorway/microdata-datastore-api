from datastore_api.common.models import CamelModel


class NewDatastoreRequest(CamelModel, extra="forbid"):
    rdn: str
    description: str
    directory: str
    name: str
    bump_enabled: bool = False
