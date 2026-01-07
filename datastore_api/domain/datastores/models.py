from pydantic import BaseModel


class NewDatastore(BaseModel, extra="forbid"):
    rdn: str
    description: str
    directory: str
    name: str
    bump_enabled: bool = False
