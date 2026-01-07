import re

from pydantic import field_validator

from datastore_api.common.models import CamelModel


class NewDatastoreRequest(CamelModel, extra="forbid"):
    rdn: str
    description: str
    name: str
    bump_enabled: bool = False

    @field_validator("rdn")
    @classmethod
    def validate_rdn(cls, value: str) -> str:
        if not re.fullmatch("^[a-z.]+$", value):
            raise ValueError(
                "Rdn may only contain lowercase letters (a-z) and dots (.)"
            )
        return value

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not re.fullmatch("^[a-zA-Z ]+$", value):
            raise ValueError(
                "Name may only contain letters (A-Z, a-z) and space"
            )
        return value
