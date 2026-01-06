import re
from pathlib import Path

from pydantic import BaseModel, field_validator

from datastore_api.common.models import CamelModel
from datastore_api.config import environment

DATASTORES_ROOT_DIR = environment.datastores_root_dir


class NewDatastore(BaseModel, extra="forbid"):
    rdn: str
    description: str
    directory: str
    name: str
    bump_enabled: bool = False


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

    def generate_datastore_dir_from_rdn(self) -> str:
        base_dir = Path(DATASTORES_ROOT_DIR).resolve()
        dir_name = self.rdn.replace(".", "_").lower()
        datastore_dir = (base_dir / dir_name).resolve()
        if not datastore_dir.is_relative_to(base_dir):
            raise ValueError(
                "Resolved datastore directory is outside allowed base directory"
            )
        return str(datastore_dir)

    def generate_new_datastore_from_request(self) -> NewDatastore:
        datastore_dir = self.generate_datastore_dir_from_rdn()
        return NewDatastore(
            name=self.name,
            rdn=self.rdn,
            description=self.description,
            directory=datastore_dir,
            bump_enabled=self.bump_enabled,
        )
