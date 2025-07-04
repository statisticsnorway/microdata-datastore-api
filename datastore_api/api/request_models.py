import re
from typing import List

from pydantic import BaseModel, field_validator

from datastore_api.exceptions.exceptions import RequestValidationException

SEMVER_4_PARTS_REG_EXP = re.compile(r"^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$")


class MetadataQuery(BaseModel, extra="forbid", validate_assignment=True):
    names: str | None = None
    version: str
    include_attributes: bool = False
    skip_code_lists: bool = False

    @field_validator("version", mode="before")
    @classmethod
    def validate_version(cls, version: str):
        if not SEMVER_4_PARTS_REG_EXP.match(version):
            raise RequestValidationException(
                f"Version is in incorrect format: {version}. "
                "Should consist of 4 parts, e.g. 1.0.0.0"
            )
        return version

    def names_as_list(self) -> List[str]:
        return [] if self.names is None else self.names.split(",")


class NameParam(BaseModel, extra="forbid"):
    names: str

    def get_names_as_list(self) -> List[str]:
        return self.names.split(",")
