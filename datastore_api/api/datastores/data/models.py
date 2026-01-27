import string
from pydantic import BaseModel, field_validator

from datastore_api.common.models import Version


class InputQuery(BaseModel):
    dataStructureName: str
    version: Version
    population: list | None = None
    includeAttributes: bool = False

    @field_validator("dataStructureName")
    @classmethod
    def validate_data_structure_name(cls, v: str) -> str:
        valid_characters = set(string.ascii_letters + string.digits + "_")
        if not all(char in valid_characters for char in v):
            raise ValueError(
                "dataStructureName must contain only "
                "alphanumeric characters and underscores"
            )
        return v

    @field_validator("version", mode="before")
    @classmethod
    def check_for_sem_ver(cls, version: str | Version | None) -> Version:
        if isinstance(version, str):
            return Version.from_str(version)
        if isinstance(version, Version):
            return version
        raise ValueError(f"'{version}' is not a valid semantic version.")

    def __str__(self) -> str:
        return (
            f"dataStructureName='{self.dataStructureName}' "
            + f"version='{str(self.version)}' "
            + f"population='<length: {len(self.population or [])}>' "
            + f"includeAttributes={self.includeAttributes}"
        )


class InputTimePeriodQuery(InputQuery):
    startDate: int
    stopDate: int

    def __str__(self) -> str:
        return (
            super().__str__()
            + f" startDate={self.startDate} stopDate={self.stopDate}"
        )


class InputTimeQuery(InputQuery):
    date: int

    def __str__(self) -> str:
        return super().__str__() + f" date={self.date}"


class InputFixedQuery(InputQuery):
    pass


class ErrorMessage(BaseModel):
    detail: str
