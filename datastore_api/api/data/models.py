import re
from typing import Optional

from pydantic import BaseModel, field_validator


class InputQuery(BaseModel):
    dataStructureName: str
    version: str
    population: Optional[list] = None
    includeAttributes: Optional[bool] = False

    @field_validator("version")
    @classmethod
    def check_for_sem_ver(cls, version):  # pylint: disable=no-self-argument
        pattern = re.compile(r"^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$")
        if not pattern.match(version):
            raise ValueError(f"'{version}' is not a valid semantic version.")
        return version

    def get_file_version(self) -> str:
        version_numbers = self.version.split(".")
        return f"{version_numbers[0]}_{version_numbers[1]}"

    def __str__(self) -> str:
        return (
            f"dataStructureName='{self.dataStructureName}' "
            + f"version='{self.version}' "
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
