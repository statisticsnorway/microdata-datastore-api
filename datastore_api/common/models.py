import re
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class CamelModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        serialize_by_alias=True,
        populate_by_name=True,
    )

    def model_dump(self, *, exclude_none=True, **kwargs):  # noqa
        return super().model_dump(exclude_none=True, **kwargs)


@dataclass(frozen=True)
class Version:
    SEMVER_4_PARTS_REG_EXP = re.compile(
        r"^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$"
    )
    major: str
    minor: str
    patch: str
    draft: str

    def to_2_underscored(self) -> str:
        return "_".join([self.major, self.minor])

    def to_3_underscored(self) -> str:
        return "_".join([self.major, self.minor, self.patch])

    def to_4_dotted(self) -> str:
        return ".".join([self.major, self.minor, self.patch, self.draft])

    def is_draft(self) -> bool:
        return self.major == "0" and self.minor == "0" and self.patch == "0"

    def __str__(self) -> str:
        return ".".join([self.major, self.minor, self.patch, self.draft])

    @staticmethod
    def from_str(version: str) -> "Version":
        if not Version.SEMVER_4_PARTS_REG_EXP.match(version):
            raise ValueError(f"Incorrect version format: {version}")
        try:
            split = version.split(".")
            return Version(split[0], split[1], split[2], split[3])
        except Exception as e:
            raise ValueError(f"Incorrect version format: {version}") from e
