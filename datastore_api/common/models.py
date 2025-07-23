from dataclasses import dataclass
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class CamelModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


@dataclass(frozen=True)
class Version:
    major: str
    minor: str
    patch: str
    draft: str

    def to_3_underscored(self):
        return "_".join([self.major, self.minor, self.patch])

    def to_4_dotted(self):
        return ".".join([self.major, self.minor, self.patch, self.draft])

    def is_draft(self):
        return self.major == "0" and self.minor == "0" and self.patch == "0"

    def __str__(self):
        return ".".join([self.major, self.minor, self.patch, self.draft])

    @staticmethod
    def from_str(version: str):
        split = version.split(".")
        return Version(split[0], split[1], split[2], split[3])
