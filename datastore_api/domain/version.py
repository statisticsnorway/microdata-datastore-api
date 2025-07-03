from dataclasses import dataclass


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


def get_version_from_string(version: str):
    split = version.split(".")
    return Version(split[0], split[1], split[2], split[3])
