from datastore_api.common.models import CamelModel


class Language(CamelModel):
    code: str
    label: str


def find_languages() -> list[Language]:
    return [Language(code="no", label="Norsk")]
