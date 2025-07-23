import re
from typing import List, Optional

from fastapi import HTTPException, Query, status
from pydantic import BaseModel, field_validator

from datastore_api.common.models import Version


class MetadataQuery(BaseModel, extra="forbid", validate_assignment=True):
    names: str | None = None
    version: Version
    include_attributes: bool = False
    skip_code_lists: bool = False

    def names_as_list(self) -> List[str]:
        return [] if self.names is None else self.names.split(",")


class NameParam(BaseModel, extra="forbid"):
    names: str

    def get_names_as_list(self) -> List[str]:
        return self.names.split(",")


def get_metadata_query(
    names: str | None = Query(None),
    version: str = Query(..., description="Semantic version (e.g. 1.2.3.4)"),
    include_attributes: bool = Query(False),
    skip_code_lists: bool = Query(False),
) -> MetadataQuery:
    try:
        return MetadataQuery(
            names=names,
            version=Version.from_str(version),
            include_attributes=include_attributes,
            skip_code_lists=skip_code_lists,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )
