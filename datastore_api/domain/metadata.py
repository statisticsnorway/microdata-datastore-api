from itertools import chain
from datastore_api.adapter.local_storage import metadata_directory
from datastore_api.common.models import Version
from datastore_api.common.exceptions import (
    InvalidStorageFormatException,
    InvalidDraftVersionException,
)


def find_all_datastore_versions():
    draft_version = metadata_directory.get_draft_version()
    datastore_versions = metadata_directory.get_datastore_versions()

    if draft_version:
        datastore_versions["versions"].insert(0, draft_version)

    return datastore_versions


def find_current_data_structure_status(
    status_query_names: list[str],
) -> dict[str, dict | None]:
    datastore_versions = find_all_datastore_versions()
    datastructure_statuses = {}
    for version in datastore_versions["versions"]:
        for data_structure in version["dataStructureUpdates"]:
            if (
                data_structure["name"] in status_query_names
                and data_structure["name"] not in datastructure_statuses
            ):
                datastructure_statuses[data_structure["name"]] = {
                    "operation": data_structure["operation"],
                    "releaseTime": version["releaseTime"],
                    "releaseStatus": data_structure["releaseStatus"],
                }
    return {
        name: datastructure_statuses.get(name, None)
        for name in status_query_names
    }


def find_data_structures(
    names: list[str],
    version: Version,
    include_attributes: bool,
    skip_code_lists: bool = False,
):
    _validate_version(version)
    metadata = (
        metadata_directory.get_metadata_all(version)
        if not skip_code_lists
        else find_all_metadata_skip_code_list_and_missing_values(version)
    )

    if names:
        matched = [
            ds for ds in metadata["dataStructures"] if ds["name"] in names
        ]
    else:
        matched = metadata["dataStructures"]

    for match in matched:
        if not include_attributes:
            match.pop("attributeVariables", None)
    return matched


def find_all_metadata(version: Version, skip_code_lists: bool = False):
    _validate_version(version)
    return (
        metadata_directory.get_metadata_all(version)
        if not skip_code_lists
        else find_all_metadata_skip_code_list_and_missing_values(version)
    )


def find_all_data_structures_ever():
    all_datastore_versions = find_all_datastore_versions()
    datastore_versions = [ver for ver in all_datastore_versions["versions"]]
    data_structures = set()
    for datastore_version in datastore_versions:
        for ds_update in datastore_version["dataStructureUpdates"]:
            data_structures.add(ds_update["name"])
    return list(data_structures)


def find_languages():
    return [
        {"code": "no", "label": "Norsk"},
    ]


def find_all_metadata_skip_code_list_and_missing_values(version: Version):
    _validate_version(version)
    metadata_all = metadata_directory.get_metadata_all(version)
    if "dataStructures" in metadata_all:
        _clear_code_list_and_missing_values(metadata_all["dataStructures"])
    else:
        raise InvalidStorageFormatException("Invalid metadata format")
    return metadata_all


def _clear_code_list_and_missing_values(data_structures: list[dict]):
    represented_variables = []
    for metadata in data_structures:
        represented_measure = metadata["measureVariable"][
            "representedVariables"
        ]
        represented_identifiers = list(
            chain(
                *[
                    identifier["representedVariables"]
                    for identifier in metadata["identifierVariables"]
                ]
            )
        )
        represented_attributes = list(
            chain(
                *[
                    attribute["representedVariables"]
                    for attribute in metadata["attributeVariables"]
                ]
            )
        )
        represented_variables += (
            represented_measure
            + represented_identifiers
            + represented_attributes
        )
    for represented_variable in represented_variables:
        if "codeList" in represented_variable["valueDomain"]:
            represented_variable["valueDomain"]["codeList"].clear()
        if "missingValues" in represented_variable["valueDomain"]:
            represented_variable["valueDomain"]["missingValues"].clear()


def _validate_version(version: Version):
    if version.is_draft() and version.draft != "0":
        draft_version = metadata_directory.get_draft_version()
        if draft_version["version"] != version.to_4_dotted():
            raise InvalidDraftVersionException(
                f"Requested draft version {version}, "
                f"but current is {draft_version['version']}"
            )
