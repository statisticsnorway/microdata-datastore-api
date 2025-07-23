import os

from pytest import MonkeyPatch

from datastore_api.adapter.local_storage import data_directory


TEST_DIR = "tests/resources/datastore_unit_test/data"
TEST_PERSON_INCOME_PATH = f"{TEST_DIR}/TEST_PERSON_INCOME/TEST_PERSON_INCOME"
TEST_PERSON_INCOME_PATH_1_0 = f"{TEST_PERSON_INCOME_PATH}__1_0.parquet"
TEST_PERSON_INCOME_PATH_DRAFT = f"{TEST_PERSON_INCOME_PATH}__DRAFT.parquet"
TEST_STUDIEPOENG_PATH_1_0 = f"{TEST_DIR}/TEST_STUDIEPOENG/TEST_STUDIEPOENG__1_0"


def test_get_file_path():
    assert TEST_PERSON_INCOME_PATH_1_0 == (
        data_directory.get_parquet_file_path("TEST_PERSON_INCOME", "1_0")
    )


def test_get_file_path_draft():
    assert TEST_PERSON_INCOME_PATH_DRAFT == (
        data_directory.get_parquet_file_path("TEST_PERSON_INCOME", "0_0")
    )


def test_get_latest_in_draft_version():
    assert TEST_STUDIEPOENG_PATH_1_0 == (
        data_directory.get_parquet_file_path("TEST_STUDIEPOENG", "0_0")
    )


def test_get_latest_version(monkeypatch: MonkeyPatch):
    monkeypatch.setattr(
        os,
        "listdir",
        lambda _: [
            "data_versions__10_0.json",
            "data_versions__8_999.json",
            "data_versions__2_0.json",
        ],
    )
    assert "10_0" == (data_directory._get_latest_version())
    monkeypatch.setattr(
        os,
        "listdir",
        lambda _: [
            "data_versions__8988_321.json",
            "data_versions__9000_0.json",
            "data_versions__2_0.json",
        ],
    )
    assert "9000_0" == (data_directory._get_latest_version())


def test_get_partitioned_file_path():
    assert TEST_STUDIEPOENG_PATH_1_0 == (
        data_directory.get_parquet_file_path("TEST_STUDIEPOENG", "1_0")
    )
