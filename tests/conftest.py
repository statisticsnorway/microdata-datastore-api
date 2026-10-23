import os

os.environ["DOCKER_HOST_NAME"] = "localhost"
os.environ["STACK"] = "test"
os.environ["COMMIT_ID"] = "abc123"
os.environ["SQLITE_URL"] = "test.db"
os.environ["JWT_AUTH"] = "OFF"
os.environ["JWKS_URL"] = "http://localhost"
os.environ["SECRETS_FILE"] = "tests/resources/secrets/secrets.json"
os.environ["DATASTORES_ROOT_DIR"] = "tests/resources/test_datastore"

import json
from pathlib import Path

import pyarrow
import pytest
from fastapi import testclient
from pyarrow import dataset
from pyarrow.parquet.encryption import (
    EncryptionConfiguration,
    KmsConnectionConfig,
)

from datastore_api.adapter.kms_client import (
    COLUMN_KEY_ID,
    FOOTER_KEY_ID,
    make_crypto_factory,
)
from datastore_api.common.models import Version
from datastore_api.domain.data import (
    EncryptedDataReader,
    _get_parquet_path,
)
from datastore_api.main import app

ALL_COLUMNS = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]
DATASET_NAME = "TEST_ENCRYPTED_DATASET"
VERSION = Version.from_str("1.0.0.0")  # NOSONAR


@pytest.fixture(scope="session")
def test_app():
    yield testclient.TestClient(app)


def pytest_addoption(parser):
    parser.addoption(
        "--include-big-data",
        action="store_true",
        dest="include-big-data",
        default=False,
        help="enable big data testing",
    )


def _write_encrypted_parquet(table: pyarrow.Table, output_dir: Path) -> None:
    encryption_configuration = EncryptionConfiguration(
        footer_key=FOOTER_KEY_ID,
        column_keys={COLUMN_KEY_ID: ALL_COLUMNS},
        encryption_algorithm="AES_GCM_V1",
        double_wrapping=True,
        plaintext_footer=False,
        data_key_length_bits=256,
    )
    encryption_config = dataset.ParquetEncryptionConfig(
        make_crypto_factory(), KmsConnectionConfig(), encryption_configuration
    )
    parquet_format = dataset.ParquetFileFormat()
    write_options = parquet_format.make_write_options(
        encryption_config=encryption_config
    )
    dataset.write_dataset(
        table,
        output_dir,
        format=parquet_format,
        file_options=write_options,
        max_rows_per_file=0,
    )


@pytest.fixture
def encrypted_reader(tmp_path) -> EncryptedDataReader:
    return EncryptedDataReader(
        parquet_path=_get_parquet_path(VERSION, DATASET_NAME, tmp_path),
        columns=ALL_COLUMNS,
    )


@pytest.fixture
def encrypted_parquet(tmp_path):
    table = pyarrow.table(
        {
            "unit_id": [1, 2, 3, 4, 5],
            "value": ["A", "B", "C", "D", "E"],
            "start_epoch_days": [18262, 18628, 18993, 19358, 19723],
            "stop_epoch_days": [18627, 18992, 19357, 19722, None],
        }
    )
    parquet_dir_name = f"{DATASET_NAME}__{VERSION.to_2_underscored()}"
    parquet_dir = tmp_path / "data" / DATASET_NAME / parquet_dir_name
    parquet_dir.mkdir(parents=True)

    datastore_dir = tmp_path / "datastore"
    datastore_dir.mkdir()
    (
        datastore_dir / f"data_versions__{VERSION.to_2_underscored()}.json"
    ).write_text(json.dumps({DATASET_NAME: parquet_dir_name}))

    _write_encrypted_parquet(table, parquet_dir)
    return tmp_path, table


@pytest.fixture
def encrypted_datastore(encrypted_parquet):
    root_dir, table = encrypted_parquet
    datastore_dir = root_dir / "datastore"

    (datastore_dir / "metadata_all__1_0_0.json").write_text(
        json.dumps({"dataStructures": [{"name": DATASET_NAME}]})
    )

    (datastore_dir / "encrypted_versions.json").write_text(
        json.dumps({"versions": ["1.0"]})
    )

    return root_dir, table
