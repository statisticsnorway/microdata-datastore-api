import json
from pathlib import Path

import pyarrow
import pytest
from pyarrow import dataset
from pyarrow.parquet.encryption import (
    CryptoFactory,
    EncryptionConfiguration,
    KmsConnectionConfig,
)

from datastore_api.adapter.kms_client import (
    COLUMN_KEY_ID,
    FOOTER_KEY_ID,
    InMemoryKmsClient,
    make_crypto_factory,
)
from datastore_api.common.models import Version
from datastore_api.domain.data import (
    EncryptedDataReader,
)

ALL_COLUMNS = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]
DATASET_NAME = "TEST_ENCRYPTED_DATASET"
VERSION = Version.from_str("1.0.0.0")  # NOSONAR


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


def _encrypted_reader(root_dir: Path) -> EncryptedDataReader:
    return EncryptedDataReader(
        dataset_name=DATASET_NAME,
        dataset_version=VERSION,
        columns=ALL_COLUMNS,
        datastore_root_dir=root_dir,
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


def test_encrypted_reader_result_matches_unencrypted(encrypted_parquet):
    root_dir, original_table = encrypted_parquet
    result = _encrypted_reader(root_dir).read_data(None)
    assert result.to_pydict() == original_table.to_pydict()


def test_encrypted_reader_with_filter(encrypted_parquet):
    root_dir, _ = encrypted_parquet
    table_filter = dataset.field("unit_id").isin([1, 3])
    result = _encrypted_reader(root_dir).read_data(table_filter)
    assert result.to_pydict() == {
        "unit_id": [1, 3],
        "value": ["A", "C"],
        "start_epoch_days": [18262, 18993],
        "stop_epoch_days": [18627, 19357],
    }


def test_encrypted_reader_wrong_key_raises_value_error(
    encrypted_parquet, monkeypatch
):
    root_dir, _ = encrypted_parquet
    wrong_key_hex = "0" * 64

    def wrong_crypto_factory():
        return CryptoFactory(
            lambda _cfg: InMemoryKmsClient(
                footer_master_key_hex=wrong_key_hex,
                column_master_key_hex=wrong_key_hex,
            )
        )

    monkeypatch.setattr(
        "datastore_api.domain.data.make_crypto_factory", wrong_crypto_factory
    )
    with pytest.raises(ValueError, match="Parquet decryption failed"):
        _encrypted_reader(root_dir).read_data(None)
