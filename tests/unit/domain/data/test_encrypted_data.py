from pathlib import Path

import pytest
from pyarrow import dataset
from pyarrow.parquet.encryption import CryptoFactory

from datastore_api.adapter.kms_client import InMemoryKmsClient
from datastore_api.common.models import Version
from datastore_api.domain.data import (
    EncryptedDataReader,
    _get_parquet_path,
)

ALL_COLUMNS = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]
DATASET_NAME = "TEST_ENCRYPTED_DATASET"
VERSION = Version.from_str("1.0.0.0")  # NOSONAR


def _encrypted_reader(root_dir: Path) -> EncryptedDataReader:
    return EncryptedDataReader(
        parquet_path=_get_parquet_path(VERSION, DATASET_NAME, root_dir),
        columns=ALL_COLUMNS,
    )


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
