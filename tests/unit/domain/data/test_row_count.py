from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datastore_api.common.exceptions import InvalidStorageFormatException
from datastore_api.domain.data import (
    EncryptedDataReader,
    UnencryptedDataReader,
)


@pytest.mark.parametrize("row_count", [0, 5])
def test_unencrypted_row_count(tmp_path, row_count):
    table = pa.table({"unit_id": list(range(row_count))})
    path = tmp_path / "dataset.parquet"
    pq.write_table(table, path, row_group_size=2)

    reader = UnencryptedDataReader(str(path), columns=["unit_id"])

    assert reader.read_data_row_count() == row_count


@pytest.mark.parametrize(
    "reader_type", [UnencryptedDataReader, EncryptedDataReader]
)
def test_row_count_reads_file_metadata(reader_type, monkeypatch, tmp_path):
    path = tmp_path / "data.parquet"
    path.touch()
    reader = reader_type(str(path), columns=[])
    read_metadata = Mock(return_value=SimpleNamespace(num_rows=50_000_000))
    monkeypatch.setattr(pq, "read_metadata", read_metadata)

    assert reader.read_data_row_count() == 50_000_000
    assert read_metadata.call_args.args == (str(path),)
    read_metadata.assert_called_once()


def test_partitioned_row_count(partitioned_parquet):
    path, table, reader_type = partitioned_parquet
    reader = reader_type(str(path), columns=[])

    assert reader.read_data_row_count() == table.num_rows
    for date, expected in [(18262, 3), (18627, 3), (18628, 2), (18993, 0)]:
        assert reader.read_data_row_count(date=date) == expected


@pytest.mark.parametrize("date", [None, 18993])
def test_row_count_empty_directory_raises(partitioned_parquet, date):
    path, _, reader_type = partitioned_parquet
    if date is None:
        path /= "empty_dataset"
        path.mkdir()
    else:
        (path / "start_year=2022").mkdir()
    reader = reader_type(str(path), columns=[])

    with pytest.raises(
        InvalidStorageFormatException,
        match="No Parquet files found in directory",
    ):
        reader.read_data_row_count(date=date)
