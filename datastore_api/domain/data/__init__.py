import logging
from pathlib import Path
from typing import Protocol

from cryptography.exceptions import InvalidTag
from pyarrow import ArrowInvalid, ArrowTypeError, Table, dataset
from pyarrow.parquet.encryption import (
    DecryptionConfiguration,
    KmsConnectionConfig,
)

from datastore_api.adapter.kms_client import (
    make_crypto_factory,
)
from datastore_api.adapter.local_storage import (
    datastore_directory,
)
from datastore_api.adapter.local_storage.datastore_directory import (
    get_encrypted_versions,
)
from datastore_api.common.exceptions import TooManyRowsException
from datastore_api.common.models import Version
from datastore_api.domain.data import filters
from datastore_api.domain.data.models import (
    DatasetEncryptionStatus,
    EncryptionStatus,
    InputFixedQuery,
    InputTimePeriodQuery,
    InputTimeQuery,
)
from datastore_api.domain.metadata import find_data_structures

logger = logging.getLogger()

EMPTY_RESULT_TEXT = "empty_result"
ALL_COLUMNS = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]


class DataReader(Protocol):
    parquet_path: str
    columns: list[str]

    def read_data(
        self,
        table_filter: dataset.Expression | None,
        *,
        row_cap: int | None = None,
        head_rows: int | None = None,
    ) -> Table: ...


class UnencryptedDataReader:
    parquet_path: str
    columns: list[str]

    def __init__(
        self,
        parquet_path: str,
        columns: list[str],
    ) -> None:
        self.parquet_path = parquet_path
        self.columns = columns

    def read_data(
        self,
        table_filter: dataset.Expression | None,
        *,
        row_cap: int | None = None,
        head_rows: int | None = None,
    ) -> Table:
        try:
            ds = dataset.dataset(self.parquet_path)

            if head_rows is not None:
                # Stops scanning as soon as head_rows matching rows are found
                table = ds.head(
                    head_rows, filter=table_filter, columns=self.columns
                )
            else:
                table = ds.to_table(filter=table_filter, columns=self.columns)

            logger.info(f"Number of rows in result set: {table.num_rows}")
            if row_cap and table.num_rows > row_cap:
                raise TooManyRowsException(
                    f"Rows exceed maximum cap of {row_cap}"
                )
            return table
        except ArrowTypeError as e:
            raise ValueError(
                f"Filter value type does not match dataset column type: {e}"
            ) from e


class EncryptedDataReader:
    parquet_path: str
    columns: list[str]

    def __init__(
        self,
        parquet_path: str,
        columns: list[str],
    ) -> None:
        self.parquet_path = parquet_path
        self.columns = columns

    def read_data(
        self,
        table_filter: dataset.Expression | None,
        *,
        row_cap: int | None = None,
        head_rows: int | None = None,
    ) -> Table:
        """
        Reads and filters an encrypted parquet file or partition and returns a
        pyarrow.Table with the requested columns.

        * table_filter: dataset.Expression - filters applied to the table
        * columns: list[str] - names of the columns to include in the
        returned table
        """
        try:
            decryption_config = dataset.ParquetDecryptionConfig(
                make_crypto_factory(),
                KmsConnectionConfig(),
                DecryptionConfiguration(),
            )
            scan_options = dataset.ParquetFragmentScanOptions(
                decryption_config=decryption_config
            )
            parquet_format = dataset.ParquetFileFormat(
                default_fragment_scan_options=scan_options
            )

            ds = dataset.dataset(self.parquet_path, format=parquet_format)

            if head_rows is not None:
                # Stops scanning as soon as head_rows matching rows are found
                table = ds.head(
                    head_rows, filter=table_filter, columns=self.columns
                )
            else:
                table = ds.to_table(filter=table_filter, columns=self.columns)

            logger.info(f"Number of rows in result set: {table.num_rows}")
            if row_cap and table.num_rows > row_cap:
                raise TooManyRowsException(
                    f"Rows exceed maximum cap of {row_cap}"
                )
            return table
        except ArrowTypeError as e:
            raise ValueError(
                f"Filter value type does not match dataset column type: {e}"
            ) from e
        except (ArrowInvalid, InvalidTag) as e:
            raise ValueError(f"Parquet decryption failed: {e}") from e


def _get_parquet_path(
    dataset_version: Version, dataset_name: str, datastore_root_dir: Path
) -> str:
    parquet_path: str | None = None
    if dataset_version.is_draft():
        parquet_path = datastore_directory.get_draft_data_file_path(
            dataset_name, datastore_root_dir
        )
    else:
        parquet_path = datastore_directory.get_data_path_from_data_versions(
            dataset_name,
            dataset_version,
            datastore_root_dir,
        )
    if parquet_path is None:
        latest_version = datastore_directory.get_latest_version(
            datastore_root_dir
        )
        parquet_path = datastore_directory.get_data_path_from_data_versions(
            dataset_name,
            latest_version,
            datastore_root_dir,
        )
    return parquet_path


def select_data_reader(
    input_query: InputTimePeriodQuery | InputTimeQuery | InputFixedQuery,
    datastore_root_dir: Path,
) -> DataReader:
    columns = ALL_COLUMNS if input_query.includeAttributes else ALL_COLUMNS[:2]
    parquet_path = _get_parquet_path(
        input_query.version,
        input_query.dataStructureName,
        datastore_root_dir,
    )
    if input_query.version.is_draft():
        is_encrypted = True
    else:
        actual_version = datastore_directory.get_version_from_data_path(
            input_query.dataStructureName, parquet_path
        )
        encrypted_versions = get_encrypted_versions(datastore_root_dir)
        is_encrypted = actual_version in encrypted_versions
    if is_encrypted:
        return EncryptedDataReader(parquet_path=parquet_path, columns=columns)
    return UnencryptedDataReader(parquet_path=parquet_path, columns=columns)


def validate_encryption(
    datastore_root_dir: Path,
    version: Version,
) -> EncryptionStatus:
    data_structures = find_data_structures(
        datastore_root_dir=datastore_root_dir,
        names=[],
        version=version,
        include_attributes=False,
    )

    dataset_encr_stats = []
    for data_structure in data_structures:
        dataset_name = data_structure["name"]
        input_query = InputFixedQuery(
            dataStructureName=dataset_name,
            version=version,
        )
        data_reader = select_data_reader(input_query, datastore_root_dir)

        if isinstance(data_reader, EncryptedDataReader):
            reader_type = "encrypted"
        elif isinstance(data_reader, UnencryptedDataReader):
            reader_type = "unencrypted"
        else:
            raise TypeError(
                f"Unexpected reader type: {type(data_reader).__name__}"
            )

        try:
            data_reader.read_data(None, head_rows=1)
            read_successfully = True
        except Exception:
            read_successfully = False

        parquet_path = _get_parquet_path(
            version,
            dataset_name,
            datastore_root_dir,
        )
        actual_version = datastore_directory.get_version_from_data_path(
            dataset_name,
            parquet_path,
        )

        dataset_encr_stat = DatasetEncryptionStatus(
            data_structure_name=dataset_name,
            reader=reader_type,
            read_successfully=read_successfully,
            actual_version=actual_version,
        )
        dataset_encr_stats.append(dataset_encr_stat)
    return EncryptionStatus(
        datastore_root_dir=str(datastore_root_dir),
        requested_version=version,
        datasets=dataset_encr_stats,
    )


def generate_fixed_filter(
    input_query: InputFixedQuery,
) -> dataset.Expression | None:
    return filters.generate_fixed_filter(
        population_filter=input_query.population,
        value_filter=input_query.values,
    )


def generate_time_filter(
    input_query: InputTimeQuery,
) -> dataset.Expression:
    return filters.generate_time_filter(
        date=input_query.date,
        population_filter=input_query.population,
        value_filter=input_query.values,
    )


def generate_time_period_filter(
    input_query: InputTimePeriodQuery,
) -> dataset.Expression:
    return filters.generate_time_period_filter(
        start=input_query.startDate,
        stop=input_query.stopDate,
        population_filter=input_query.population,
        value_filter=input_query.values,
    )
