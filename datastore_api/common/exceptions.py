class NotFoundException(Exception): ...


class InvalidStorageFormatException(Exception): ...


class RequestValidationException(Exception): ...


class AuthError(Exception): ...


class InternalServerError(Exception): ...


class JobExistsException(Exception): ...


class JobAlreadyCompleteException(Exception): ...


class NoSuchImportableDataset(Exception): ...


class BadQueryException(Exception): ...


class BumpingDisabledException(Exception): ...


class NameValidationError(Exception): ...


class DatastoreNotFoundException(Exception): ...


class DatastoreRdnMissingException(Exception): ...


class DatastoreExistsException(Exception): ...


class DatastorePathExistsException(Exception): ...


class DatastoreSetupException(Exception): ...


class MigrationException(Exception): ...


class PublicKeyNotFoundException(Exception): ...


class PublicKeyAlreadyExistsException(Exception): ...


class PublicKeyInvalidException(Exception): ...


class StartUpException(Exception): ...


class TooManyRowsException(Exception):
    def __init__(self, row_limit: int, row_count: int) -> None:
        self.row_limit = row_limit
        self.row_count = row_count
        super().__init__(f"Rows exceed maximum cap of {row_limit}")


class InvalidDraftVersionException(Exception):
    def __init__(
        self, requested_draft_version: str, current_draft_version: str
    ) -> None:
        self.requested_draft_version = requested_draft_version
        self.current_draft_version = current_draft_version
        super().__init__(
            f"Requested draft version {requested_draft_version}, "
            f"but current is {current_draft_version}."
        )
