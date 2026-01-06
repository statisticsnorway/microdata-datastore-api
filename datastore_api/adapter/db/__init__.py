from typing import Protocol

from datastore_api.adapter.db.models import (
    Datastore,
    Job,
    JobStatus,
    MaintenanceStatus,
    Operation,
    Target,
)
from datastore_api.adapter.db.sqlite import SqliteDbClient
from datastore_api.api.datastores.models import NewDatastore
from datastore_api.config import environment


class DatabaseClient(Protocol):
    def get_job(self, job_id: int | str) -> Job: ...
    def get_jobs(
        self,
        *,
        datastore_id: int | None,
        status: JobStatus | None,
        operations: list[Operation] | None,
        ignore_completed: bool = False,
    ) -> list[Job]: ...
    def get_jobs_for_target(
        self, *, name: str, datastore_id: int
    ) -> list[Job]: ...
    def new_job(self, new_job: Job) -> Job: ...
    def update_job(
        self,
        *,
        job_id: str,
        status: JobStatus | None,
        description: str | None,
        log: str | None,
    ) -> Job: ...
    def set_maintenance_status(
        self, msg: str, paused: bool
    ) -> MaintenanceStatus: ...
    def get_latest_maintenance_status(self) -> MaintenanceStatus: ...
    def get_maintenance_history(self) -> list[MaintenanceStatus]: ...
    def initialize_maintenance(self) -> MaintenanceStatus: ...
    def get_targets(self, datastore_id: int) -> list[Target]: ...
    def update_target(self, job: Job) -> None: ...
    def update_bump_targets(self, job: Job) -> None: ...
    def get_datastores(self) -> list[str]: ...
    def get_datastore(self, datastore_id: int) -> Datastore: ...
    def get_datastore_id_from_rdn(self, rdn: str) -> int | None: ...
    def new_datastore(self, new_datastore: NewDatastore) -> None: ...


def get_database_client() -> DatabaseClient:
    return SqliteDbClient(environment.sqlite_url)
