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
from datastore_api.config import environment


class DatabaseClient(Protocol):
    def get_job(self, job_id: int | str) -> Job: ...
    def get_jobs(
        self,
        status: JobStatus | None,
        operations: list[Operation] | None,
        ignore_completed: bool = False,
    ) -> list[Job]: ...
    def get_jobs_for_target(self, name: str) -> list[Job]: ...
    def new_job(self, new_job: Job) -> Job: ...
    def update_job(
        self,
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
    def get_targets(self) -> list[Target]: ...
    def update_target(self, job: Job) -> None: ...
    def update_bump_targets(self, job: Job) -> None: ...
    def get_datastore(self) -> Datastore: ...


def get_database_client() -> DatabaseClient:
    return SqliteDbClient(environment.sqlite_url)
