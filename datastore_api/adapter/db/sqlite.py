import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from datastore_api.adapter.db.models import (
    Datastore,
    Job,
    JobParameters,
    JobStatus,
    Log,
    MaintenanceStatus,
    Operation,
    Target,
    UserInfo,
)
from datastore_api.common.exceptions import (
    JobAlreadyCompleteException,
    JobExistsException,
    NotFoundException,
)

logger = logging.getLogger()

sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())
sqlite3.register_converter(
    "timestamp", lambda s: datetime.fromisoformat(s.decode())
)


class SqliteDbClient:
    db_path: Path

    def __init__(self, db_url: str) -> None:
        self.db_path = Path(db_url.replace("sqlite://", ""))
        self._ensure_schema()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_schema(self) -> None:
        conn = self._conn()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job (
                    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target TEXT,
                    datastore_id INTEGER,
                    status TEXT,
                    created_at TIMESTAMP,
                    created_by TEXT,
                    parameters TEXT,
                    FOREIGN KEY(datastore_id) REFERENCES datastore(datastore_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS maintenance (
                    maintenance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    datastore_id INTEGER,
                    msg TEXT,
                    paused BOOLEAN,
                    timestamp TIMESTAMP,
                    FOREIGN KEY(datastore_id) REFERENCES datastore(datastore_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS target (
                    name TEXT,
                    datastore_id INTEGER,
                    status TEXT,
                    action TEXT,
                    last_updated_at TIMESTAMP,
                    last_updated_by TEXT,
                    PRIMARY KEY (name, datastore_id)
                    FOREIGN KEY(datastore_id) REFERENCES datastore(datastore_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_log (
                    job_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER,
                    msg TEXT,
                    at TIMESTAMP,
                    FOREIGN KEY(job_id) REFERENCES job(job_id) ON DELETE CASCADE
                )
            """)
            conn.commit()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS datastore (
                    datastore_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rdn TEXT,
                    description TEXT,
                    directory TEXT,
                    name TEXT,
                    bump_enabled BOOLEAN DEFAULT FALSE
                )
            """)
            conn.commit()
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_rdn
                ON datastore(rdn)
                """)
            conn.commit()
        finally:
            conn.close()

    def _get_job_row_with_logs(
        self, cursor: sqlite3.Cursor, job_id: int | str
    ) -> sqlite3.Row | None:
        job_id = int(job_id)
        job_row = cursor.execute(
            """
            SELECT
                j.job_id,
                j.status,
                j.parameters,
                j.created_at,
                j.created_by,
                COALESCE((
                    SELECT json_group_array(
                        json_object(
                            'at', job_log_row.at,
                            'message', job_log_row.msg
                        )
                    )
                    FROM (
                        SELECT at, msg
                        FROM job_log
                        WHERE job_log.job_id = j.job_id
                        ORDER BY at ASC
                    ) AS job_log_row
                ), '[]') AS logs_json
            FROM job j
            WHERE j.job_id = ?;
            """,
            (job_id,),
        ).fetchone()
        return job_row

    def get_job(self, job_id: int | str) -> Job:
        """
        Returns job with matching job_id from database.
        Raises NotFoundException if no such job is found.
        """
        conn = self._conn()
        try:
            cursor = conn.cursor()
            job_id = int(job_id)
            job_row = self._get_job_row_with_logs(cursor, job_id)

            if not job_row:
                raise NotFoundException(f"No job found for jobId: {job_id}")

            return Job(
                job_id=str(job_row["job_id"]),
                status=job_row["status"],
                parameters=JobParameters.model_validate(
                    json.loads(job_row["parameters"])
                ),
                created_at=job_row["created_at"].isoformat(),
                created_by=UserInfo.model_validate(
                    json.loads(job_row["created_by"])
                ),
                log=[
                    Log(at=row["at"], message=row["message"])
                    for row in json.loads(job_row["logs_json"])
                ],
            )
        finally:
            conn.close()

    def get_jobs(
        self,
        status: JobStatus | None,
        operations: list[Operation] | None,
        ignore_completed: bool = False,
    ) -> list[Job]:
        """
        Returns list of jobs with matching status from database.
        """
        where_conditions = []
        if status is not None:
            where_conditions.append(f"status = '{status}'")
        if ignore_completed:
            where_conditions.append("status NOT IN ('completed', 'failed')")
        if operations is not None:
            in_clause = ",".join([f"'{str(op)}'" for op in operations])
            where_conditions.append(
                f"json_extract(parameters, '$.operation')  IN ({in_clause})"
            )
        where_conditions = (
            "WHERE " + " AND ".join(where_conditions)
            if where_conditions
            else ""
        )
        conn = self._conn()
        try:
            cursor = conn.cursor()
            job_rows = cursor.execute(
                f"""
                SELECT
                    j.job_id,
                    j.status,
                    j.parameters,
                    j.created_at,
                    j.created_by,
                    COALESCE((
                        SELECT json_group_array(
                            json_object(
                                'at', job_log_row.at,
                                'message', job_log_row.msg
                            )
                        )
                        FROM (
                            SELECT at, msg
                            FROM job_log
                            WHERE job_log.job_id = j.job_id
                            ORDER BY at ASC
                        ) AS job_log_row
                    ), '[]') AS logs_json
                FROM job j
                {where_conditions}
                """,
            ).fetchall()
            if not job_rows:
                return []
            return [
                Job(
                    job_id=str(job_row["job_id"]),
                    status=job_row["status"],
                    parameters=json.loads(job_row["parameters"]),
                    created_at=job_row["created_at"].isoformat(),
                    created_by=json.loads(job_row["created_by"]),
                    log=[
                        Log(at=row["at"], message=row["message"])
                        for row in json.loads(job_row["logs_json"])
                    ],
                )
                for job_row in job_rows
            ]
        finally:
            conn.close()

    def get_jobs_for_target(self, name: str) -> list[Job]:
        """
        Returns list of jobs with matching target name for database.
        Including datastore bump jobs that include the name in
        datastructureUpdates.
        """
        conn = self._conn()
        try:
            cursor = conn.cursor()
            job_rows = cursor.execute(
                """
                SELECT
                    j.job_id,
                    j.status,
                    j.parameters,
                    j.created_at,
                    j.created_by,
                    COALESCE((
                        SELECT json_group_array(
                            json_object(
                                'at', job_log_row.at,
                                'message', job_log_row.msg
                            )
                        )
                        FROM (
                            SELECT at, msg
                            FROM job_log
                            WHERE job_log.job_id = j.job_id
                            ORDER BY at ASC
                        ) AS job_log_row
                    ), '[]') AS logs_json
                FROM job j
                WHERE j.target = ?;
                """,
                (name,),
            ).fetchall()
            if not job_rows:
                return []
            return [
                Job(
                    job_id=str(job_row["job_id"]),
                    status=job_row["status"],
                    parameters=json.loads(job_row["parameters"]),
                    created_at=job_row["created_at"].isoformat(),
                    created_by=json.loads(job_row["created_by"]),
                    log=[
                        Log(at=row["at"], message=row["message"])
                        for row in json.loads(job_row["logs_json"])
                    ],
                )
                for job_row in job_rows
            ]
        finally:
            conn.close()

    def new_job(self, new_job: Job) -> Job:
        """
        Creates a new job for supplied command, status and dataset_name, and
        returns job_id of created job.
        Raises JobExistsException if job already exists in database.
        """
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 1 FROM job
                WHERE target = ? AND datastore_id = ?
                AND status NOT IN ('completed', 'failed')
                LIMIT 1
                """,
                (new_job.parameters.target, 1),
            )
            in_progress_job = cursor.fetchone()
            if not in_progress_job:
                cursor.execute(
                    """
                    INSERT INTO job
                    (
                        target,
                        datastore_id,
                        status,
                        parameters,
                        created_at,
                        created_by
                    )
                    VALUES
                    ( ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_job.parameters.target,
                        1,
                        new_job.status,
                        json.dumps(
                            new_job.parameters.model_dump(by_alias=True)
                        ),
                        new_job.created_at,
                        json.dumps(
                            new_job.created_by.model_dump(by_alias=True)
                        ),
                    ),
                )
                job_id = cursor.lastrowid
                conn.commit()
                new_job.job_id = str(job_id)
                return new_job
            else:
                conn.rollback()
                raise JobExistsException(
                    f"Job already in progress for {new_job.parameters.target}"
                )
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def update_job(
        self,
        job_id: str,
        status: JobStatus | None,
        description: str | None,
        log: str | None,
    ) -> Job:
        """
        Updates job with supplied job_id with new status, log, or description.
        Ensures atomic, isolated update.
        """
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            job_row = self._get_job_row_with_logs(cursor, job_id)
            if job_row is None:
                raise NotFoundException(f"Could not find job with id {job_id}")
            if job_row["status"] in ["completed", "failed"]:
                raise JobAlreadyCompleteException(
                    f"Job with id {job_id} has already been completed"
                )
            if description is not None:
                cursor.execute(
                    """
                    UPDATE job
                    SET parameters = json_set(parameters, '$.description', ?)
                    WHERE job_id = ?
                    """,
                    (description, int(job_id)),
                )
                cursor.execute(
                    """
                    INSERT INTO job_log (job_id, msg, at)
                    VALUES (?, ?, ?)
                    """,
                    (job_id, "Added update description", datetime.now()),
                )

            if status is not None:
                cursor.execute(
                    "UPDATE job SET status = ? WHERE job_id = ?",
                    (status, job_id),
                )
                cursor.execute(
                    """
                    INSERT INTO job_log (job_id, msg, at)
                    VALUES (?, ?, ?)
                    """,
                    (job_id, f"Set status: {status}", datetime.now()),
                )

            if log is not None:
                cursor.execute(
                    """
                    INSERT INTO job_log (job_id, msg, at)
                    VALUES (?, ?, ?)
                    """,
                    (job_id, log, datetime.now()),
                )
            conn.commit()
            job_row = self._get_job_row_with_logs(cursor, job_id)
            if job_row is None:
                raise Exception(
                    f"Could not find job with id {job_id} after update"
                )
            return Job(
                job_id=str(job_row["job_id"]),
                status=job_row["status"],
                parameters=json.loads(job_row["parameters"]),
                created_at=job_row["created_at"].isoformat(),
                created_by=json.loads(job_row["created_by"]),
                log=[
                    Log(at=row["at"], message=row["message"])
                    for row in json.loads(job_row["logs_json"])
                ],
            )
        except sqlite3.Error as e:
            conn.rollback()
            raise e
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def initialize_maintenance(self) -> MaintenanceStatus:
        """
        Inserts an initial maintenance status row if table is empty
        """
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM maintenance")
            count = cursor.fetchone()[0]
            if count == 0:
                timestamp = datetime.now().isoformat()
                cursor.execute(
                    """
                    INSERT INTO maintenance (
                        datastore_id,
                        msg,
                        paused,
                        timestamp
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        1,
                        "Initial status inserted by at startup.",
                        False,
                        timestamp,
                    ),
                )
                conn.commit()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT msg, paused, timestamp FROM maintenance
                WHERE datastore_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (1,),
            )
            row = cursor.fetchone()
            return MaintenanceStatus(
                msg=row["msg"],
                paused=bool(row["paused"]),
                timestamp=str(row["timestamp"]),
            )
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def get_latest_maintenance_status(self) -> MaintenanceStatus:
        """
        Retrieves the latest maintenance status, initializing if necessary
        """
        conn = self._conn()
        try:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT msg, paused, timestamp FROM maintenance
                WHERE datastore_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (1,),
            )
            row = cursor.fetchone()
            if row is None:
                return self.initialize_maintenance()

            return MaintenanceStatus(
                msg=row["msg"],
                paused=bool(row["paused"]),
                timestamp=str(row["timestamp"]),
            )
        finally:
            conn.close()

    def get_maintenance_history(self) -> list[MaintenanceStatus]:
        """
        Returns full history of maintenance entries, initializing if needed.
        """
        conn = self._conn()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT msg, paused, timestamp FROM maintenance
                WHERE datastore_id = ?
                ORDER BY timestamp DESC
                """,
                (1,),
            )
            rows = cursor.fetchall()
            if rows:
                return [
                    MaintenanceStatus(
                        msg=row["msg"],
                        paused=bool(row["paused"]),
                        timestamp=str(row["timestamp"]),
                    )
                    for row in rows
                ]
            else:
                return [self.initialize_maintenance()]
        finally:
            conn.close()

    def set_maintenance_status(
        self, msg: str, paused: bool
    ) -> MaintenanceStatus:
        """
        Inserts a new maintenance status record.
        """
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            timestamp = datetime.now().isoformat()
            cursor.execute(
                """
                INSERT INTO maintenance (datastore_id, msg, paused, timestamp)
                VALUES (?, ?, ?, ?)
                """,
                (
                    1,
                    msg,
                    paused,
                    timestamp,
                ),
            )
            conn.commit()
            return self.get_latest_maintenance_status()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def get_targets(self) -> list[Target]:
        conn = self._conn()
        try:
            cursor = conn.cursor()
            target_rows = cursor.execute(
                """
                SELECT
                    name,
                    datastore_id,
                    status,
                    action,
                    last_updated_at,
                    last_updated_by
                FROM target
                WHERE datastore_id = ?
                """,
                (1,),
            ).fetchall()
            return [
                Target(
                    name=target_row["name"],
                    status=target_row["status"],
                    action=target_row["action"].split(","),
                    last_updated_at=target_row["last_updated_at"].isoformat(),
                    last_updated_by=UserInfo(
                        **json.loads(target_row["last_updated_by"])
                    ),
                )
                for target_row in target_rows
            ]
        finally:
            conn.close()

    def _upsert_one_target(
        self,
        cursor: sqlite3.Cursor,
        name: str,
        status: str,
        timestamp: datetime,
        created_by: str,
        action: str,
    ) -> None:
        cursor.execute(
            """
                INSERT INTO target (
                    name,
                    datastore_id,
                    status,
                    last_updated_at,
                    last_updated_by,
                    action
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(name, datastore_id) DO UPDATE SET
                    status = excluded.status,
                    last_updated_at = excluded.last_updated_at,
                    last_updated_by = excluded.last_updated_by,
                    action = excluded.action
                """,
            (name, 1, status, timestamp, created_by, action),
        )

    def update_target(self, job: Job) -> None:
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            self._upsert_one_target(
                cursor,
                job.parameters.target,
                job.status,
                datetime.now(),
                json.dumps(
                    job.created_by.model_dump(exclude_none=True, by_alias=True)
                ),
                ",".join(job.get_action()),
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def update_bump_targets(self, job: Job) -> None:
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            bump_manifesto = job.parameters.bump_manifesto
            if bump_manifesto is None:
                raise ValueError(
                    "Can't update bump targets if bump_manifesto is None"
                )
            updates = [
                update
                for update in bump_manifesto.data_structure_updates
                if update.release_status != "DRAFT"
            ]
            version = job.parameters.bump_to_version
            created_by = json.dumps(
                job.created_by.model_dump(exclude_none=True, by_alias=True)
            )
            cursor = conn.cursor()
            for update in updates:
                operation = (
                    "RELEASED"
                    if update.release_status == "PENDING_RELEASE"
                    else "REMOVED"
                )
                self._upsert_one_target(
                    cursor,
                    update.name,
                    job.status,
                    datetime.now(),
                    created_by,
                    ",".join([operation, str(version)]),
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def get_datastore(self, datastore_id: int) -> Datastore:
        conn = self._conn()
        try:
            cursor = conn.cursor()
            datastore = cursor.execute(
                """
                SELECT
                    datastore_id,
                    rdn,
                    description,
                    directory,
                    name,
                    bump_enabled
                FROM datastore
                WHERE datastore_id = ?
                """,
                (datastore_id,),
            ).fetchone()
            return Datastore(
                datastore_id=datastore["datastore_id"],
                rdn=datastore["rdn"],
                description=datastore["description"],
                directory=datastore["directory"],
                name=datastore["name"],
                bump_enabled=datastore["bump_enabled"],
            )
        finally:
            conn.close()

    def get_datastore_id_from_rdn(self, rdn: str) -> int | None:
        conn = self._conn()
        try:
            cursor = conn.cursor()
            datastore_id = cursor.execute(
                """
                SELECT
                    datastore_id
                FROM datastore
                WHERE rdn = ?
                """,
                (rdn,),
            ).fetchone()
            return int(datastore_id) if datastore_id is not None else None
        finally:
            conn.close()
