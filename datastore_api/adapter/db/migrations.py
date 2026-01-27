import hashlib
import logging
import sqlite3
from datetime import date, datetime
from pathlib import Path

from datastore_api.common.exceptions import MigrationException

logger = logging.getLogger()


def _file_hash(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def _parse_migration_date(filename: str) -> date:
    date_from_filename = filename.split("_", 1)[0]
    if len(date_from_filename) != 8 or not date_from_filename.isdigit():
        raise MigrationException(
            f"Cannot parse date from filename: {filename}. "
            "Migrations filename must start with YYYYMMDD_"
        )
    return date(
        year=int(date_from_filename[0:4]),
        month=int(date_from_filename[4:6]),
        day=int(date_from_filename[6:8]),
    )


def _validate_applied_migrations(
    applied_migrations: dict[str, str], disk_migrations: dict[str, str]
) -> None:
    # Detects if any previously applied migration file is missing on disk
    missing_in_directory = set(applied_migrations) - set(disk_migrations)
    if missing_in_directory:
        missing_list = ", ".join(sorted(missing_in_directory))
        raise MigrationException(
            "The following file(s) from the migrations table are missing "
            f"from the migrations directory: {missing_list}"
        )

    # Detects migrations that has been applied but later modified on disk
    altered_migrations = {
        filename
        for filename in applied_migrations.keys() & disk_migrations.keys()
        if applied_migrations[filename] != disk_migrations[filename]
    }

    if altered_migrations:
        altered_list = ", ".join(sorted(altered_migrations))
        raise MigrationException(
            "The following migration file(s) has been modified after being "
            f"applied: {altered_list} "
        )


def apply_migrations(db_path: Path, migrations_dir: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS migrations (
                filename TEXT PRIMARY KEY,
                hash TEXT NOT NULL,
                applied_at TIMESTAMP NOT NULL
            )
            """
        )
        conn.commit()

        applied_migrations: dict[str, str] = {
            filename: hash
            for filename, hash in cursor.execute(
                """
                SELECT filename, hash
                FROM migrations
                """
            )
        }

        last_applied_migration_date = (
            _parse_migration_date(max(applied_migrations.keys()))
            if applied_migrations
            else None
        )

        migration_files = sorted(Path(migrations_dir).glob("*.sql"))
        disk_migrations = {
            path.name: _file_hash(path) for path in migration_files
        }

        _validate_applied_migrations(applied_migrations, disk_migrations)

        new_migration_files = set(disk_migrations) - set(applied_migrations)
        for migration_file in migration_files:
            if migration_file.name in new_migration_files:
                filename = migration_file.name
                hash = _file_hash(migration_file)
                migration_date = _parse_migration_date(filename)
                now = datetime.now().isoformat()

                if (
                    last_applied_migration_date
                    and migration_date < last_applied_migration_date
                ):
                    raise MigrationException(
                        "Date in filename cannot be older than the "
                        "last applied migration"
                    )

                with open(migration_file, "r") as file:
                    migration_sql = file.read()
                    try:
                        with conn:
                            cursor.executescript(migration_sql)
                            cursor.execute(
                                """
                                INSERT INTO migrations (
                                    filename,
                                    hash,
                                    applied_at
                                )
                                VALUES (?, ?, ?)
                                """,
                                (filename, hash, now),
                            )
                        logger.info(f"Applied migration: {migration_file}")
                    except Exception as e:
                        raise MigrationException(
                            f"Migration failed: {migration_file}, {e}"
                        ) from e
    except Exception as e:
        raise MigrationException(f"Migration failed: {e}") from e
    finally:
        conn.close()
