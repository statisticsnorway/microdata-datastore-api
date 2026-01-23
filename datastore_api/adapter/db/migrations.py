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
    d = date(
        year=int(date_from_filename[0:4]),
        month=int(date_from_filename[4:6]),
        day=int(date_from_filename[6:8]),
    )
    return d


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
        for migration_file in migration_files:
            hash = _file_hash(Path(migration_file))
            filename = Path(migration_file).name
            migration_date = _parse_migration_date(filename)
            timestamp = datetime.now().isoformat()

            if filename in applied_migrations:
                stored_hash = applied_migrations[filename]

                if hash != stored_hash:
                    raise MigrationException(
                        f"Migration {filename} "
                        "has been modified after being applied"
                    )
                continue

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
                            (filename, hash, timestamp),
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
