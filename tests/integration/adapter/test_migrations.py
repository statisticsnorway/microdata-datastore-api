import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from datastore_api.adapter.db.migrations import apply_migrations
from datastore_api.common.exceptions import MigrationException


@pytest.fixture
def sqlite_db(tmp_path):
    db_file = tmp_path / "test.db"
    yield db_file


def _copy_migrations(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for file in src.glob("*.sql"):
        shutil.copy(file, dst / file.name)


def test_valid_migrations(tmp_path, sqlite_db):
    migrations_dir = tmp_path / "migrations"
    _copy_migrations(Path("tests/resources/migrations/valid"), migrations_dir)
    conn = sqlite3.connect(sqlite_db)
    try:
        apply_migrations(sqlite_db, migrations_dir)
        db_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        tables = {
            "job",
            "maintenance",
            "target",
            "job_log",
            "datastore",
            "anothertable",
            "migrations",
        }
        assert tables.issubset(db_tables)

        migrated_files = [
            row[0] for row in conn.execute("SELECT filename FROM migrations")
        ]
        assert "20260101_valid_schema.sql" in migrated_files
    finally:
        conn.close()


def test_invalid_migrations(tmp_path, sqlite_db):
    migrations_dir = tmp_path / "migrations"
    _copy_migrations(Path("tests/resources/migrations/invalid"), migrations_dir)
    with pytest.raises(MigrationException):
        apply_migrations(sqlite_db, migrations_dir)


def test_run_migrations_with_altered_file_forbidden(tmp_path, sqlite_db):
    migrations_dir = tmp_path / "migrations"
    _copy_migrations(Path("tests/resources/migrations/valid"), migrations_dir)
    apply_migrations(sqlite_db, migrations_dir)

    # Simulate a previously applied migration by inserting it manually
    # into the migrations table with an intentionally incorrect hash.

    conn = sqlite3.connect(sqlite_db)
    filename = "20260101_hash_changed.sql"
    hash = "fakehash"
    applied_at = datetime.now().isoformat()
    conn.execute(
        """
        INSERT INTO migrations (filename, hash, applied_at)
        VALUES (?, ?, ?)
        """,
        (filename, hash, applied_at),
    )
    conn.commit()
    conn.close()

    # Copy a migration file with the same filename but different contents
    # (and therefore a different hash) into the migrations directory.
    _copy_migrations(
        Path("tests/resources/migrations/hash_changed"), migrations_dir
    )

    with pytest.raises(MigrationException) as e:
        apply_migrations(sqlite_db, migrations_dir)

    assert "has been modified after being applied" in str(e.value)


def test_migration_date_older_than_last_applied_forbidden(tmp_path, sqlite_db):
    migrations_dir = tmp_path / "migrations"

    _copy_migrations(
        Path("tests/resources/migrations/valid"),
        migrations_dir,
    )
    apply_migrations(sqlite_db, migrations_dir)

    _copy_migrations(
        Path("tests/resources/migrations/date_violation"),
        migrations_dir,
    )
    with pytest.raises(MigrationException) as e:
        apply_migrations(sqlite_db, migrations_dir)

    assert (
        "Date in filename cannot be older than the last applied migration"
        in str(e.value)
    )

def test_migration_invalid_filename_forbidden(tmp_path, sqlite_db):
    migrations_dir = tmp_path / "migrations"

    _copy_migrations(
        Path("tests/resources/migrations/name_violation"),
        migrations_dir,
    )
    with pytest.raises(MigrationException) as e:
        apply_migrations(sqlite_db, migrations_dir)

    assert (
        "Cannot parse date from filename"
        in str(e.value)
    )


def test_all_migrations_are_valid(sqlite_db):
    # Test passes if all migration files can be applied without error
    migrations_dir = Path("migrations")
    apply_migrations(sqlite_db, migrations_dir)
