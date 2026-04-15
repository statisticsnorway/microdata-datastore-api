import logging
from pathlib import Path

from fastapi import FastAPI

from datastore_api.adapter import local_storage
from datastore_api.adapter.db.migrations import apply_migrations
from datastore_api.adapter.db.sqlite import SqliteDbClient
from datastore_api.api import setup_api
from datastore_api.common.exceptions import MigrationException
from datastore_api.config import environment
from datastore_api.config.logging import setup_logging

logger = logging.getLogger()


def setup_db(db_path: Path, migrations_dir: Path) -> None:
    try:
        apply_migrations(db_path, migrations_dir)
        insert_baseline(db_path)
    except MigrationException as e:
        logger.error(f"Startup aborted due to migration failure: {e}")
        raise


def insert_baseline(db_path: Path) -> None:
    """
    Insert baseline datastores into the sqlite database if a baseline file
    is provided via the BASELINE_FILE environment variable (optional).
    """
    baseline_file = local_storage.read_baseline_file()
    if baseline_file is None:
        return None

    client = SqliteDbClient(str(db_path))
    for datastore_baseline in baseline_file.datastores:
        client.insert_new_datastore(
            rdn=datastore_baseline.rdn,
            description=datastore_baseline.description,
            directory=datastore_baseline.directory,
            name=datastore_baseline.name,
            bump_enabled=datastore_baseline.bump_enabled,
        )


setup_db(Path(environment.sqlite_url), Path(environment.migrations_dir))
app = FastAPI(title="Datastore API", version="1.0.0")
setup_logging(app)
setup_api(app)

# update OpenAPI docs
if environment.stack == "dev":
    import yaml

    with open("doc/openapi.yaml", "w") as f:
        yaml.dump(app.openapi(), f, sort_keys=False)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "datastore_api.main:app", host="0.0.0.0", port=8000, reload=True
    )
