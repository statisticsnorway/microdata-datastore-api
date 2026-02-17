import logging
from pathlib import Path

from fastapi import FastAPI

from datastore_api.adapter.db.migrations import apply_migrations
from datastore_api.api import setup_api
from datastore_api.common.exceptions import MigrationException
from datastore_api.config import environment
from datastore_api.config.logging import setup_logging

logger = logging.getLogger()


def setup_db(db_path: Path, migrations_dir: Path) -> None:
    try:
        apply_migrations(db_path, migrations_dir)
    except MigrationException as e:
        logger.error(f"Startup aborted due to migration failure: {e}")
        raise


setup_db(Path(environment.sqlite_url), Path(environment.migrations_dir))
app = FastAPI()
setup_logging(app)
setup_api(app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "datastore_api.main:app", host="0.0.0.0", port=8000, reload=True
    )
