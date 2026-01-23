import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from datastore_api.adapter.db.migrations import apply_migrations
from datastore_api.api import setup_api
from datastore_api.common.exceptions import MigrationException
from datastore_api.config import environment
from datastore_api.config.logging import setup_logging

logger = logging.getLogger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator:
    try:
        DB_PATH = Path(environment.sqlite_url)
        MIGRATIONS_DIR = Path(environment.migrations_dir)
        apply_migrations(DB_PATH, MIGRATIONS_DIR)
    except MigrationException as e:
        logger.error(f"Startup aborted due to migration failure: {e}")
        raise
    yield


app = FastAPI(lifespan=lifespan)
setup_logging(app)
setup_api(app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "datastore_api.main:app", host="0.0.0.0", port=8000, reload=True
    )
