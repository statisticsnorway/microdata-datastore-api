import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from datastore_api.adapter.db.migrations import apply_migrations
from datastore_api.api import setup_api
from datastore_api.common.exceptions import MigrationException
from datastore_api.config.logging import setup_logging

logger = logging.getLogger()

DB_PATH = Path(os.environ["SQLITE_URL"])
MIGRATIONS_DIRECTORY = Path("migrations")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator:
    try:
        apply_migrations(DB_PATH, MIGRATIONS_DIRECTORY)
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
