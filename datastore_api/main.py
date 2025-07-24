import logging

from fastapi import FastAPI

from datastore_api.api import setup_api
from datastore_api.config.logging import setup_logging

logger = logging.getLogger()

app = FastAPI()
setup_logging(app)
setup_api(app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "datastore_api.main:app", host="0.0.0.0", port=8000, reload=True
    )
