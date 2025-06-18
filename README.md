# Datastore API
API service for a microdata.no datastore.

## Development

### Get started
After cloning the repo, install and activate an environment with:
```sh
uv venv && uv sync && source .venv/bin/activate 
```

### Running the test suite
To run the unit and integration tests:
```sh
uv run pytest
```

### Running the application
You can run the application locally with:
```sh
uv run python -m datastore_api.main
```

## Built with
- [FastAPI](https://fastapi.tiangolo.com/)
- [Uvicorn](https://www.uvicorn.org/)
- [Gunicorn](https://gunicorn.org/)
- [uv](https://docs.astral.sh/uv/)

