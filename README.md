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
You can run the application locally with hot reloading:
```sh
export DOCKER_HOST_NAME=localhost \
export STACK=test \
export COMMIT_ID=abc123 \
export SQLITE_URL=test.db \
export JWT_AUTH=OFF \
export JWKS_URL=http://localhost
uv run python datastore_api/main.py
```

## Pre-commit
There are currently 3 active rules: Ruff-format, Ruff-lint and sync lock file.
Install pre-commit 
```sh
pip install pre-commit
```
If you've made changes to the pre-commit-config.yaml or its a new project install the hooks with:
```sh
pre-commit install
```
Now it should run when you do:
```sh
git commit
```

By default it only runs against changed files. To force the hooks to run against all files:
```sh
pre-commit run --all-files
```
if you dont have it installed on your system you can use: 
(but then it wont run when you use the git-cli)
```sh
uv run pre-commit
```
Read more about [pre-commit](https://pre-commit.com/#intro) 

## Built with
- [FastAPI](https://fastapi.tiangolo.com/)
- [Uvicorn](https://www.uvicorn.org/)
- [Gunicorn](https://gunicorn.org/)
- [uv](https://docs.astral.sh/uv/)

