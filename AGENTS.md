# DATASTORE-API

This application serves as a datastore for the microdata.no plattform. It is a FastAPI application that persist data as
parquet files, metadata in json files or sqlite tables, and job information in sqlite tables.

## Modules
The application uses a layered architecture inspired by hexagonal architecture:
- **datastore_api/** directory containing the source code
    - **api/**: definitions for the restful api
    - **domain/**: all core domain logic for the application
    - **adapter/**: adapters for databases, filesystem and external services
    - **common/**: common modules used by the whole stack
    - **config/**: configuration for application and logging
    - **main.py**: the entrypoint for application startup

The application flows from api, to  domain, to adapters. No imports in the wrong direction. api can import from domain
or adapters, but adapters can never import from domain or api.
When developing a feature that cuts through every layer of the application, the implementation should start from the
bottom to the top, from adapters to api.

## Development Workflow (uv)
- Use `uv` for Python package and environment management.
- Add dependencies with `uv add <package>` (and dev dependencies with `uv add --dev <package>`).
- Format code with `uv run ruff format`.
- Run autofixes with `uv run ruff check --fix`.
- Sort imports specifically with `uv run ruff check --fix --select I`.
- Run tests with `uv run pytest`

