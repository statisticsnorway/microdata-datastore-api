import os
from dataclasses import dataclass


@dataclass
class Environment:
    datastore_root_dir: str
    docker_host_name: str
    commit_id: str
    input_dir: str
    sqlite_url: str
    jwks_url: str
    stack: str
    jwt_auth: bool
    bump_enabled: bool


def _initialize_environment() -> Environment:
    return Environment(
        datastore_root_dir=os.environ["DATASTORE_ROOT_DIR"],
        docker_host_name=os.environ["DOCKER_HOST_NAME"],
        commit_id=os.environ["COMMIT_ID"],
        input_dir=os.environ["INPUT_DIR"],
        sqlite_url=os.environ["SQLITE_URL"],
        jwks_url=os.environ["JWKS_URL"],
        stack=os.environ["STACK"],
        jwt_auth=(
            False if os.environ.get("JWT_AUTH", "true") == "false" else True
        ),
        bump_enabled=(
            True if os.environ.get("BUMP_ENABLED", "false") == "true" else False
        ),
    )


environment = _initialize_environment()
