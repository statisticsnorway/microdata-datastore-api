import os
from dataclasses import dataclass


@dataclass
class Environment:
    docker_host_name: str
    commit_id: str
    sqlite_url: str
    jwks_url: str
    stack: str
    jwt_auth: bool
    no_jwt_validation_auth: bool


def _initialize_environment() -> Environment:
    return Environment(
        docker_host_name=os.environ["DOCKER_HOST_NAME"],
        commit_id=os.environ["COMMIT_ID"],
        sqlite_url=os.environ["SQLITE_URL"],
        jwks_url=os.environ["JWKS_URL"],
        stack=os.environ["STACK"],
        jwt_auth=(
            False if os.environ.get("JWT_AUTH", "true") == "false" else True
        ),
        no_jwt_validation_auth=(
            True
            if os.environ.get("NO_JWT_VALIDATION_AUTH", "false") == "true"
            else False
        ),
    )


environment = _initialize_environment()
