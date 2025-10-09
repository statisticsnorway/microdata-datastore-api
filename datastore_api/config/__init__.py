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
    )


environment = _initialize_environment()
