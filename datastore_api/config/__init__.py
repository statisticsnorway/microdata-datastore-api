import json
import os
from dataclasses import dataclass
from typing import Literal


@dataclass
class Environment:
    docker_host_name: str
    commit_id: str
    sqlite_url: str
    jwks_url: str
    stack: str
    jwt_auth: Literal["FULL"] | Literal["SKIP_SIGNATURE"] | Literal["OFF"]
    access_control_file: str
    datastores_root_dir: str


def _initialize_environment() -> Environment:
    jwt_auth = os.environ.get("JWT_AUTH", "FULL")
    if jwt_auth not in ["FULL", "SKIP_SIGNATURE", "OFF"]:
        raise ValueError(f"Invalid value for JWT_AUTH: {jwt_auth}")
    return Environment(
        docker_host_name=os.environ["DOCKER_HOST_NAME"],
        commit_id=os.environ["COMMIT_ID"],
        sqlite_url=os.environ["SQLITE_URL"],
        jwks_url=os.environ["JWKS_URL"],
        stack=os.environ["STACK"],
        jwt_auth=jwt_auth,
        access_control_file=os.environ["ACCESS_CONTROL_FILE"],
        datastores_root_dir=os.environ["DATASTORES_ROOT_DIR"],
    )


environment = _initialize_environment()


@dataclass
class AccessControl:
    allowed_users: list[str]


def _initialize_access_control() -> AccessControl:
    with open(environment.access_control_file, encoding="utf-8") as f:
        access_control_file = json.load(f)
    return AccessControl(allowed_users=access_control_file["allowed_users"])


access_control = _initialize_access_control()
