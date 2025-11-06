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
    jwt_auth: Literal["FULL"] | Literal["SKIP_SIGNATURE"] | Literal["OFF"]
    

def _initialize_environment() -> Environment:
    jwt_auth = os.environ.get("JWT_AUTH", "FULL")
    if jwt_auth not in ["FULL", "SKIP_SIGNATURE", "OFF"]:
        raise ValueError("Invalid value for JWT_AUTH: {jwt_auth}") 
    return Environment(
      docker_host_name=os.environ["DOCKER_HOST_NAME"],
      commit_id=os.environ["COMMIT_ID"],
      sqlite_url=os.environ["SQLITE_URL"],
      jwks_url=os.environ["JWKS_URL"],
      stack=os.environ["STACK"],
      jwt_auth=jwt_auth
    )


environment = _initialize_environment()
