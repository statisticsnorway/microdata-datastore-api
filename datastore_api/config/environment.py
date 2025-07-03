import os


def _initialize_environment() -> dict:
    return {
        "DATASTORE_ROOT_DIR": os.environ["DATASTORE_ROOT_DIR"],
        "DOCKER_HOST_NAME": os.environ["DOCKER_HOST_NAME"],
        "COMMIT_ID": os.environ["COMMIT_ID"],
    }


_ENVIRONMENT_VARIABLES = _initialize_environment()


def get(key: str) -> str:
    return _ENVIRONMENT_VARIABLES[key]
