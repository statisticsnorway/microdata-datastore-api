from uvicorn.config import LOGGING_CONFIG


def setup_uvicorn_logging():
    fmt = (
        '{"@timestamp": "%(asctime)s",'
        '"pid": "%(process)d", '
        '"loggerName": "%(name)s",'
        '"levelName": "%(levelname)s",'
        '"schemaVersion": "v3",'
        '"serviceVersion": "TODO",'
        '"serviceName": "datastore-api",'
        '"xRequestId": "",'
        '"message": "%(message)s"}'
    )
    datefmt = "%Y-%m-%dT%H:%M:%S"
    LOGGING_CONFIG["formatters"]["default"]["fmt"] = fmt
    LOGGING_CONFIG["formatters"]["default"]["datefmt"] = datefmt
    LOGGING_CONFIG["formatters"]["access"]["fmt"] = fmt
    LOGGING_CONFIG["formatters"]["access"]["datefmt"] = datefmt
