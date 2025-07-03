import re
import sys
import uuid
import json
import logging
import datetime
from time import perf_counter_ns
from typing import Callable

from fastapi import Request
from contextvars import ContextVar

from datastore_api.config import environment


request_start_time: ContextVar[int] = ContextVar("request_start_time")
correlation_id: ContextVar[str] = ContextVar("correlation_id")
method: ContextVar[str] = ContextVar("method")
url: ContextVar[str] = ContextVar("url")
remote_host: ContextVar[str] = ContextVar("remote_host")
response_status: ContextVar[int] = ContextVar("response_status")
response_time_ms: ContextVar[int] = ContextVar("response_time_ms")


class MicrodataJSONFormatter(logging.Formatter):
    def __init__(self):
        self.host = environment.get("DOCKER_HOST_NAME")
        self.command = json.dumps(sys.argv)
        self.commit_id = environment.get("COMMIT_ID")

    def format(self, record: logging.LogRecord) -> str:
        stack_trace = ""
        if record.exc_info is not None:
            stack_trace = self.formatException(record.exc_info)
        return json.dumps(
            {
                "@timestamp": datetime.datetime.fromtimestamp(
                    record.created,
                    tz=datetime.timezone.utc,
                ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                + "Z",
                "command": self.command,
                "error.stack": stack_trace,
                "host": self.host,
                "message": record.getMessage(),
                "level": record.levelno,
                "levelName": record.levelname,
                "loggerName": record.name,
                "method": method.get(""),
                "responseTime": response_time_ms.get(""),
                "schemaVersion": "v3",
                "serviceName": "metadata-service",
                "serviceVersion": self.commit_id,
                "source_host": remote_host.get(""),
                "statusCode": response_status.get(""),
                "thread": record.threadName,
                "url": url.get(""),
                "xRequestId": re.sub(r"[^\w\-]", "", correlation_id.get("")),
            }
        )


def setup_logging(app, log_level=logging.INFO):
    logger = logging.getLogger()
    logger.setLevel(log_level)

    formatter = MicrodataJSONFormatter()

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next: Callable):
        request_start_time.set(perf_counter_ns())
        corr_id = request.headers.get("X-Request-ID", None)
        if corr_id is None:
            correlation_id.set("metadata-service-" + str(uuid.uuid1()))
        else:
            correlation_id.set(corr_id)
        method.set(request.method)
        url.set(str(request.url))
        client = request.client
        host = ""
        if client is not None:
            host = client.host
        remote_host.set(host)

        response = await call_next(request)

        response_time = int(
            (perf_counter_ns() - request_start_time.get()) / 1_000_000
        )
        response_time_ms.set(response_time)
        response_status.set(response.status_code)
        response.headers["X-Request-ID"] = correlation_id.get()
        logger.info("responded")
        return response
