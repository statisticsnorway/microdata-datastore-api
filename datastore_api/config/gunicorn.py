import logging

from gunicorn import glogging


class CustomLogger(glogging.Logger):
    """Custom logger for Gunicorn log messages."""

    def setup(self, cfg):
        """Configure Gunicorn application logging configuration."""
        super().setup(cfg)

        # Override Gunicorn's `error_log` configuration.
        self._set_handler(
            self.error_log,
            cfg.errorlog,
            logging.Formatter(
                fmt=(
                    '{"@timestamp": "%(asctime)s",'
                    '"pid": "%(process)d", '
                    '"loggerName": "gunicorn_custom",'
                    '"levelName": "%(levelname)s",'
                    '"schemaVersion": "v3",'
                    '"serviceVersion": "TODO",'
                    '"serviceName": "metadata-service",'
                    '"xRequestId": "TODO",'
                    '"message": "%(message)s}"'
                ),
                datefmt="%Y-%m-%dT%H:%M:%SZ",
            ),
        )
