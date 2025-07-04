class DataNotFoundException(Exception):
    def __init__(self, msg):
        super().__init__()
        self.message = {
            "type": "DATA_NOT_FOUND",
            "code": 105,
            "service": "datastore-api",
            "message": msg,
        }

    def to_dict(self):
        return self.message


class InvalidStorageFormatException(Exception):
    def __init__(self, msg):
        super().__init__()
        self.message = {
            "type": "SYSTEM_ERROR",
            "code": 202,
            "service": "datastore-api",
            "message": msg,
        }

    def to_dict(self):
        return self.message


class RequestValidationException(Exception):
    def __init__(self, msg):
        super().__init__()
        self.message = {
            "type": "REQUEST_VALIDATION_ERROR",
            "code": 101,
            "service": "datastore-api",
            "message": msg,
        }

    def to_dict(self):
        return self.message


class InvalidDraftVersionException(Exception): ...
