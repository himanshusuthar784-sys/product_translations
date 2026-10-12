class ApiError(Exception):
    """Domain error mapped to an HTTP response by the API's exception handler."""

    def __init__(self, status_code: int, code: str, message: str, **extra):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.extra = extra


class NotFound(ApiError):
    def __init__(self, what: str, ident: str):
        super().__init__(404, "not_found", f"{what} '{ident}' not found")


class Conflict(ApiError):
    def __init__(self, code: str, message: str, **extra):
        super().__init__(409, code, message, **extra)


class Unprocessable(ApiError):
    def __init__(self, code: str, message: str, **extra):
        super().__init__(422, code, message, **extra)
