from __future__ import annotations


class AdapterError(Exception):
    code = "INTERNAL_ERROR"
    exit_code = 1

    def __init__(self, *args, code: str | None = None, message: str | None = None,
                 exit_code: int | None = None):
        if len(args) == 3:
            code, message, exit_code = args
        elif len(args) == 1 and message is None:
            message = args[0]
        elif args:
            raise TypeError("AdapterError accepts message or code, message, exit_code")
        if message is None:
            raise TypeError("AdapterError message is required")
        self.message = str(message)
        if code is not None:
            self.code = str(code)
        if exit_code is not None:
            self.exit_code = int(exit_code)
        super().__init__(self.message)


class InvalidInputError(AdapterError):
    code = "INVALID_INPUT"
    exit_code = 2


class UsageError(AdapterError):
    code = "USAGE_ERROR"
    exit_code = 2


class ConfigError(AdapterError):
    code = "CONFIG_ERROR"
    exit_code = 3


class AuthError(AdapterError):
    code = "AUTH_ERROR"
    exit_code = 4


class MattermostAPIError(AdapterError):
    code = "MATTERMOST_API_ERROR"
    exit_code = 5
