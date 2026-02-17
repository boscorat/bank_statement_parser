class StatementError(BaseException):
    def __init__(self, message):
        BaseException.__init__(self, message)


class ConfigError(StatementError):
    def __init__(self, message):
        StatementError.__init__(self, message)


class ConfigFileError(ConfigError):
    def __init__(self, config_file):
        message = f"No User or Base Config File : {config_file}"
        ConfigError.__init__(self, message)
