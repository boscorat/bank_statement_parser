from pathlib import Path


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


class NotAValidConfigFolder(ConfigError):
    def __init__(self, config_path: Path, missing_files: list[str]):
        message = f"Config folder '{config_path}' is missing required .toml files: {', '.join(missing_files)}"
        ConfigError.__init__(self, message)
