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


class ProjectError(StatementError):
    def __init__(self, message: str):
        StatementError.__init__(self, message)


class ProjectFolderNotFound(ProjectError):
    def __init__(self, project_path: Path):
        message = f"Project folder not found: {project_path}"
        ProjectError.__init__(self, message)


class ProjectSubFolderNotFound(ProjectError):
    def __init__(self, expected_path: Path):
        message = f"Project sub-folder not found: {expected_path}"
        ProjectError.__init__(self, message)


class ProjectDatabaseMissing(ProjectError):
    def __init__(self, db_path: Path):
        message = f"Project database not found: {db_path}"
        ProjectError.__init__(self, message)


class ProjectConfigMissing(ProjectError):
    def __init__(self, config_path: Path):
        message = f"Project config folder not found or contains no .toml files: {config_path}"
        ProjectError.__init__(self, message)
