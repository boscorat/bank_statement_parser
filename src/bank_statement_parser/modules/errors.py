from pathlib import Path


class StatementError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)


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


class TestGateFailure(StatementError):
    """Raised when bsp's own pytest suite fails during TestHarness.setup().

    Attributes:
        failed: Number of test failures reported by pytest.
        errors: Number of test errors reported by pytest.
        output: Captured stdout/stderr from the pytest run.
    """

    __slots__ = ("failed", "errors", "output")

    def __init__(self, failed: int, errors: int, output: str) -> None:
        self.failed = failed
        self.errors = errors
        self.output = output
        message = f"bsp test gate failed: {failed} failed, {errors} errors.\n{output}"
        StatementError.__init__(self, message)
