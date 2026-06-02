from pathlib import Path


class StatementError(Exception):
    """Root exception for statement processing errors."""

    pass


class ConfigError(StatementError):
    """Configuration error during setup."""

    pass


class ConfigFileError(ConfigError):
    """Configuration file not found."""

    def __init__(self, config_file: Path) -> None:
        super().__init__(f"No User or Base Config File : {config_file}")


class NotAValidConfigFolder(ConfigError):
    """Config folder is missing required TOML files."""

    def __init__(self, config_path: Path, missing_files: list[str]) -> None:
        message = f"Config folder '{config_path}' is missing required .toml files: {', '.join(missing_files)}"
        super().__init__(message)


class ProjectError(StatementError):
    """Project error during setup or operation."""

    pass


class ProjectFolderNotFound(ProjectError):
    """Project folder not found at specified path."""

    def __init__(self, project_path: Path) -> None:
        super().__init__(f"Project folder not found: {project_path}")


class ProjectSubFolderNotFound(ProjectError):
    """Required project subfolder not found."""

    def __init__(self, expected_path: Path) -> None:
        super().__init__(f"Project sub-folder not found: {expected_path}")


class ProjectDatabaseMissing(ProjectError):
    """Project database file not found."""

    def __init__(self, db_path: Path) -> None:
        super().__init__(f"Project database not found: {db_path}")


class ProjectConfigMissing(ProjectError):
    """Project config folder missing or empty."""

    def __init__(self, config_path: Path) -> None:
        super().__init__(f"Project config folder not found or contains no .toml files: {config_path}")


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
        super().__init__(message)
