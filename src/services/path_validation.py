"""Path validation utilities for security-critical file operations.

Provides functions to validate that file paths remain within expected
boundaries, preventing path traversal attacks (CWE-22).
"""

from pathlib import Path


class PathValidationError(Exception):
    """Raised when a path fails security validation.

    Attributes:
        message: Human-readable error description.
        path: The path that failed validation.
        root: The root directory the path should be within.
    """

    def __init__(self, message: str, path: Path, root: Path) -> None:
        """Initialize PathValidationError.

        Args:
            message: Human-readable error description.
            path: The path that failed validation.
            root: The root directory the path should be within.
        """
        self.message = message
        self.path = path
        self.root = root
        super().__init__(message)


def validate_path_within_root(path: Path | str, root: Path | str) -> Path:
    """Validate that a path is within a root directory.

    Resolves both paths to absolute paths and verifies the target path
    is contained within the root directory. This prevents path traversal
    attacks using sequences like '../' or absolute paths outside the root.

    Args:
        path: The path to validate (can be relative or absolute).
        root: The root directory that path must be within.

    Returns:
        Path: The resolved absolute path if validation passes.

    Raises:
        PathValidationError: If path is not within root directory.
        PathValidationError: If root directory does not exist.

    Example:
        >>> root = Path("/data/runs")
        >>> validate_path_within_root("/data/runs/file.json", root)
        PosixPath('/data/runs/file.json')

        >>> validate_path_within_root("../../../etc/passwd", root)
        PathValidationError: Path escapes root directory

    Security:
        This function addresses CWE-22 (Path Traversal) by:
        1. Resolving symlinks and '..' sequences via resolve()
        2. Checking containment using is_relative_to()
        3. Requiring the root directory to exist
    """
    # Convert to Path objects if strings
    path = Path(path) if isinstance(path, str) else path
    root = Path(root) if isinstance(root, str) else root

    # Resolve to absolute paths (follows symlinks, resolves ..)
    try:
        resolved_root = root.resolve()
    except (OSError, RuntimeError) as e:
        raise PathValidationError(
            f"Cannot resolve root directory: {root}",
            path=path,
            root=root,
        ) from e

    # Root must exist
    if not resolved_root.exists():
        raise PathValidationError(
            f"Root directory does not exist: {resolved_root}",
            path=path,
            root=root,
        )

    if not resolved_root.is_dir():
        raise PathValidationError(
            f"Root path is not a directory: {resolved_root}",
            path=path,
            root=root,
        )

    # If path is relative, make it relative to root
    if not path.is_absolute():
        path = root / path

    # Resolve the target path
    try:
        resolved_path = path.resolve()
    except (OSError, RuntimeError) as e:
        raise PathValidationError(
            f"Cannot resolve path: {path}",
            path=path,
            root=root,
        ) from e

    # Check if resolved path is within resolved root
    try:
        if not resolved_path.is_relative_to(resolved_root):
            raise PathValidationError(
                f"Path escapes root directory: {resolved_path} is not within {resolved_root}",
                path=path,
                root=root,
            )
    except ValueError:
        # is_relative_to raises ValueError in some Python versions for non-relative paths
        raise PathValidationError(
            f"Path escapes root directory: {resolved_path} is not within {resolved_root}",
            path=path,
            root=root,
        ) from None

    return resolved_path


def validate_file_path_within_root(path: Path | str, root: Path | str) -> Path:
    """Validate that a file path is within a root directory and exists.

    Same as validate_path_within_root but also verifies the path points
    to an existing file (not a directory).

    Args:
        path: The file path to validate.
        root: The root directory that path must be within.

    Returns:
        Path: The resolved absolute path if validation passes.

    Raises:
        PathValidationError: If path is not within root directory.
        PathValidationError: If path does not exist or is not a file.

    Example:
        >>> root = Path("/data/runs")
        >>> validate_file_path_within_root("/data/runs/file.json", root)
        PosixPath('/data/runs/file.json')
    """
    resolved_path = validate_path_within_root(path, root)

    if not resolved_path.exists():
        raise PathValidationError(
            f"File does not exist: {resolved_path}",
            path=Path(path),
            root=Path(root),
        )

    if not resolved_path.is_file():
        raise PathValidationError(
            f"Path is not a file: {resolved_path}",
            path=Path(path),
            root=Path(root),
        )

    return resolved_path
