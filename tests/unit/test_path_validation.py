"""Tests for the path validation security utilities.

Tests the path validation functions that prevent path traversal attacks (CWE-22).
"""

from pathlib import Path

import pytest

from src.services.path_validation import (
    PathValidationError,
    validate_file_path_within_root,
    validate_path_within_root,
)

pytestmark = pytest.mark.tier1


class TestValidatePathWithinRoot:
    """Tests for validate_path_within_root function."""

    def test_valid_absolute_path_within_root(self, tmp_path: Path) -> None:
        """Test that a valid absolute path within root passes validation."""
        # Create a file in the temp directory
        test_file = tmp_path / "data" / "file.json"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.touch()

        result = validate_path_within_root(test_file, tmp_path)
        assert result == test_file.resolve()

    def test_valid_relative_path_within_root(self, tmp_path: Path) -> None:
        """Test that a valid relative path within root passes validation."""
        # Create a file in the temp directory
        test_file = tmp_path / "subdir" / "file.json"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.touch()

        # Use relative path from root
        result = validate_path_within_root("subdir/file.json", tmp_path)
        assert result == test_file.resolve()

    def test_path_traversal_with_dotdot_rejected(self, tmp_path: Path) -> None:
        """Test that '../' path traversal sequences are rejected."""
        # Create a nested structure
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        # Try to escape with ../
        with pytest.raises(PathValidationError) as exc_info:
            validate_path_within_root("../etc/passwd", subdir)

        assert "escapes root directory" in exc_info.value.message

    def test_absolute_path_outside_root_rejected(self, tmp_path: Path) -> None:
        """Test that absolute paths outside root are rejected."""
        with pytest.raises(PathValidationError) as exc_info:
            validate_path_within_root("/etc/passwd", tmp_path)

        assert "escapes root directory" in exc_info.value.message

    def test_nested_dotdot_sequences_rejected(self, tmp_path: Path) -> None:
        """Test that nested '../../../' sequences are rejected."""
        deep_dir = tmp_path / "a" / "b" / "c" / "d"
        deep_dir.mkdir(parents=True)

        with pytest.raises(PathValidationError) as exc_info:
            validate_path_within_root("../../../../../../../../etc/passwd", deep_dir)

        assert "escapes root directory" in exc_info.value.message

    def test_path_with_dotdot_escaping_subdir_rejected(self, tmp_path: Path) -> None:
        """Test that paths with '..' escaping the specified root are rejected.

        Even if the resolved path is within a parent of the root, it must
        remain within the specified root directory for security.
        """
        # Create nested directories
        subdir = tmp_path / "a" / "b"
        target = tmp_path / "a" / "file.json"
        subdir.mkdir(parents=True)
        target.touch()

        # ../file.json from a/b resolves to a/file.json which escapes subdir
        # This MUST be rejected even though it's within tmp_path
        with pytest.raises(PathValidationError) as exc_info:
            validate_path_within_root("../file.json", subdir)

        assert "escapes root directory" in exc_info.value.message

    def test_nonexistent_root_raises_error(self, tmp_path: Path) -> None:
        """Test that a nonexistent root directory raises an error."""
        nonexistent_root = tmp_path / "does_not_exist"

        with pytest.raises(PathValidationError) as exc_info:
            validate_path_within_root("file.txt", nonexistent_root)

        assert "does not exist" in exc_info.value.message

    def test_root_is_file_not_directory_raises_error(self, tmp_path: Path) -> None:
        """Test that a file passed as root raises an error."""
        root_file = tmp_path / "root_file.txt"
        root_file.touch()

        with pytest.raises(PathValidationError) as exc_info:
            validate_path_within_root("file.txt", root_file)

        assert "not a directory" in exc_info.value.message

    def test_string_inputs_accepted(self, tmp_path: Path) -> None:
        """Test that string inputs are accepted and converted to Path."""
        test_file = tmp_path / "file.txt"
        test_file.touch()

        result = validate_path_within_root(str(test_file), str(tmp_path))
        assert result == test_file.resolve()

    def test_path_inputs_accepted(self, tmp_path: Path) -> None:
        """Test that Path inputs are accepted directly."""
        test_file = tmp_path / "file.txt"
        test_file.touch()

        result = validate_path_within_root(test_file, tmp_path)
        assert result == test_file.resolve()


class TestValidateFilePathWithinRoot:
    """Tests for validate_file_path_within_root function."""

    def test_valid_file_passes(self, tmp_path: Path) -> None:
        """Test that a valid existing file passes validation."""
        test_file = tmp_path / "data.json"
        test_file.write_text("{}")

        result = validate_file_path_within_root(test_file, tmp_path)
        assert result == test_file.resolve()

    def test_nonexistent_file_raises_error(self, tmp_path: Path) -> None:
        """Test that a nonexistent file raises an error."""
        with pytest.raises(PathValidationError) as exc_info:
            validate_file_path_within_root(tmp_path / "missing.json", tmp_path)

        assert "does not exist" in exc_info.value.message

    def test_directory_instead_of_file_raises_error(self, tmp_path: Path) -> None:
        """Test that a directory path raises an error."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        with pytest.raises(PathValidationError) as exc_info:
            validate_file_path_within_root(subdir, tmp_path)

        assert "not a file" in exc_info.value.message

    def test_path_traversal_to_existing_file_rejected(self, tmp_path: Path) -> None:
        """Test that path traversal to an existing file is still rejected."""
        # Create a file outside the intended root
        outer_file = tmp_path / "outer" / "secret.txt"
        outer_file.parent.mkdir(parents=True)
        outer_file.write_text("secret data")

        # Create the intended root
        inner_root = tmp_path / "inner"
        inner_root.mkdir()

        # Try to access outer file from inner root
        with pytest.raises(PathValidationError) as exc_info:
            validate_file_path_within_root("../outer/secret.txt", inner_root)

        assert "escapes root directory" in exc_info.value.message


class TestPathValidationError:
    """Tests for PathValidationError exception."""

    def test_error_attributes(self, tmp_path: Path) -> None:
        """Test that PathValidationError has correct attributes."""
        path = tmp_path / "test.txt"
        root = tmp_path

        error = PathValidationError("Test error message", path=path, root=root)

        assert error.message == "Test error message"
        assert error.path == path
        assert error.root == root
        assert str(error) == "Test error message"

    def test_error_raised_with_attributes(self, tmp_path: Path) -> None:
        """Test that raised PathValidationError has accessible attributes."""
        with pytest.raises(PathValidationError) as exc_info:
            validate_path_within_root("/etc/passwd", tmp_path)

        error = exc_info.value
        assert error.path == Path("/etc/passwd")
        assert error.root == tmp_path
        assert "escapes root directory" in error.message


class TestRealWorldScenarios:
    """Tests for real-world attack scenarios."""

    def test_decoded_path_traversal_sequences_blocked(self, tmp_path: Path) -> None:
        """Test that decoded path traversal sequences are blocked.

        Note: URL decoding should happen at the web framework layer before
        path validation. This test verifies that decoded traversal paths
        are correctly rejected by the validation function.
        """
        # Decoded path traversal sequence (as would come from URL decoding)
        with pytest.raises(PathValidationError):
            validate_path_within_root("../../etc/passwd", tmp_path)

        # Double-encoded sequences remain literal and create a filename
        # (doesn't escape because %2F is not treated as /)
        # This is expected - URL decoding is the web layer's responsibility

    def test_null_byte_injection(self, tmp_path: Path) -> None:
        """Test that null bytes in paths are handled safely."""
        # In Python, null bytes in paths typically cause errors
        # This test ensures they don't bypass validation
        try:
            validate_path_within_root("file\x00.txt/../etc/passwd", tmp_path)
            pytest.fail("Expected an error for null byte in path")
        except (PathValidationError, ValueError, OSError):
            # Any of these errors is acceptable - the path should not validate
            pass

    def test_windows_style_traversal_on_unix(self, tmp_path: Path) -> None:
        """Test that Windows-style paths don't bypass Unix validation."""
        # On Unix, backslashes are literal characters in filenames
        # This shouldn't bypass validation as it creates a literal filename
        result = validate_path_within_root("..\\..\\etc\\passwd", tmp_path)
        # The path should resolve within tmp_path since backslash is literal
        assert result.is_relative_to(tmp_path.resolve())

    def test_deeply_nested_valid_path(self, tmp_path: Path) -> None:
        """Test that deeply nested but valid paths pass validation."""
        deep_path = tmp_path / "a" / "b" / "c" / "d" / "e" / "f" / "g" / "file.txt"
        deep_path.parent.mkdir(parents=True)
        deep_path.touch()

        result = validate_path_within_root(deep_path, tmp_path)
        assert result == deep_path.resolve()

    def test_symlink_escape_attempt(self, tmp_path: Path) -> None:
        """Test that symlinks pointing outside root are rejected."""
        # Create a directory to be our "root"
        root = tmp_path / "root"
        root.mkdir()

        # Create an external file
        external = tmp_path / "external" / "secret.txt"
        external.parent.mkdir(parents=True)
        external.write_text("secret")

        # Create a symlink inside root pointing outside
        link = root / "escape_link"
        link.symlink_to(external.parent)

        # Attempting to access via symlink should fail because resolve()
        # follows the symlink and the resolved path is outside root
        with pytest.raises(PathValidationError) as exc_info:
            validate_file_path_within_root(link / "secret.txt", root)

        assert "escapes root directory" in exc_info.value.message
