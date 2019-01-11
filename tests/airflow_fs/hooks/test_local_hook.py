"""Tests for the dataflows.hooks.fs.local_hook module."""

import io
import os
from os import path

import pytest

from airflow_fs.hooks.local_hook import LocalHook

# pylint: disable=redefined-outer-name,no-self-use


@pytest.fixture
def test_dir(tmpdir):
    """Fixture of a test directory, containing various test files."""

    # Creates following directory structure:
    # - files
    #   - hello.txt
    #   - nested
    #     - nested.txt
    #   - nested_empty

    # Create base dir.
    base_dir = path.join(str(tmpdir), "files")
    os.makedirs(base_dir)

    with open(path.join(base_dir, "hello.txt"), "wb") as file_:
        file_.write(b"Hello world!\n")

    # Create nested dir.
    nested_dir = path.join(base_dir, "nested")
    os.makedirs(nested_dir)

    with open(path.join(nested_dir, "nested.txt"), "wb") as file_:
        file_.write(b"Nested!\n")

    # Create empty dir.
    os.makedirs(path.join(base_dir, "nested_empty"))

    return str(tmpdir)


class TestLocalHook:
    """Tests for LocalHook."""

    def test_with_disconnect(self, mocker):
        """Tests if context manager disconnects."""

        mock_disconnect = mocker.patch.object(LocalHook, "disconnect")

        with LocalHook() as _:
            pass

        assert mock_disconnect.call_count == 1

    def test_open(self, test_dir):
        """Tests open function."""

        with LocalHook() as hook:
            # Test read.
            read_path = path.join(test_dir, "files", "hello.txt")
            with hook.open(read_path, "rb") as file_:
                assert file_.read() == b"Hello world!\n"

            # Test write.
            write_path = path.join(test_dir, "new.txt")
            with hook.open(write_path, "wb") as file_:
                file_.write(b"New!\n")

            assert path.exists(write_path)

    def test_exists(self, test_dir):
        """Tests exists method."""

        with LocalHook() as hook:
            assert not hook.exists(path.join(test_dir, "test.txt"))
            assert hook.exists(path.join(test_dir, "files", "hello.txt"))

    def test_makedirs(self, tmpdir):
        """Tests creation of nested directory with makedirs."""

        with LocalHook() as hook:
            # Tests creation of non-existing directory.
            dir_path = path.join(str(tmpdir), "test", "test2")
            hook.makedirs(dir_path)

            assert path.exists(dir_path)

            # Tests error on existing.
            with pytest.raises(OSError):
                hook.makedirs(dir_path, exist_ok=False)

            # Tests with exist_ok = True.
            hook.makedirs(dir_path, exist_ok=True)

    def test_rmtree(self, test_dir):
        """Tests rmtree method."""

        dir_path = path.join(test_dir, "files")
        assert path.exists(dir_path)

        # Delete directory.
        with LocalHook() as hook:
            hook.rmtree(dir_path)

        assert not path.exists(dir_path)

    def test_copy_file(self, test_dir):
        """Tests copy_file method."""

        src_path = path.join(test_dir, "files", "hello.txt")
        dest_path = path.join(test_dir, "hello2.txt")

        with LocalHook() as hook:
            hook.copy_file(src_path, dest_path)

        # Check copied file contents.
        with open(dest_path, "rb") as file_:
            assert file_.read() == b"Hello world!\n"

    def test_copy_file_obj(self, tmpdir):
        """Tests copy_file_obj method."""

        # Copy buffer to dest.
        buffer = io.BytesIO(b"Hello world!\n")
        dest_path = path.join(str(tmpdir), "test2.txt")

        with LocalHook() as hook:
            hook.copy_fileobj(buffer, dest_path)

        # Check copied file contents.
        with open(dest_path, "rb") as file_:
            assert file_.read() == b"Hello world!\n"

    def test_copy_dir(self, test_dir):
        """Tests copy_dir method."""

        # Copy directory.
        src_dir = path.join(test_dir, "files")
        dest_dir = path.join(test_dir, "files2")

        with LocalHook() as hook:
            hook.copy_dir(src_dir, dest_dir)

        # Check copied files.
        assert path.exists(path.join(dest_dir, "nested"))
        assert path.exists(path.join(dest_dir, "nested_empty"))
        assert path.exists(path.join(dest_dir, "nested", "nested.txt"))
        assert path.exists(path.join(dest_dir, "hello.txt"))
