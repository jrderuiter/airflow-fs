from distutils.dir_util import copy_tree
import os
import posixpath

import pytest

from airflow_fs.hooks import LocalHook


@pytest.fixture
def test_dir(mock_data_dir, tmpdir):
    """Creates a temp directory containing the standard test data."""
    copy_tree(mock_data_dir, str(tmpdir))
    return str(tmpdir)


class TestLocalHook:
    """Tests for the LocalHook class."""

    def test_open_read(self, test_dir):
        """Tests reading of a file using the `open` method."""

        file_path = posixpath.join(test_dir, "test.txt")

        with LocalHook() as hook:
            with hook.open(file_path) as file_:
                content = file_.read()

        assert content == b"Test file\n"

    def test_open_write(self, tmpdir):
        """Tests writing of a file using the `open` method."""

        file_path = posixpath.join(tmpdir, "test2.txt")
        assert not posixpath.exists(file_path)

        with LocalHook() as hook:
            with hook.open(file_path, "wb") as file_:
                file_.write(b"Test file\n")

        assert posixpath.exists(file_path)

    def test_exists(self, test_dir):
        """Tests the `exists` method."""

        with LocalHook() as hook:
            assert hook.exists(posixpath.join(test_dir, "subdir"))
            assert hook.exists(posixpath.join(test_dir, "test.txt"))
            assert not hook.exists(posixpath.join(test_dir, "non-existing.txt"))

    def test_isdir(self, test_dir):
        """Tests the `isdir` method."""

        with LocalHook() as hook:
            assert hook.isdir(posixpath.join(test_dir, "subdir"))
            assert not hook.isdir(posixpath.join(test_dir, "test.txt"))

    def test_listdir(self, test_dir):
        """Tests the `listdir` method."""

        with LocalHook() as hook:
            assert set(hook.listdir(test_dir)) == {"test.txt", "subdir"}

    def test_mkdir(self, tmpdir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(tmpdir, "subdir")
        assert not posixpath.exists(dir_path)

        with LocalHook() as hook:
            hook.mkdir(dir_path, mode=0o750)

        assert posixpath.exists(dir_path)
        assert oct(os.stat(dir_path).st_mode)[-3:] == "750"

    def test_mkdir_exists(self, tmpdir):
        """Tests the `mkdir` method with the exists_ok parameter."""

        dir_path = posixpath.join(tmpdir, "subdir")
        assert not posixpath.exists(dir_path)

        with LocalHook() as hook:
            hook.mkdir(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.mkdir(dir_path, exist_ok=False)

            hook.mkdir(dir_path, exist_ok=True)

    def test_rm(self, test_dir):
        """Tests the `rm` method."""

        file_path = posixpath.join(test_dir, "test.txt")
        assert posixpath.exists(file_path)

        with LocalHook() as hook:
            hook.rm(file_path)

        assert not posixpath.exists(file_path)

    def test_rmtree(self, test_dir):
        """Tests the `rmtree` method."""

        dir_path = posixpath.join(test_dir, "subdir")
        assert posixpath.exists(dir_path)

        with LocalHook() as hook:
            hook.rmtree(dir_path)

        assert not posixpath.exists(dir_path)

    def test_makedirs(self, tmpdir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(tmpdir, "some", "nested", "dir")

        with LocalHook() as hook:
            hook.makedirs(dir_path, mode=0o750)

        assert posixpath.exists(dir_path)
        assert oct(os.stat(dir_path).st_mode)[-3:] == "750"

    def test_makedirs_exists(self, tmpdir):
        """Tests the `mkdir` method with exists_ok parameter."""

        dir_path = posixpath.join(tmpdir, "some", "nested", "dir")

        with LocalHook() as hook:
            hook.makedirs(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.makedirs(dir_path, exist_ok=False)

            hook.makedirs(dir_path, exist_ok=True)

    def test_walk(self, test_dir):
        """Tests the `walk` method."""

        with LocalHook() as hook:
            entries = list(hook.walk(test_dir))

        assert entries[0] == (test_dir, ["subdir"], ["test.txt"])
        assert entries[1] == (posixpath.join(test_dir, "subdir"), [], ["nested.txt"])
