import glob
import os
import posixpath
import sys

import pytest

from airflow_fs.hooks import LocalHook


class TestLocalHook:
    """Tests for the LocalHook class."""

    def test_open_read(self, local_mock_dir):
        """Tests reading of a file using the `open` method."""

        file_path = posixpath.join(local_mock_dir, "test.txt")

        with LocalHook() as hook:
            with hook.open(file_path) as file_:
                content = file_.read()

        assert content == b"Test file\n"

    def test_open_write(self, tmpdir):
        """Tests writing of a file using the `open` method."""

        file_path = posixpath.join(str(tmpdir), "test2.txt")
        assert not posixpath.exists(file_path)

        with LocalHook() as hook:
            with hook.open(file_path, "wb") as file_:
                file_.write(b"Test file\n")

        assert posixpath.exists(file_path)

    def test_exists(self, local_mock_dir):
        """Tests the `exists` method."""

        with LocalHook() as hook:
            assert hook.exists(posixpath.join(local_mock_dir, "subdir"))
            assert hook.exists(posixpath.join(local_mock_dir, "test.txt"))
            assert not hook.exists(posixpath.join(local_mock_dir, "non-existing.txt"))

    def test_isdir(self, local_mock_dir):
        """Tests the `isdir` method."""

        with LocalHook() as hook:
            assert hook.isdir(posixpath.join(local_mock_dir, "subdir"))
            assert not hook.isdir(posixpath.join(local_mock_dir, "test.txt"))

    def test_listdir(self, local_mock_dir, mock_data_dir):
        """Tests the `listdir` method."""

        with LocalHook() as hook:
            assert set(hook.listdir(local_mock_dir)) == set(os.listdir(mock_data_dir))

    def test_mkdir(self, tmpdir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(str(tmpdir), "subdir")
        assert not posixpath.exists(dir_path)

        with LocalHook() as hook:
            hook.mkdir(dir_path, mode=0o750)

        assert posixpath.exists(dir_path)
        assert oct(os.stat(dir_path).st_mode)[-3:] == "750"

    def test_mkdir_exists(self, tmpdir):
        """Tests the `mkdir` method with the exists_ok parameter."""

        dir_path = posixpath.join(str(tmpdir), "subdir")
        assert not posixpath.exists(dir_path)

        with LocalHook() as hook:
            hook.mkdir(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.mkdir(dir_path, exist_ok=False)

            hook.mkdir(dir_path, exist_ok=True)

    def test_rm(self, local_mock_dir):
        """Tests the `rm` method."""

        file_path = posixpath.join(local_mock_dir, "test.txt")
        assert posixpath.exists(file_path)

        with LocalHook() as hook:
            hook.rm(file_path)

        assert not posixpath.exists(file_path)

    def test_rmtree(self, local_mock_dir):
        """Tests the `rmtree` method."""

        dir_path = posixpath.join(local_mock_dir, "subdir")
        assert posixpath.exists(dir_path)

        with LocalHook() as hook:
            hook.rmtree(dir_path)

        assert not posixpath.exists(dir_path)

    def test_makedirs(self, tmpdir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(str(tmpdir), "some", "nested", "dir")

        with LocalHook() as hook:
            hook.makedirs(dir_path, mode=0o750)

        assert posixpath.exists(dir_path)
        assert oct(os.stat(dir_path).st_mode)[-3:] == "750"

    def test_makedirs_exists(self, tmpdir):
        """Tests the `mkdir` method with exists_ok parameter."""

        dir_path = posixpath.join(str(tmpdir), "some", "nested", "dir")

        with LocalHook() as hook:
            hook.makedirs(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.makedirs(dir_path, exist_ok=False)

            hook.makedirs(dir_path, exist_ok=True)

    def test_walk(self, local_mock_dir, mock_data_dir):
        """Tests the `walk` method."""

        with LocalHook() as hook:
            entries = list(hook.walk(local_mock_dir))

        pytest.helpers.assert_walk_equal(entries, os.walk(mock_data_dir))

    def test_glob(self, local_mock_dir, mock_data_dir):
        """Tests the `glob` method."""

        with LocalHook() as hook:
            # Test simple glob on txt files.
            txt_files = hook.glob(posixpath.join(local_mock_dir, "*.txt"))
            txt_expected = glob.glob(os.path.join(mock_data_dir, "*.txt"))
            assert_paths_equal(
                txt_files, txt_expected, root_a=local_mock_dir, root_b=mock_data_dir
            )

            # Test glob for non-existing tsv files.
            assert not set(hook.glob(posixpath.join(local_mock_dir, "*.tsv")))

            # Test glob on directory.
            dir_paths = hook.glob(posixpath.join(local_mock_dir, "subdir"))
            assert set(strip_root(p, root=local_mock_dir) for p in dir_paths) == {
                "subdir"
            }

            # Test glob with dir pattern.
            file_paths = hook.glob(posixpath.join(local_mock_dir, "*", "*.txt"))
            expected_paths = glob.glob(os.path.join(mock_data_dir, "*", "*.txt"))
            assert_paths_equal(
                file_paths, expected_paths, root_a=local_mock_dir, root_b=mock_data_dir
            )

    @pytest.mark.skipif(
        sys.version_info < (3, 5), reason="recursive glob requires Python 3.5+"
    )
    def test_glob_recursive(self, local_mock_dir, mock_data_dir):
        """Tests the `glob` method with recursive = True."""

        with LocalHook() as hook:
            file_paths = hook.glob(
                posixpath.join(local_mock_dir, "**", "*.txt"), recursive=True
            )
            expected_paths = set(
                glob.glob(os.path.join(mock_data_dir, "**", "*.txt"), recursive=True)
            )
            assert_paths_equal(
                file_paths, expected_paths, root_a=local_mock_dir, root_b=mock_data_dir
            )


def assert_paths_equal(paths_a, paths_b, root_a, root_b):
    """Helper that asserts if two sets of paths are equal after
       removing the given root directories.
    """
    paths_a = set(strip_root(p, root_a) for p in paths_a)
    paths_b = set(strip_root(p, root_b) for p in paths_b)
    assert paths_a == paths_b


def strip_root(path_, root):
    """Strips root path from given file path."""

    if root[-1] != "/":
        root = root + "/"

    if path_.startswith(root):
        return path_[len(root) :]

    return path_
