import os
import posixpath

import pytest
from pyarrow import hdfs

from airflow_fs.hooks import HdfsHook
from airflow_fs.testing import copy_tree


@pytest.fixture(scope="session")
def client():
    """Hdfs client to be used while testing."""
    return hdfs.connect()


@pytest.fixture
def remote_mock_dir(mock_data_dir, client, remote_temp_dir):
    """A mock remote directory containing standard test data."""

    def _upload(src_path, dest_path):
        with open(src_path, "rb") as file_:
            client.upload(dest_path, file_)

    copy_tree(mock_data_dir, remote_temp_dir, mkdir_func=client.mkdir, cp_func=_upload)

    return str(remote_temp_dir)


@pytest.fixture
def remote_temp_dir(tmpdir):
    """A mock remote temp directory."""
    return str(tmpdir)


class TestHdfsHook:
    """Tests for the HdfsHook class."""

    def test_open_read(self, remote_mock_dir):
        """Tests reading of a file using the `open` method."""

        file_path = posixpath.join(remote_mock_dir, "test.txt")
        with HdfsHook() as hook:
            with hook.open(file_path) as file_:
                content = file_.read()
            assert content == b"Test file\n"

    def test_open_write(self, client, remote_temp_dir):
        """Tests writing of a file using the `open` method."""

        file_path = posixpath.join(remote_temp_dir, "test2.txt")
        assert not client.exists(file_path)

        with HdfsHook() as hook:
            with hook.open(file_path, "wb") as file_:
                file_.write(b"Test file\n")

        assert client.exists(file_path)

    def test_exists(self, remote_mock_dir):
        """Tests the `exists` method."""

        with HdfsHook() as hook:
            assert hook.exists(posixpath.join(remote_mock_dir, "subdir"))
            assert hook.exists(posixpath.join(remote_mock_dir, "test.txt"))
            assert not hook.exists(posixpath.join(remote_mock_dir, "non-existing.txt"))

    def test_isdir(self, remote_mock_dir):
        """Tests the `isdir` method."""

        with HdfsHook() as hook:
            assert hook.isdir(posixpath.join(remote_mock_dir, "subdir"))
            assert not hook.isdir(posixpath.join(remote_mock_dir, "test.txt"))

    def test_listdir(self, remote_mock_dir, mock_data_dir):
        """Tests the `listdir` method."""

        with HdfsHook() as hook:
            assert set(hook.listdir(remote_mock_dir)) == set(os.listdir(mock_data_dir))

    def test_mkdir(self, client, remote_temp_dir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(remote_temp_dir, "subdir")
        assert not client.exists(dir_path)

        with HdfsHook() as hook:
            hook.mkdir(dir_path, mode=0o750)

        assert client.exists(dir_path)
        assert client.info(dir_path)["permissions"] == 0o750

    def test_mkdir_exists(self, client, remote_temp_dir):
        """Tests the `mkdir` method with the exists_ok parameter."""

        dir_path = posixpath.join(remote_temp_dir, "subdir")
        assert not client.exists(dir_path)

        with HdfsHook() as hook:
            hook.mkdir(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.mkdir(dir_path, exist_ok=False)

            hook.mkdir(dir_path, exist_ok=True)

    def test_rm(self, client, remote_mock_dir):
        """Tests the `rm` method."""

        file_path = posixpath.join(remote_mock_dir, "test.txt")
        assert client.exists(file_path)

        with HdfsHook() as hook:
            hook.rm(file_path)

        assert not client.exists(file_path)

    def test_rmtree(self, client, remote_mock_dir):
        """Tests the `rmtree` method."""

        dir_path = posixpath.join(remote_mock_dir, "subdir")
        assert client.exists(dir_path)

        with HdfsHook() as hook:
            hook.rmtree(dir_path)

        assert not client.exists(dir_path)

    def test_makedirs(self, client, remote_temp_dir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(remote_temp_dir, "some", "nested", "dir")

        with HdfsHook() as hook:
            hook.makedirs(dir_path, mode=0o750)

        assert client.exists(dir_path)
        assert client.info(dir_path)["permissions"] == 0o750

    def test_makedirs_exists(self, client, remote_temp_dir):
        """Tests the `mkdir` method with exists_ok parameter."""

        dir_path = posixpath.join(remote_temp_dir, "some", "nested", "dir")

        with HdfsHook() as hook:
            hook.makedirs(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.makedirs(dir_path, exist_ok=False)

            hook.makedirs(dir_path, exist_ok=True)

    def test_walk(self, client, remote_mock_dir, mock_data_dir):
        """Tests the `walk` method."""

        with HdfsHook() as hook:
            entries = list(hook.walk(remote_mock_dir))

        pytest.helpers.assert_walk_equal(entries, os.walk(mock_data_dir))
