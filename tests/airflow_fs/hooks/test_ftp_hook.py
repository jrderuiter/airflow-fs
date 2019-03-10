import os
import posixpath

import ftputil
import pytest

from airflow_fs.hooks import FtpHook
from airflow_fs.testing import MockConnection, copy_tree


@pytest.fixture
def ftp_conn(mocker):
    conn = MockConnection(host="localhost", login="ftp-user", password="default")
    mocker.patch.object(FtpHook, "get_connection", return_value=conn)
    return conn


@pytest.fixture(scope="session")
def ftp_client():
    """FTP client to be used while testing."""
    return ftputil.FTPHost("localhost", "ftp-user", "default")


@pytest.fixture
def ftp_tmpdir(ftp_client, tmpdir):
    dir_path = posixpath.join("/ftp", str(tmpdir)[1:])
    ftp_client.makedirs(dir_path)
    yield dir_path
    ftp_client.rmtree(dir_path)


@pytest.fixture
def ftp_mock_dir(mock_data_dir, ftp_client, ftp_tmpdir):
    """Creates a mock directory containing the standard test data."""

    copy_tree(
        mock_data_dir,
        ftp_tmpdir,
        mkdir_func=ftp_client.mkdir,
        cp_func=ftp_client.upload,
    )
    return ftp_tmpdir


class TestFtpHook:
    """Tests for the FtpHook class."""

    def test_open_read(self, ftp_conn, ftp_mock_dir):
        """Tests reading of a file using the `open` method."""

        file_path = posixpath.join(ftp_mock_dir, "test.txt")

        with FtpHook("ftp_default") as hook:
            with hook.open(file_path) as file_:
                content = file_.read()

        assert content == b"Test file\n"

    def test_open_write(self, ftp_conn, ftp_client, ftp_tmpdir):
        """Tests writing of a file using the `open` method."""

        file_path = posixpath.join(ftp_tmpdir, "test2.txt")
        assert not ftp_client.path.exists(file_path)

        with FtpHook("ftp_default") as hook:
            with hook.open(file_path, "wb") as file_:
                file_.write(b"Test file\n")

        assert ftp_client.path.exists(file_path)

    def test_exists(self, ftp_conn, ftp_mock_dir):
        """Tests the `exists` method."""

        with FtpHook("ftp_default") as hook:
            assert hook.exists(posixpath.join(ftp_mock_dir, "subdir"))
            assert hook.exists(posixpath.join(ftp_mock_dir, "test.txt"))
            assert not hook.exists(posixpath.join(ftp_mock_dir, "non-existing.txt"))

    def test_isdir(self, ftp_conn, ftp_mock_dir):
        """Tests the `isdir` method."""

        with FtpHook("ftp_default") as hook:
            assert hook.isdir(posixpath.join(ftp_mock_dir, "subdir"))
            assert not hook.isdir(posixpath.join(ftp_mock_dir, "test.txt"))

    def test_listdir(self, ftp_conn, ftp_mock_dir, mock_data_dir):
        """Tests the `listdir` method."""

        with FtpHook("ftp_default") as hook:
            assert set(hook.listdir(ftp_mock_dir)) == set(os.listdir(mock_data_dir))

    def test_mkdir(self, ftp_conn, ftp_client, ftp_tmpdir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(ftp_tmpdir, "subdir")
        assert not ftp_client.path.exists(dir_path)

        with FtpHook("ftp_default") as hook:
            hook.mkdir(dir_path, mode=0o750)

        assert ftp_client.path.exists(dir_path)
        assert oct(ftp_client.stat(dir_path).st_mode)[-3:] == "750"

    def test_mkdir_exists(self, ftp_conn, ftp_client, ftp_tmpdir):
        """Tests the `mkdir` method with the exists_ok parameter."""

        dir_path = posixpath.join(ftp_tmpdir, "subdir")
        assert not ftp_client.path.exists(dir_path)

        with FtpHook("ftp_default") as hook:
            hook.mkdir(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.mkdir(dir_path, exist_ok=False)

            hook.mkdir(dir_path, exist_ok=True)

    def test_rm(self, ftp_conn, ftp_client, ftp_mock_dir):
        """Tests the `rm` method."""

        file_path = posixpath.join(ftp_mock_dir, "test.txt")
        assert ftp_client.path.exists(file_path)

        with FtpHook("ftp_default") as hook:
            hook.rm(file_path)

        # Invalidate cache to get current state.
        ftp_client.stat_cache.invalidate(file_path)
        assert not ftp_client.path.exists(file_path)

    def test_rmtree(self, ftp_conn, ftp_client, ftp_mock_dir):
        """Tests the `rmtree` method."""

        dir_path = posixpath.join(ftp_mock_dir, "subdir")
        assert ftp_client.path.exists(dir_path)

        with FtpHook("ftp_default") as hook:
            hook.rmtree(dir_path)

        # Invalidate cache to get current state.
        ftp_client.stat_cache.invalidate(dir_path)
        assert not ftp_client.path.exists(dir_path)

    def test_makedirs(self, ftp_conn, ftp_client, ftp_tmpdir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(ftp_tmpdir, "some", "nested", "dir")

        with FtpHook("ftp_default") as hook:
            hook.makedirs(dir_path, mode=0o750)

        assert ftp_client.path.exists(dir_path)
        assert oct(ftp_client.stat(dir_path).st_mode)[-3:] == "750"

    def test_makedirs_exists(self, ftp_conn, ftp_client, ftp_tmpdir):
        """Tests the `mkdir` method with exists_ok parameter."""

        dir_path = posixpath.join(ftp_tmpdir, "some", "nested", "dir")

        with FtpHook("ftp_default") as hook:
            hook.makedirs(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.makedirs(dir_path, exist_ok=False)

            hook.makedirs(dir_path, exist_ok=True)

    def test_walk(self, ftp_conn, ftp_mock_dir, mock_data_dir):
        """Tests the `walk` method."""

        with FtpHook("ftp_default") as hook:
            entries = list(hook.walk(ftp_mock_dir))

        pytest.helpers.assert_walk_equal(entries, os.walk(mock_data_dir))
