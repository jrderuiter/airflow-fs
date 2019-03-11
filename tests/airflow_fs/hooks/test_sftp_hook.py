from getpass import getuser
import os
import posixpath

import pysftp
import pytest

from airflow_fs.hooks import SftpHook
from airflow_fs.testing import MockConnection


@pytest.fixture
def sftp_conn(mocker):
    conn = MockConnection(
        host="localhost", login="root", extra={"ignore_hostkey_verification": True}
    )
    mocker.patch.object(SftpHook, "get_connection", return_value=conn)
    return conn


@pytest.fixture(scope="session")
def sftp_client():
    """SFTP client to be used while testing."""

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    return pysftp.Connection("localhost", username=getuser(), cnopts=cnopts)


@pytest.fixture
def sftp_mock_dir(mock_data_dir, sftp_client, tmpdir):
    """Creates a mock directory containing the standard test data."""

    sftp_client.put_r(mock_data_dir, str(tmpdir))
    return str(tmpdir)


class TestSftpHook:
    """Tests for the SftpHook class."""

    def test_open_read(self, sftp_conn, sftp_mock_dir):
        """Tests reading of a file using the `open` method."""

        file_path = posixpath.join(sftp_mock_dir, "test.txt")

        with SftpHook("sftp_default") as hook:
            with hook.open(file_path) as file_:
                content = file_.read()

        assert content == b"Test file\n"

    def test_open_write(self, sftp_conn, sftp_client, tmpdir):
        """Tests writing of a file using the `open` method."""

        file_path = posixpath.join(str(tmpdir), "test2.txt")
        assert not sftp_client.exists(file_path)

        with SftpHook("sftp_default") as hook:
            with hook.open(file_path, "wb") as file_:
                file_.write(b"Test file\n")

        assert sftp_client.exists(file_path)

    def test_exists(self, sftp_conn, sftp_mock_dir):
        """Tests the `exists` method."""

        with SftpHook("sftp_default") as hook:
            assert hook.exists(posixpath.join(sftp_mock_dir, "subdir"))
            assert hook.exists(posixpath.join(sftp_mock_dir, "test.txt"))
            assert not hook.exists(posixpath.join(sftp_mock_dir, "non-existing.txt"))

    def test_isdir(self, sftp_conn, sftp_mock_dir):
        """Tests the `isdir` method."""

        with SftpHook("sftp_default") as hook:
            assert hook.isdir(posixpath.join(sftp_mock_dir, "subdir"))
            assert not hook.isdir(posixpath.join(sftp_mock_dir, "test.txt"))

    def test_listdir(self, sftp_conn, sftp_mock_dir, mock_data_dir):
        """Tests the `listdir` method."""

        with SftpHook("sftp_default") as hook:
            assert set(hook.listdir(sftp_mock_dir)) == set(os.listdir(mock_data_dir))

    def test_mkdir(self, sftp_conn, sftp_client, tmpdir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(str(tmpdir), "subdir")
        assert not sftp_client.exists(dir_path)

        with SftpHook("sftp_default") as hook:
            hook.mkdir(dir_path, mode=0o750)

        assert sftp_client.exists(dir_path)
        assert pysftp.st_mode_to_int(sftp_client.stat(dir_path).st_mode) == 750

    def test_mkdir_exists(self, sftp_conn, sftp_client, tmpdir):
        """Tests the `mkdir` method with the exists_ok parameter."""

        dir_path = posixpath.join(str(tmpdir), "subdir")
        assert not sftp_client.exists(dir_path)

        with SftpHook("sftp_default") as hook:
            hook.mkdir(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.mkdir(dir_path, exist_ok=False)

            hook.mkdir(dir_path, exist_ok=True)

    def test_rm(self, sftp_conn, sftp_client, sftp_mock_dir):
        """Tests the `rm` method."""

        file_path = posixpath.join(sftp_mock_dir, "test.txt")
        assert sftp_client.exists(file_path)

        with SftpHook("sftp_default") as hook:
            hook.rm(file_path)

        assert not sftp_client.exists(file_path)

    def test_rmtree(self, sftp_conn, sftp_client, sftp_mock_dir):
        """Tests the `rmtree` method."""

        dir_path = posixpath.join(sftp_mock_dir, "subdir")
        assert sftp_client.exists(dir_path)

        with SftpHook("sftp_default") as hook:
            hook.rmtree(dir_path)

        assert not sftp_client.exists(dir_path)

    def test_makedirs(self, sftp_conn, sftp_client, tmpdir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(str(tmpdir), "some", "nested", "dir")

        with SftpHook("sftp_default") as hook:
            hook.makedirs(dir_path, mode=0o750)

        assert sftp_client.exists(dir_path)
        assert pysftp.st_mode_to_int(sftp_client.stat(dir_path).st_mode) == 750

    def test_makedirs_exists(self, sftp_conn, sftp_client, tmpdir):
        """Tests the `mkdir` method with exists_ok parameter."""

        dir_path = posixpath.join(str(tmpdir), "some", "nested", "dir")

        with SftpHook("sftp_default") as hook:
            hook.makedirs(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.makedirs(dir_path, exist_ok=False)

            hook.makedirs(dir_path, exist_ok=True)

    def test_walk(self, sftp_conn, sftp_mock_dir, mock_data_dir):
        """Tests the `walk` method."""

        with SftpHook("sftp_default") as hook:
            entries = list(hook.walk(sftp_mock_dir))

        pytest.helpers.assert_walk_equal(entries, os.walk(mock_data_dir))
