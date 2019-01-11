"""Tests for the sftp_hook module."""

import pytest

from airflow_fs.hooks.sftp_hook import SftpHook

# pylint: disable=redefined-outer-name,no-self-use


class TestSftpHook:
    """
    Tests for the SftpHook class.

    Note that the get_conn method is mocked in most of these tests
    to avoid the requirement of having a local SFTP host for testing.
    """

    def test_with(self, mocker):
        """Tests if context manager closes the connection."""

        with SftpHook(conn_id="test") as hook:
            mock = mocker.patch.object(hook, "_conn")

        assert mock.close.call_count == 1

    def test_open(self, mocker):
        """Tests for the `open` method."""

        mock = mocker.patch.object(SftpHook, "get_conn")

        with SftpHook(conn_id="test") as hook:
            hook.open("test.txt", mode="rb")

        mock.return_value.open.assert_called_once_with("test.txt", mode="rb")

    def test_exists(self, mocker):
        """Tests for the `exists` method."""

        mock = mocker.patch.object(SftpHook, "get_conn")

        with SftpHook(conn_id="test") as hook:
            hook.exists("test.txt")

        mock.return_value.exists.assert_called_once_with("test.txt")

    def test_makedirs(self, mocker):
        """Tests for the `makedirs` method."""

        mock = mocker.patch.object(SftpHook, "get_conn")
        mock.return_value.exists.return_value = False

        with SftpHook(conn_id="test") as hook:
            hook.makedirs("path/to/dir")

        mock.return_value.makedirs.assert_called_once_with("path/to/dir", mode=0o755)

    def test_makedirs_exists(self, mocker):
        """Tests for the `makedirs` method with existing path."""

        mock = mocker.patch.object(SftpHook, "get_conn")
        mock.return_value.exists.return_value = True

        with SftpHook(conn_id="test") as hook:
            with pytest.raises(ValueError):
                hook.makedirs("path/to/dir", exist_ok=False)

            hook.makedirs("path/to/dir", exist_ok=True)

    def test_rmtree(self, mocker):
        """Tests for the `rmtree` method."""

        mock = mocker.patch.object(SftpHook, "get_conn")
        mock.return_value.execute.return_value = []

        with SftpHook(conn_id="test") as hook:
            hook.rmtree("test_dir")

        mock.return_value.execute.assert_called_once_with("rm -r 'test_dir'")

    def test_rmtree_error(self, mocker):
        """Tests for the `rmtree` method with error."""

        mock = mocker.patch.object(SftpHook, "get_conn")
        mock.return_value.execute.return_value = [b"rm: Failed to..."]

        with SftpHook(conn_id="test") as hook:
            with pytest.raises(OSError):
                hook.rmtree("test_dir")
