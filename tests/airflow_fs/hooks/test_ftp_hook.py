"""Tests for the dataflows.hooks.fs.ftp_hook module."""

import pytest

from airflow_fs.hooks.ftp_hook import FtpHook

# pylint: disable=redefined-outer-name,no-self-use


class TestFtpHook:
    """
    Tests for the FtpHook class.

    Note that the get_conn method is mocked in most of these tests
    to avoid the requirement of having a local FTP instance for testing.
    """

    def test_with(self, mocker):
        """Tests if context manager closes the connection."""

        with FtpHook(conn_id="test") as hook:
            mock = mocker.patch.object(hook, "_conn")

        assert mock.close.call_count == 1

    def test_open(self, mocker):
        """Tests for the `open` method."""

        mock = mocker.patch.object(FtpHook, "get_conn")

        with FtpHook(conn_id="test") as hook:
            hook.open("test.txt", mode="rb")

        mock.return_value.open.assert_called_once_with("test.txt", mode="rb")

    def test_exists(self, mocker):
        """Tests for the `exists` method."""

        mock = mocker.patch.object(FtpHook, "get_conn")

        with FtpHook(conn_id="test") as hook:
            hook.exists("test.txt")

        mock.return_value.path.exists.assert_called_once_with("test.txt")

    def test_makedirs(self, mocker):
        """Tests for the `makedirs` method."""

        mock = mocker.patch.object(FtpHook, "get_conn")
        mock.return_value.exists.return_value = False

        with FtpHook(conn_id="test") as hook:
            hook.makedirs("path/to/dir")

        mock.return_value.makedirs.assert_called_once_with("path/to/dir")

    def test_makedirs_exists(self, mocker):
        """Tests for the `makedirs` method with existing path."""

        mock = mocker.patch.object(FtpHook, "get_conn")
        mock.return_value.exists.return_value = True

        with FtpHook(conn_id="test") as hook:
            with pytest.raises(ValueError):
                hook.makedirs("path/to/dir", exist_ok=False)

            hook.makedirs("path/to/dir", exist_ok=True)

    def test_rmtree(self, mocker):
        """Tests for the `rmtree` method."""

        mock = mocker.patch.object(FtpHook, "get_conn")

        with FtpHook(conn_id="test") as hook:
            hook.rmtree("test_dir")

        mock.return_value.rmtree.assert_called_once_with(
            "test_dir", ignore_errors=False
        )
