"""Tests for the dataflows.hooks.fs.hdfs3_hook module."""

from airflow_fs.hooks.hdfs_hook import HdfsHook, hdfs3

# pylint: disable=redefined-outer-name,no-self-use


class TestHdfs3Hook:
    """
    Tests for the Hdfs3Hook class.

    Note that the HDFileSystem class is mocked in most of these tests
    to avoid the requirement of having a local HDFS instance for testing.
    """

    def test_with(self, mocker):
        """Tests if context manager closes the connection."""

        with HdfsHook() as hook:
            mock = mocker.patch.object(hook, "_conn")

        assert mock.disconnect.call_count == 1

    def test_open(self, mocker):
        """Tests for the `open` method."""

        mock = mocker.patch.object(hdfs3, "HDFileSystem")

        with HdfsHook() as hook:
            hook.open("test.txt", mode="rb")

        mock.return_value.open.assert_called_once_with("test.txt", mode="rb")

    def test_exists(self, mocker):
        """Tests for the `exists` method."""

        mock = mocker.patch.object(hdfs3, "HDFileSystem")

        with HdfsHook() as hook:
            hook.exists("test.txt")

        mock.return_value.exists.assert_called_once_with("test.txt")

    def test_makedirs(self, mocker):
        """Tests for the `makedirs` method."""

        mock = mocker.patch.object(hdfs3, "HDFileSystem")
        mock.return_value.exists.return_value = False

        with HdfsHook() as hook:
            hook.makedirs("path/to/dir")

        mkdirs = mock.return_value.makedirs
        assert mkdirs.call_count == 1
        assert mkdirs.call_args_list[0][0][0] == "path/to/dir"

    def test_glob(self, mocker):
        """Tests for the `glob` method."""

        mock = mocker.patch.object(hdfs3, "HDFileSystem")

        with HdfsHook() as hook:
            hook.glob("*.txt")

        mock.return_value.glob.assert_called_once_with("*.txt")

    def test_rmtree(self, mocker):
        """Tests for the `rmtree` method."""

        mock = mocker.patch.object(hdfs3, "HDFileSystem")

        with HdfsHook() as hook:
            hook.rmtree("test_dir")

        mock.return_value.rm.assert_called_once_with("test_dir", recursive=True)
