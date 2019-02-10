import os
import posixpath

import boto3
from moto import mock_s3
import pytest
import s3fs

from airflow_fs.hooks import S3Hook
from airflow_fs.testing import copy_tree


@pytest.fixture(scope="session")
def client():
    """S3 client to be used while testing."""

    os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "bar"

    return s3fs.S3FileSystem()


@pytest.fixture
def remote_mock_dir(client, mock_data_dir, remote_temp_dir):
    """A mock remote directory containing standard test data."""

    copy_tree(
        mock_data_dir, remote_temp_dir, mkdir_func=lambda x: x, cp_func=client.put
    )

    return remote_temp_dir


@pytest.fixture
def remote_temp_dir(request, tmpdir):
    """A mock remote temp directory."""

    mock = mock_s3()
    mock.start()
    request.addfinalizer(lambda: mock.stop())

    conn = boto3.resource("s3")
    conn.create_bucket(Bucket="test_bucket")

    return "test_bucket" + str(tmpdir)


class TestS3Hook:
    """Tests for the S3Hook class."""

    def test_open_read(self, remote_mock_dir):
        """Tests reading of a file using the `open` method."""

        file_path = posixpath.join(remote_mock_dir, "test.txt")

        with S3Hook() as hook:
            with hook.open(file_path) as file_:
                content = file_.read()

        assert content == b"Test file\n"

    def test_open_write(self, client, remote_temp_dir):
        """Tests writing of a file using the `open` method."""

        file_path = posixpath.join(remote_temp_dir, "test2.txt")
        assert not client.exists(file_path)

        with S3Hook() as hook:
            with hook.open(file_path, "wb") as file_:
                file_.write(b"Test file\n")

        assert client.exists(file_path)

    def test_exists(self, remote_mock_dir):
        """Tests the `exists` method."""

        with S3Hook() as hook:
            assert hook.exists(posixpath.join(remote_mock_dir, "subdir"))
            assert hook.exists(posixpath.join(remote_mock_dir, "test.txt"))
            assert not hook.exists(posixpath.join(remote_mock_dir, "non-existing.txt"))

    def test_isdir(self, remote_mock_dir):
        """Tests the `isdir` method."""

        with S3Hook() as hook:
            assert hook.isdir(posixpath.join(remote_mock_dir, "subdir"))
            assert not hook.isdir(posixpath.join(remote_mock_dir, "test.txt"))

    def test_listdir(self, remote_mock_dir):
        """Tests the `listdir` method."""

        with S3Hook() as hook:
            assert set(hook.listdir(remote_mock_dir)) == {"test.txt", "subdir"}

    def test_mkdir(self, client, remote_temp_dir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(remote_temp_dir, "subdir")
        assert not client.exists(dir_path)

        with S3Hook() as hook:
            hook.mkdir(dir_path, mode=0o750)

        assert client.exists(dir_path)

    def test_mkdir_exists(self, client, remote_temp_dir):
        """Tests the `mkdir` method with the exists_ok parameter."""

        dir_path = posixpath.join(remote_temp_dir, "subdir")
        assert not client.exists(dir_path)

        with S3Hook() as hook:
            hook.mkdir(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.mkdir(dir_path, exist_ok=False)

            hook.mkdir(dir_path, exist_ok=True)

    def test_rm(self, client, remote_mock_dir):
        """Tests the `rm` method."""

        file_path = posixpath.join(remote_mock_dir, "test.txt")
        assert client.exists(file_path)

        with S3Hook() as hook:
            hook.rm(file_path)

        assert not client.exists(file_path)

    def test_rmtree(self, client, remote_mock_dir):
        """Tests the `rmtree` method."""

        dir_path = posixpath.join(remote_mock_dir, "subdir")
        assert client.exists(dir_path)

        with S3Hook() as hook:
            hook.rmtree(dir_path)

        client.invalidate_cache(dir_path)
        assert not client.exists(dir_path)

    def test_makedirs(self, client, remote_temp_dir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(remote_temp_dir, "some", "nested", "dir")

        with S3Hook() as hook:
            hook.makedirs(dir_path, mode=0o750)

        assert client.exists(dir_path)

    def test_makedirs_exists(self, client, remote_temp_dir):
        """Tests the `mkdir` method with exists_ok parameter."""

        dir_path = posixpath.join(remote_temp_dir, "some", "nested", "dir")

        with S3Hook() as hook:
            hook.makedirs(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.makedirs(dir_path, exist_ok=False)

            client.invalidate_cache(dir_path)
            hook.makedirs(dir_path, exist_ok=True)

    def test_walk(self, client, remote_mock_dir):
        """Tests the `walk` method."""

        with S3Hook() as hook:
            entries = list(hook.walk(remote_mock_dir))

        assert entries[0] == (remote_mock_dir, ["subdir"], ["test.txt"])
        assert entries[1] == (
            posixpath.join(remote_mock_dir, "subdir"),
            [],
            ["nested.txt"],
        )
