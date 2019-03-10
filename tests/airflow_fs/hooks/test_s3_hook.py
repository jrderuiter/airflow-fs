import os
import posixpath

import pytest

from airflow_fs.hooks import S3Hook
from airflow_fs.testing import copy_tree


class TestS3Hook:
    """Tests for the S3Hook class."""

    def test_open_read(self, s3_mock_dir):
        """Tests reading of a file using the `open` method."""

        file_path = posixpath.join(s3_mock_dir, "test.txt")

        with S3Hook() as hook:
            with hook.open(file_path) as file_:
                content = file_.read()

        assert content == b"Test file\n"

    def test_open_write(self, s3_client, s3_temp_dir):
        """Tests writing of a file using the `open` method."""

        file_path = posixpath.join(s3_temp_dir, "test2.txt")
        assert not s3_client.exists(file_path)

        with S3Hook() as hook:
            with hook.open(file_path, "wb") as file_:
                file_.write(b"Test file\n")

        assert s3_client.exists(file_path)

    def test_exists(self, s3_mock_dir):
        """Tests the `exists` method."""

        with S3Hook() as hook:
            assert hook.exists(posixpath.join(s3_mock_dir, "subdir"))
            assert hook.exists(posixpath.join(s3_mock_dir, "test.txt"))
            assert not hook.exists(posixpath.join(s3_mock_dir, "non-existing.txt"))

    def test_isdir(self, s3_mock_dir):
        """Tests the `isdir` method."""

        with S3Hook() as hook:
            assert hook.isdir(posixpath.join(s3_mock_dir, "subdir"))
            assert not hook.isdir(posixpath.join(s3_mock_dir, "test.txt"))

    def test_listdir(self, s3_mock_dir, mock_data_dir):
        """Tests the `listdir` method."""

        with S3Hook() as hook:
            assert set(hook.listdir(s3_mock_dir)) == set(os.listdir(mock_data_dir))

    def test_mkdir(self, s3_client, s3_temp_dir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(s3_temp_dir, "subdir")
        assert not s3_client.exists(dir_path)

        with S3Hook() as hook:
            hook.mkdir(dir_path, mode=0o750)

        assert s3_client.exists(dir_path)

    def test_mkdir_exists(self, s3_client, s3_temp_dir):
        """Tests the `mkdir` method with the exists_ok parameter."""

        dir_path = posixpath.join(s3_temp_dir, "subdir")
        assert not s3_client.exists(dir_path)

        with S3Hook() as hook:
            hook.mkdir(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.mkdir(dir_path, exist_ok=False)

            hook.mkdir(dir_path, exist_ok=True)

    def test_rm(self, s3_client, s3_mock_dir):
        """Tests the `rm` method."""

        file_path = posixpath.join(s3_mock_dir, "test.txt")
        assert s3_client.exists(file_path)

        with S3Hook() as hook:
            hook.rm(file_path)

        assert not s3_client.exists(file_path)

    def test_rmtree(self, s3_client, s3_mock_dir):
        """Tests the `rmtree` method."""

        dir_path = posixpath.join(s3_mock_dir, "subdir")
        assert s3_client.exists(dir_path)

        with S3Hook() as hook:
            hook.rmtree(dir_path)

        s3_client.invalidate_cache(dir_path)
        assert not s3_client.exists(dir_path)

    def test_makedirs(self, s3_client, s3_temp_dir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(s3_temp_dir, "some", "nested", "dir")

        with S3Hook() as hook:
            hook.makedirs(dir_path, mode=0o750)

        assert s3_client.exists(dir_path)

    def test_makedirs_exists(self, s3_client, s3_temp_dir):
        """Tests the `mkdir` method with exists_ok parameter."""

        dir_path = posixpath.join(s3_temp_dir, "some", "nested", "dir")

        with S3Hook() as hook:
            hook.makedirs(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.makedirs(dir_path, exist_ok=False)

            s3_client.invalidate_cache(dir_path)
            hook.makedirs(dir_path, exist_ok=True)

    def test_walk(self, s3_client, s3_mock_dir, mock_data_dir):
        """Tests the `walk` method."""

        with S3Hook() as hook:
            entries = list(hook.walk(s3_mock_dir))

        pytest.helpers.assert_walk_equal(entries, os.walk(mock_data_dir))
