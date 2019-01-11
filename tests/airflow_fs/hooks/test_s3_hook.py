"""Tests for the dataflows.hooks.fs.s3fs_hook module."""

import io

import boto3
from moto import mock_s3
import pytest

from airflow_fs.hooks.s3_hook import S3Hook

# pylint: disable=redefined-outer-name,no-self-use


@pytest.fixture
def mock_bucket():
    """Mocked bucket using moto."""

    mock_s3().start()
    conn = boto3.resource("s3")
    bucket = conn.create_bucket(Bucket="test_bucket")

    yield bucket

    mock_s3().stop()


@pytest.fixture
def test_bucket(mock_bucket):
    """Bootstraps the bucket with some test files."""

    buffer = io.BytesIO(b"Hello world!\n")
    mock_bucket.upload_fileobj(buffer, "hello.txt")

    buffer = io.BytesIO(b"Hello world!\n")
    mock_bucket.upload_fileobj(buffer, "hello.csv")

    buffer = io.BytesIO(b"Nested\n")
    mock_bucket.upload_fileobj(buffer, "test/nested.txt")

    return mock_bucket


def s3_exists(bucket, key):
    """Checks whether key exists in bucket."""
    bucket_keys = {obj.key for obj in bucket.objects.all()}
    return key in bucket_keys


class TestS3Hook:
    """Tests for the dataflows S3Hook."""

    def test_with(self, mocker):
        """Tests if context manager closes the connection."""

        mock = mocker.patch.object(S3Hook, "disconnect")

        with S3Hook() as _:
            pass

        assert mock.call_count == 1

    def test_open(self, test_bucket):
        """Tests the open method."""

        with S3Hook() as hook:
            url = "s3://{}/new.txt".format(test_bucket.name)

            # Try to write file.
            with hook.open(url, "wb") as file_:
                file_.write(b"Hello world!")

            # Check file exists.
            assert s3_exists(test_bucket, "new.txt")

            # Check reading file.
            with hook.open(url, "rb") as file_:
                assert file_.read() == b"Hello world!"

    def test_exists(self, test_bucket):
        """Tests the exists method."""

        with S3Hook() as hook:
            assert hook.exists("s3://{}/hello.txt".format(test_bucket.name))
            assert not hook.exists("s3://{}/random.txt".format(test_bucket.name))

    def test_makedirs(self, test_bucket):
        """Tests the makedirs method (effectively a no-op)."""

        with S3Hook() as hook:
            hook.makedirs("s3://{}/test/nested".format(test_bucket.name))

    def test_glob(self, test_bucket):
        """Tests glob method."""

        with S3Hook() as hook:
            assert hook.glob("s3://{}/*.txt".format(test_bucket.name)) == [
                "{}/hello.txt".format(test_bucket.name)
            ]

            assert hook.glob("s3://{}/**/*.txt".format(test_bucket.name)) == [
                "{}/test/nested.txt".format(test_bucket.name)
            ]

            assert hook.glob("s3://{}/*.xml".format(test_bucket.name)) == []

    def test_rmtree(self, test_bucket):
        """Tests the rmtree method."""

        with S3Hook() as hook:
            assert s3_exists(test_bucket, "test/nested.txt")

            hook.rmtree("s3://{}/test".format(test_bucket.name))
            assert not s3_exists(test_bucket, "test/nested.txt")
