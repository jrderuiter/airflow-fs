import os
import posixpath

import boto3
from moto import mock_s3
import pytest
import s3fs

from airflow_fs.testing import copy_tree


@pytest.fixture(scope="session")
def mock_data_dir():
    """Directory containing test data files."""
    return os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture
def local_mock_dir(mock_data_dir, tmpdir):
    """Creates a local mock directory containing the standard test data."""
    copy_tree(mock_data_dir, str(tmpdir))
    return str(tmpdir)


@pytest.fixture(scope="session")
def s3_client():
    """S3 client to be used while testing."""

    os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "bar"

    return s3fs.S3FileSystem()


@pytest.fixture
def s3_temp_dir(tmpdir):
    """A mock remote temp directory."""

    mock = mock_s3()
    mock.start()

    conn = boto3.resource("s3")
    conn.create_bucket(Bucket="test_bucket")

    yield "test_bucket" + str(tmpdir)

    mock.stop()


@pytest.fixture
def s3_mock_dir(s3_client, mock_data_dir, s3_temp_dir):
    """A mock remote directory containing standard test data."""

    copy_tree(
        mock_data_dir, s3_temp_dir, mkdir_func=lambda x: x, cp_func=s3_client.put
    )

    return s3_temp_dir


@pytest.helpers.register
def assert_walk_equal(entries_a, entries_b):
    """Helper that asserts if two sets of walk entries are equal."""

    entries_a, entries_b = list(entries_a), list(entries_b)
    assert len(entries_a) == len(entries_b)

    for (root_a, dirs_a, files_a), (root_b, dirs_b, files_b) in zip(entries_a, entries_b):
        assert posixpath.dirname(root_a) == posixpath.dirname(root_a)
        assert set(dirs_a) == set(dirs_b)
        assert set(files_a) == set(files_b)
