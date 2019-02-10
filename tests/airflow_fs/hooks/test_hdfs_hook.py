from os import path, walk
import posixpath
from pyarrow import hdfs

import pytest

from airflow_fs.hooks import HdfsHook


@pytest.fixture(scope="session")
def mock_data_dir():
    """Directory containing test data files."""
    return path.join(path.dirname(__file__), "..", "data")


@pytest.fixture(scope="session")
def hdfs_client():
    """Hdfs client to be used while testing."""
    return hdfs.connect()


@pytest.fixture
def hdfs_dir(mock_data_dir, hdfs_client, tmpdir):
    """Creates a mock hdfs directory containing the standard test data."""

    src_base_dir, dest_base_dir = mock_data_dir, tmpdir

    for root, dirs, files in walk(src_base_dir):
        rel_root = path.relpath(root, src_base_dir)

        dest_root = posixpath.join(dest_base_dir, rel_root)
        hdfs_client.mkdir(dest_root)

        for file_name in files:
            src_path = posixpath.join(src_base_dir, rel_root, file_name)
            dest_path = posixpath.join(dest_base_dir, rel_root, file_name)
            with open(src_path, "rb") as file_:
                hdfs_client.upload(dest_path, file_)

    # request.addfinalizer(lambda: hdfs_client.rm(str(dest_base_dir), recursive=True))

    return str(dest_base_dir)


class TestHdfsHook:
    """Tests for the HdfsHook class."""

    def test_open_read(self, hdfs_dir):
        """Tests reading of a file using the `open` method."""

        file_path = posixpath.join(hdfs_dir, "test.txt")
        with HdfsHook() as hook:
            with hook.open(file_path) as file_:
                content = file_.read()
            assert content == b"Test file\n"

    def test_open_write(self, hdfs_client, tmpdir):
        """Tests writing of a file using the `open` method."""

        file_path = posixpath.join(tmpdir, "test2.txt")
        assert not hdfs_client.exists(file_path)

        with HdfsHook() as hook:
            with hook.open(file_path, "wb") as file_:
                file_.write(b"Test file\n")

        assert hdfs_client.exists(file_path)

    def test_exists(self, hdfs_dir):
        """Tests the `exists` method."""

        with HdfsHook() as hook:
            assert hook.exists(posixpath.join(hdfs_dir, "subdir"))
            assert hook.exists(posixpath.join(hdfs_dir, "test.txt"))
            assert not hook.exists(posixpath.join(hdfs_dir, "non-existing.txt"))

    def test_isdir(self, hdfs_dir):
        """Tests the `isdir` method."""

        with HdfsHook() as hook:
            assert hook.isdir(posixpath.join(hdfs_dir, "subdir"))
            assert not hook.isdir(posixpath.join(hdfs_dir, "test.txt"))

    def test_listdir(self, hdfs_dir):
        """Tests the `listdir` method."""

        with HdfsHook() as hook:
            assert set(hook.listdir(hdfs_dir)) == {"test.txt", "subdir"}

    def test_mkdir(self, hdfs_client, tmpdir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(tmpdir, "subdir")
        assert not hdfs_client.exists(dir_path)

        with HdfsHook() as hook:
            hook.mkdir(dir_path, mode=0o750)

        assert hdfs_client.exists(dir_path)
        assert hdfs_client.info(dir_path)["permissions"] == 0o750

    def test_mkdir_exists(self, hdfs_client, tmpdir):
        """Tests the `mkdir` method with the exists_ok parameter."""

        dir_path = posixpath.join(tmpdir, "subdir")
        assert not hdfs_client.exists(dir_path)

        with HdfsHook() as hook:
            hook.mkdir(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.mkdir(dir_path, exist_ok=False)

            hook.mkdir(dir_path, exist_ok=True)

    def test_rm(self, hdfs_client, hdfs_dir):
        """Tests the `rm` method."""

        file_path = posixpath.join(hdfs_dir, "test.txt")
        assert hdfs_client.exists(file_path)

        with HdfsHook() as hook:
            hook.rm(file_path)

        assert not hdfs_client.exists(file_path)

    def test_rmtree(self, hdfs_client, hdfs_dir):
        """Tests the `rmtree` method."""

        dir_path = posixpath.join(hdfs_dir, "subdir")
        assert hdfs_client.exists(dir_path)

        with HdfsHook() as hook:
            hook.rmtree(dir_path)

        assert not hdfs_client.exists(dir_path)

    def test_makedirs(self, hdfs_client, tmpdir):
        """Tests the `mkdir` method with mode parameter."""

        dir_path = posixpath.join(tmpdir, "some", "nested", "dir")

        with HdfsHook() as hook:
            hook.makedirs(dir_path, mode=0o750)

        assert hdfs_client.exists(dir_path)
        assert hdfs_client.info(dir_path)["permissions"] == 0o750

    def test_makedirs_exists(self, hdfs_client, tmpdir):
        """Tests the `mkdir` method with exists_ok parameter."""

        dir_path = posixpath.join(tmpdir, "some", "nested", "dir")

        with HdfsHook() as hook:
            hook.makedirs(dir_path, exist_ok=False)

            with pytest.raises(IOError):
                hook.makedirs(dir_path, exist_ok=False)

            hook.makedirs(dir_path, exist_ok=True)

    def test_walk(self, hdfs_client, hdfs_dir):
        """Tests the `walk` method."""

        with HdfsHook() as hook:
            entries = list(hook.walk(hdfs_dir))

        assert entries[0] == (hdfs_dir, ["subdir"], ["test.txt"])
        assert entries[1] == (posixpath.join(hdfs_dir, "subdir"), [], ["nested.txt"])
