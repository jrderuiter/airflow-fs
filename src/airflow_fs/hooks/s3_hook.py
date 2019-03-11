"""File system hook for the S3 file system."""

from builtins import super
import posixpath

try:
    import s3fs
except ImportError:
    s3fs = None

from . import FsHook


class S3Hook(FsHook):
    """Hook for interacting with files in S3."""

    def __init__(self, conn_id=None):
        super().__init__()
        self._conn_id = conn_id
        self._conn = None

    def get_conn(self):
        if s3fs is None:
            raise ImportError("s3fs must be installed to use the S3Hook")

        if self._conn is None:
            if self._conn_id is None:
                self._conn = s3fs.S3FileSystem()
            else:
                config = self.get_connection(self._conn_id)

                extra_kwargs = {}
                if "encryption" in config.extra_dejson:
                    extra_kwargs["ServerSideEncryption"] = config.extra_dejson[
                        "encryption"
                    ]

                self._conn = s3fs.S3FileSystem(
                    key=config.login,
                    secret=config.password,
                    s3_additional_kwargs=extra_kwargs,
                )

        return self._conn

    def disconnect(self):
        self._conn = None

    def open(self, file_path, mode="rb"):
        return self.get_conn().open(file_path, mode=mode)

    def exists(self, file_path):
        return self.get_conn().exists(file_path)

    def isdir(self, path):
        if "/" not in path:
            # Path looks like a bucket name.
            return True

        parent_dir = posixpath.dirname(path)

        for child in self.get_conn().ls(parent_dir, detail=True):
            if child["Key"] == path and child["StorageClass"] == "DIRECTORY":
                return True

        return False

    def mkdir(self, dir_path, mode=0o755, exist_ok=True):
        self.makedirs(dir_path, mode=mode, exist_ok=exist_ok)

    def listdir(self, dir_path):
        return [posixpath.relpath(fp, start=dir_path)
                for fp in self.get_conn().ls(dir_path, details=False)]

    def rm(self, file_path):
        self.get_conn().rm(file_path, recursive=False)

    def rmtree(self, dir_path):
        self.get_conn().rm(dir_path, recursive=True)

    # Overridden default implementations.

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if self.exists(dir_path):
            if not exist_ok:
                self._raise_dir_exists(dir_path)
        else:
            self.get_conn().mkdir(dir_path)

    def walk(self, root):
        root = _remove_trailing_slash(root)
        for entry in super().walk(root):
            yield entry


def _remove_trailing_slash(path):
    if path.endswith("/"):
        return path[:-1]
    return path
