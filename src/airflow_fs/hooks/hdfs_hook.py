"""File system hook for the HDFS file system."""

from builtins import super

try:
    from pyarrow import hdfs
except ImportError:
    hdfs = None

from . import FsHook


class HdfsHook(FsHook):
    """Hook for interacting with files over HDFS."""

    def __init__(self, conn_id=None):
        super().__init__()
        self._conn_id = conn_id
        self._conn = None

    def get_conn(self):
        if hdfs is None:
            raise ImportError("hdfs3 must be installed to use the HdfsHook")

        if self._conn is None:
            if self._conn_id is None:
                self._conn = hdfs.connect()
            else:
                config = self.get_connection(self._conn_id)
                config_extra = config.extra_dejson

                # Build connection.
                self._conn = hdfs.connect(
                    host=config.host or "default",
                    port=config.port or 0,
                    user=config.login,
                    driver=config_extra.get("driver", "libhdfs"),
                    extra_conf=config_extra.get("extra_conf", None),
                )

        return self._conn

    def disconnect(self):
        self._conn = None

    def open(self, file_path, mode="rb"):
        return self.get_conn().open(file_path, mode=mode)

    def exists(self, file_path):
        return self.get_conn().exists(file_path)

    def isdir(self, path):
        info = self.get_conn().info(path)
        return info["kind"] == "directory"

    def listdir(self, dir_path):
        return [
            self._strip_prefix(path_, parent=dir_path)
            for path_ in self.get_conn().ls(dir_path)
        ]

    def mkdir(self, dir_path, mode=0e755, exist_ok=True):
        self.makedirs(dir_path, mode=mode, exist_ok=exist_ok)

    @staticmethod
    def _strip_prefix(path_, parent="/"):
        """Strips 'file:' prefix and (optional) parent dir from file path."""

        stripped = path_.split(":")[-1]

        if not parent.endswith("/"):
            parent = parent + "/"

        if stripped.startswith(parent):
            stripped = stripped[len(parent) :]

        return stripped

    def rm(self, file_path):
        self.get_conn().delete(file_path, recursive=False)

    def rmtree(self, dir_path):
        self.get_conn().delete(dir_path, recursive=True)

    # Overridden default implementations.

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        conn = self.get_conn()

        if conn.exists(dir_path):
            if not exist_ok:
                self._raise_dir_exists(dir_path)
        else:
            # mkdir is recursive by default.
            conn.mkdir(dir_path)
            conn.chmod(dir_path, mode=mode)

    def walk(self, root):
        for tup in self.get_conn().walk(root):
            yield tup
