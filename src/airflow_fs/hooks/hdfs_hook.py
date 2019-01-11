try:
    import hdfs3
except ImportError:
    hdfs3 = None

from . import FsHook


class HdfsHook(FsHook):
    """Hook for interacting with files over HDFS."""

    def __init__(self, conn_id=None):
        super().__init__()
        self._conn_id = conn_id
        self._conn = None

    def get_conn(self):
        if hdfs3 is None:
            raise ImportError("hfds3 must be installed to use the HdfsHook")

        if self._conn is None:
            if self._conn_id is None:
                self._conn = hdfs3.HDFileSystem()
            else:
                config = self.get_connection(self._conn_id)
                config_extra = config.extra_dejson

                # Extract hadoop parameters from extra.
                pars = config_extra.get("pars", {})

                # Collect extra parameters to pass to kwargs.
                extra_kws = {}
                if config.login is not None:
                    extra_kws["user"] = config.login

                # Build connection.
                self._conn = hdfs3.HDFileSystem(
                    host=config.host, port=config.port, pars=pars, **extra_kws
                )

        return self._conn

    def disconnect(self):
        if self._conn is not None:
            self._conn.disconnect()
        self._conn = None

    def open(self, file_path, mode="rb"):
        return self.get_conn().open(file_path, mode=mode)

    def exists(self, file_path):
        return self.get_conn().exists(file_path)

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            raise ValueError("Directory already exists")
        self.get_conn().makedirs(dir_path, mode=mode)

    def glob(self, pattern):
        return self.get_conn().glob(pattern)

    def remove(self, file_path):
        self.get_conn().rm(file_path, recursive=False)

    def rmtree(self, dir_path):
        self.get_conn().rm(dir_path, recursive=True)
