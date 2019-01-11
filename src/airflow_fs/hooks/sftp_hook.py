from os import path

try:
    import pysftp
except ImportError:
    pysftp = None

from airflow_fs.utils import fnmatch

from . import FsHook




class SftpHook(FsHook):
    """Hook for interacting with files over SFTP."""

    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id
        self._conn = None

    def get_conn(self):
        if pysftp is None:
            raise ImportError("pysftp must be installed to use the SftpHook")

        if self._conn is None:
            config = self.get_connection(self._conn_id)

            private_key = config.extra_dejson.get("private_key", False)

            if not private_key:
                self._conn = pysftp.Connection(
                    config.host, username=config.login, password=config.password
                )
            elif private_key and config.password:
                self._conn = pysftp.Connection(
                    config.host,
                    username=config.login,
                    private_key=private_key,
                    private_key_pass=config.password,
                )
            else:
                self._conn = pysftp.Connection(
                    config.host, username=config.login, private_key=private_key
                )

        return self._conn

    def disconnect(self):
        if self._conn is not None:
            self._conn.close()
        self._conn = None

    def open(self, file_path, mode="rb"):
        return self.get_conn().open(file_path, mode=mode)

    def exists(self, file_path):
        return self.get_conn().exists(file_path)

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            raise ValueError("Directory {} already exists".format(dir_path))
        self.get_conn().makedirs(dir_path, mode=mode)

    def glob(self, pattern):
        # Determine base directory to start searching from.
        base_dir = path.dirname(pattern.split("*")[0])

        # Obtain all file paths in base_dir, recursively.
        file_paths = []
        self.get_conn().walktree(
            base_dir,
            recurse=True,
            fcallback=file_paths.append,
            dcallback=lambda x: x,
            ucallback=lambda x: x,
        )

        return fnmatch.filter(file_paths, pattern, sep="/")

    def remove(self, file_path):
        self.get_conn().remove(file_path)

    def rmtree(self, dir_path):
        result = self.get_conn().execute("rm -r {!r}".format(dir_path))

        if result:
            message = b"\n".join(result)
            raise OSError(message.decode())
