from builtins import super
import posixpath

try:
    import pysftp
except ImportError:
    pysftp = None

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
            params = self.get_connection(self._conn_id)

            cn_opts = pysftp.CnOpts()

            if params.extra_dejson.get('ignore_hostkey_verification', False):
                cn_opts.hostkeys = None

            private_key = params.extra_dejson.get('private_key', None)

            if not private_key:
                self._conn = pysftp.Connection(
                    params.host,
                    username=params.login,
                    password=params.password)
            elif private_key and params.password:
                self._conn = pysftp.Connection(
                    params.host,
                    username=params.login,
                    private_key=private_key,
                    private_key_pass=params.password)
            else:
                self._conn = pysftp.Connection(
                    params.host,
                    username=params.login,
                    private_key=private_key)

        return self._conn

    def disconnect(self):
        if self._conn is not None:
            self._conn.close()
        self._conn = None

    def open(self, file_path, mode='rb'):
        return self.get_conn().open(file_path, mode=mode)

    def exists(self, file_path):
        return self.get_conn().exists(file_path)

    def isdir(self, path):
        return self.get_conn().isdir(path)

    def mkdir(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            self._raise_dir_exists(dir_path)
        self.get_conn().mkdir(dir_path, mode=int(oct(mode)[2:]))

    def listdir(self, dir_path):
        return self.get_conn().listdir(dir_path)

    def rm(self, file_path):
        self.get_conn().remove(file_path)

    def rmtree(self, dir_path):
        result = self.get_conn().execute('rm -r {!r}'.format(dir_path))

        if result:
            message = b'\n'.join(result)
            raise OSError(message.decode())

    # Overridden default implementations.

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            self._raise_dir_exists(dir_path)
        self.get_conn().makedirs(dir_path, mode=int(oct(mode)[2:]))
