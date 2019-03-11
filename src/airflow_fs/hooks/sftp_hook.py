"""File system hook for the SFTP (SSH) file system."""

from builtins import super

try:
    import pysftp
except ImportError:
    pysftp = None

from . import FsHook


class SftpHook(FsHook):
    """Hook for interacting with files over SFTP."""

    # TODO: Use walktree for a more efficient walk implementation?

    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id
        self._conn = None

    def get_conn(self):
        if pysftp is None:
            raise ImportError("pysftp must be installed to use the SftpHook")

        if self._conn is None:
            params = self.get_connection(self._conn_id)
            private_key = params.extra_dejson.get('private_key', None)

            cnopts = pysftp.CnOpts()
            if params.extra_dejson.get('ignore_hostkey_verification', False):
                cnopts.hostkeys = None

            if not private_key:
                self._conn = pysftp.Connection(
                    params.host,
                    username=params.login,
                    password=params.password,
                    cnopts=cnopts)
            elif private_key and params.password:
                self._conn = pysftp.Connection(
                    params.host,
                    username=params.login,
                    private_key=private_key,
                    private_key_pass=params.password,
                    cnopts=cnopts)
            else:
                self._conn = pysftp.Connection(
                    params.host,
                    username=params.login,
                    private_key=private_key,
                    cnopts=cnopts)

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
        if self.exists(dir_path):
            if not exist_ok:
                self._raise_dir_exists(dir_path)
        else:
            self.get_conn().mkdir(dir_path, mode=self._int_mode(mode))

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
        if self.exists(dir_path):
            if not exist_ok:
                self._raise_dir_exists(dir_path)
        else:
            self.get_conn().makedirs(dir_path, mode=self._int_mode(mode))

    @staticmethod
    def _int_mode(mode):
        """Convert octal mode to its literal int representation."""
        return int(oct(mode)[-3:])
