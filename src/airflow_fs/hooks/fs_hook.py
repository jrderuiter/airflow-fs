"""Base class defining the file system hook interface."""

from builtins import super
import errno
import posixpath
import shutil

from airflow.hooks.base_hook import BaseHook

from airflow_fs.ports import glob


class FsHook(BaseHook):
    """Base FsHook defining the FsHook interface and providing some basic
       functionality built on this interface.
    """

    def __init__(self):
        super().__init__(source=None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def disconnect(self):
        """Closes fs connection (if applicable)."""

    # Interface methods (should be implemented by sub-classes).

    # pylint: disable=missing-docstring
    def get_conn(self):
        raise NotImplementedError()

    def open(self, file_path, mode='rb'):
        """Returns file_obj for given file path.

        :param str file_path: Path to the file to open.
        :param str mode: Mode to open the file in.

        :returns: An opened file object.
        """
        raise NotImplementedError()

    def exists(self, file_path):
        """Checks whether the given file path exists.

        :param str file_path: File path.

        :returns: True if the file exists, else False.
        :rtype: bool
        """
        raise NotImplementedError()

    def isdir(self, path):
        """Returns true if the given path points to a directory.

        :param str path: File or directory path.
        """
        raise NotImplementedError()

    def listdir(self, dir_path):
        """Lists names of entries in the given path."""
        raise NotImplementedError()

    def mkdir(self, dir_path, mode=0o755, exist_ok=True):
        """Creates the directory, without creating intermediate directories."""
        raise NotImplementedError()

    def rm(self, file_path):
        """Deletes the given file path.

        :param str file_path: Path to file:
        """
        raise NotImplementedError()

    def rmtree(self, dir_path):
        """Deletes given directory tree recursively.

        :param str dir_path: Path to directory to delete.
        """
        raise NotImplementedError()

    @staticmethod
    def _raise_dir_exists(dir_path):
        raise IOError(errno.EEXIST,
                      'Directory exists: {!r}'.format(dir_path))

    # General utility methods built on the above interface methods.

    # These methods can be overridden in sub-classes if more efficient
    # implementations are available for a specific file system.

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        """Creates directory, creating intermediate directories if needed.

        :param str dir_path: Path to the directory to create.
        :param int mode: Mode to use for directory (if created).
        :param bool exist_ok: Whether the directory is already allowed to exist.
            If false, an IOError is raised if the directory exists.
        """

        head, tail = posixpath.split(dir_path)
        if not tail:
            head, tail = posixpath.split(head)
        if head and tail and not self.exists(head):
            try:
                self.makedirs(head, mode=mode, exist_ok=exist_ok)
            except FileExistsError:
                # Defeats race condition when another thread created the path
                pass
            current_dir = posixpath.curdir
            if isinstance(tail, bytes):
                current_dir = bytes(posixpath.curdir, 'ASCII')
            if tail == current_dir: # xxx/newdir/. exists if xxx/newdir exists
                return
        try:
            self.mkdir(dir_path, mode=mode, exist_ok=exist_ok)
        except OSError:
            # Cannot rely on checking for EEXIST, since the operating system
            # could give priority to other errors like EACCES or EROFS
            if not exist_ok or not self.isdir(dir_path):
                raise

    def walk(self, root):
        """Directory tree generator, similar to os.walk."""

        sub_dirs, files = [], []
        for item in self.listdir(root):
            full_path = posixpath.join(root, item)
            if self.isdir(full_path):
                sub_dirs.append(item)
            else:
                files.append(item)

        yield root, sub_dirs, files

        for sub_dir in sub_dirs:
            for entry in self.walk(posixpath.join(root, sub_dir)):
                yield entry

    def glob(self, pattern, recursive=False):
        """Return a list of paths matching a pathname pattern."""
        return glob.glob(pattern, recursive=recursive, hook=self)

    # Methods for copying files between hooks.

    def copy(self, src_path, dest_path, src_hook=None):
        """Copies file(s) into the hooks file system.

        By default, source files are assumed to be on the same file system as the
        destination (the hooks file system). To copy between different file systems
        or file systems in different locations, a source file hook can be provided
        using the `src_hook` argument.
        """

        # TODO: Allow short circuiting when copying within the same filesystem
        #   with the same connection details?

        with src_hook.open(src_path, "rb") as src_file, \
                self.open(dest_path, "wb") as dest_file:
            shutil.copyfileobj(src_file, dest_file)

    def copy_fileobj(self, file_obj, dest_path):
        """Copies a file object into the hooks file system."""
        with self.open(dest_path, "wb") as dst_file:
            shutil.copyfileobj(file_obj, dst_file)


class NotSupportedError(NotImplementedError):
    """Exception that can be raised by FsHooks if they don't support
       a given operation.
    """
