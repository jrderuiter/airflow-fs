import os
from os import path
import shutil

from airflow.hooks.base_hook import BaseHook


class FsHook(BaseHook):
    """Base FsHook defining the FsHook interface and providing some basic
       functionality built on this interface.
    """

    # TODO: Allow copy_* methods to copy from non-local file systems
    #   using the hooks themselves. Requires a `walk` implementation.
    # TODO: Add methods for deleting files (`rm`).
    # TODO: Add non-recursive mkdir method? (May allow better performance
    #   than makedirs.)

    def __init__(self):
        super().__init__(source=None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def disconnect(self):
        """Closes fs connection (if applicable)."""
        pass

    def open(self, file_path, mode="rb"):
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

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        """Creates directory, creating intermediate directories if needed.

        :param str dir_path: Path to the directory to create.
        :param int mode: Mode to use for directory (if created).
        :param bool exist_ok: Whether the directory is already allowed to exist.
            If false, a ValueError is raised if the directory exists.
        """
        raise NotImplementedError()

    def glob(self, pattern):
        """Returns list of paths matching pattern (i.e., with “*”s).

        :param str pattern: Pattern to match

        :returns: List of matched file paths.
        :rtype: list[str]
        """
        raise NotImplementedError()

    def remove(self, file_path):
        """Deletes the given file path.

        :param str file_path: Path to file:
        """
        raise NotImplementedError()

    def rmtree(self, dir_path):
        """Deletes given directory tree recursively.

        :param str dir_path: Path to directory to delete.
        """
        raise NotImplementedError()

    def copy_file(self, src_path, dest_path):
        """Copies local file to given path.

        :param str src_path: Path to source file.
        :param str dest_path: Path to destination file.
        """

        with open(src_path, "rb") as src_file, self.open(dest_path, "wb") as dest_file:
            shutil.copyfileobj(src_file, dest_file)

    def copy_fileobj(self, src_obj, dest_path):
        """Copies fileobj to given path.

        :param src_obj: Source file-like object.
        :param str dest_path: Path to destination file.
        """

        with self.open(dest_path, "wb") as dest_file:
            shutil.copyfileobj(src_obj, dest_file)

    def copy_dir(self, src_dir, dest_dir):
        """Copies local directory recursively to given path.

        :param str src_dir: Path to source directory.
        :param str dest_path: Path to destination directory.
        """

        # Create root dest dir.
        self.makedirs(dest_dir, exist_ok=True)

        for root, dirs, files in os.walk(src_dir):
            # Copy over files.
            for item in files:
                src_path = path.join(root, item)

                rel_path = path.relpath(src_path, src_dir)
                dest_path = path.join(dest_dir, rel_path)

                self.copy_file(src_path, dest_path)

            # Create sub-directories.
            for item in dirs:
                src_path = path.join(root, item)

                rel_path = path.relpath(src_path, src_dir)
                dest_path = path.join(dest_dir, rel_path)

                self.makedirs(dest_path, exist_ok=True)


class NotSupportedError(NotImplementedError):
    """Exception that may be raised by FsHooks if the don't support
       the given operation.
    """
