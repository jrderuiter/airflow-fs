"""File system hook for the local file system."""

from builtins import open, str
import os
import shutil

from . import FsHook


class LocalHook(FsHook):
    """Hook for interacting with local files on the local file system."""

    sep = os.sep

    def get_conn(self):
        return None

    def open(self, file_path, mode="rb"):
        return open(str(file_path), mode=mode)

    def exists(self, file_path):
        return os.path.exists(str(file_path))

    def isdir(self, path):
        return os.path.isdir(path)

    def mkdir(self, dir_path, mode=0o755, exist_ok=True):
        if os.path.exists(dir_path):
            if not exist_ok:
                self._raise_dir_exists(dir_path)
        else:
            os.mkdir(dir_path, mode)

    def listdir(self, dir_path):
        return os.listdir(dir_path)

    def rm(self, file_path):
        os.unlink(str(file_path))

    def rmtree(self, dir_path):
        shutil.rmtree(str(dir_path))

    # Overridden default implementations.

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if os.path.exists(dir_path):
            if not exist_ok:
                self._raise_dir_exists(dir_path)
        else:
            os.makedirs(str(dir_path), mode=mode)

    def walk(self, root):
        for tup in os.walk(root):
            yield tup
