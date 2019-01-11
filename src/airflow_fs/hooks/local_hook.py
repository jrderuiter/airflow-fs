import glob
import os
import shutil

from . import FsHook


class LocalHook(FsHook):
    """Dummy hook that represents local file system."""

    def get_conn(self):
        return None

    def open(self, file_path, mode="rb"):
        return open(str(file_path), mode=mode)

    def exists(self, file_path):
        return os.path.exists(str(file_path))

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        os.makedirs(str(dir_path), mode=mode, exist_ok=exist_ok)

    def glob(self, pattern):
        return glob.glob(str(pattern))

    def remove(self, file_path):
        os.unlink(file_path)

    def rmtree(self, dir_path):
        shutil.rmtree(str(dir_path))
