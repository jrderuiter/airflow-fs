"""File system operators, built on the file system hook interface."""

import posixpath

from airflow.models import BaseOperator
from airflow.utils import apply_defaults

from airflow_fs.hooks.local_hook import LocalHook
from airflow_fs.ports import glob

# pylint: disable=unused-argument,missing-docstring


class CopyFileOperator(BaseOperator):
    """Operator for copying files between file systems."""

    template_fields = ("_src_path", "_dest_path")

    @apply_defaults
    def __init__(self, src_path, dest_path, src_hook=None, dest_hook=None, **kwargs):
        super(CopyFileOperator, self).__init__(**kwargs)

        self._src_path = src_path
        self._dest_path = dest_path

        self._src_hook = src_hook or LocalHook()
        self._dest_hook = dest_hook or LocalHook()

    def execute(self, context):
        with self._src_hook as src_hook, self._dest_hook as dest_hook:
            for src_path, dest_path in self._glob_copy_paths(
                self._src_path, self._dest_path, src_hook=src_hook
            ):
                dest_hook.copy(src_path, dest_path, src_hook=src_hook)

    @staticmethod
    def _glob_copy_paths(src_path, dest_path, src_hook):
        if glob.has_magic(src_path):
            for src_file_path in src_hook.glob(src_path):
                base_name = posixpath.basename(src_file_path)
                dest_file_path = posixpath.join(dest_path, base_name)
                yield src_file_path, dest_file_path
        else:
            yield src_path, dest_path


class DeleteFileOperator(BaseOperator):
    """Deletes files at given path."""

    template_fields = ("_path",)

    @apply_defaults
    def __init__(self, path, hook=None, **kwargs):
        super(DeleteFileOperator, self).__init__(**kwargs)
        self._path = path
        self._hook = hook or LocalHook()

    def execute(self, context):
        with self._hook as hook:
            for path_ in hook.glob(self._path):
                if not hook.isdir(path_):
                    self.log.info("Deleting file %s", path_)
                    hook.rm(path_)


class DeleteTreeOperator(BaseOperator):
    """Deletes files at given path."""

    template_fields = ("_path",)

    @apply_defaults
    def __init__(self, path, hook=None, **kwargs):
        super(DeleteTreeOperator, self).__init__(**kwargs)
        self._path = path
        self._hook = hook or LocalHook()

    def execute(self, context):
        with self._hook as hook:
            for path_ in hook.glob(self._path):
                if hook.isdir(path_):
                    self.log.info("Deleting directory %s", path_)
                    hook.rmtree(path_)
