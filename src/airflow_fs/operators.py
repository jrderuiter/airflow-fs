import os
import shutil

from airflow.models import BaseOperator
from airflow.utils import apply_defaults

from airflow_fs.hooks.local_hook import LocalHook


class CopyFileOperator(BaseOperator):
    """Operator for copying files between file systems."""

    template_fields = ("_src_path", "_dest_path")

    @apply_defaults
    def __init__(
        self, src_path, dest_path, src_hook=None, dest_hook=None, glob=False, **kwargs
    ):
        super().__init__(**kwargs)

        self._src_path = src_path
        self._dest_path = dest_path

        self._src_hook = src_hook or LocalHook()
        self._dest_hook = dest_hook or LocalHook()

        self._glob = glob

    def execute(self, context):
        with self._src_hook as src_hook, self._dest_hook as dest_hook:
            if self._glob:
                try:
                    tasks = [
                        (
                            file_path,
                            os.path.join(self._dest_path, os.path.basename(file_path)),
                        )
                        for file_path in src_hook.glob(self._src_path)
                    ]
                except NotImplementedError:
                    raise ValueError(
                        "Glob is not supported by {}".format(src_hook.__class__)
                    ) from None
            else:
                tasks = [(self._src_path, self._dest_path)]

            # Create directory if it doesn't exist. Note we only do this once,
            # as all dest_paths should share the same parent folder in the
            # current implementation.
            if tasks:
                dest_dir = os.path.dirname(tasks[0][1])
                dest_hook.makedirs(dest_dir, exist_ok=True)

            for src_path, dest_path in tasks:
                self.log.info("Copying file %s to %s", src_path, dest_path)
                with src_hook.open(src_path, mode="rb") as src_file:
                    with dest_hook.open(dest_path, mode="wb") as dest_file:
                        shutil.copyfileobj(src_file, dest_file)


class DeleteFileOperator(BaseOperator):
    """Deletes files at given path."""

    template_fields = ("_path",)

    @apply_defaults
    def __init__(self, path, glob=False, hook=None, **kwargs):
        super().__init__(**kwargs)
        self._path = path
        self._glob = glob
        self._hook = hook or LocalHook()

    def execute(self, context):
        with self._hook as hook:
            if self._glob:
                file_paths = hook.glob(self._path)
            else:
                file_paths = [self._path]

            for file_path in file_paths:
                self.log.info("Deleting file %s", file_path)
                hook.rm(file_path)


class DeleteTreeOperator(BaseOperator):
    """Deletes files at given path."""

    template_fields = ("_path",)

    @apply_defaults
    def __init__(self, path, glob=False, hook=None, **kwargs):
        super().__init__(**kwargs)
        self._path = path
        self._glob = glob
        self._hook = hook or LocalHook()

    def execute(self, context):
        with self._hook as hook:
            if self._glob:
                tree_paths = hook.glob(self._path)
            else:
                tree_paths = [self._path]

            for tree_path in tree_paths:
                self.log.info("Deleting file %s", tree_path)
                hook.rmtree(tree_path)
