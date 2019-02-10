import os
import posixpath


class MockConnection:
    def __init__(self, host=None, login=None, password=None, extra=None, port=None):
        self.host = host
        self.login = login
        self.password = password
        self.extra_dejson = extra or {}
        self.port = port


def copy_tree(local_dir, dest_dir, mkdir_func, cp_func):
    """Copies a local directory to a remote fs, using only an
       implementation of mkdir and cp (to copy files) functions.
    """

    for root, dirs, files in os.walk(local_dir):
        rel_root = posixpath.relpath(root, local_dir)

        dest_root = posixpath.join(dest_dir, rel_root)
        mkdir_func(dest_root)

        for file_name in files:
            src_path = posixpath.normpath(
                posixpath.join(local_dir, rel_root, file_name)
            )
            dest_path = posixpath.normpath(
                posixpath.join(dest_dir, rel_root, file_name)
            )
            cp_func(src_path, dest_path)