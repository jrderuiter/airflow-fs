.. highlight:: shell

========
Concepts
========

Why airflow-fs?
---------------

Although Airflow provides a large set of builtin hooks and operators to work with,
these builtin components generally lack a common reusable interface across related
components. This limitation is especially glaring in the set of file system hooks
provided by Airflow, which require a developer to use entirely different interfaces
for working with different file systems. This (among other things) has lead to the
widespread development of many a-to-b operators (e.g., S3ToHiveOperator,
GcsToS3Operator, etc.), resulting in unnecessary code duplication.

airflow-fs aims to solve this issue by defining a common interface for file system hooks,
which is based on a subset of functions from the `os` and `shutil` modules in the
Python standard library. This interface allows operations to be performed across
different file systems using the same code, easing their use for developers. Moreover,
the common interface increases the composability of file system hooks, enabling the
development of common operators for performing tasks, independent of the underlying
file system(s).

File system hooks
-----------------

File system hooks are Airflow hooks that follow a common interface by extending the
`FSHook` base class and providing implementations for several abstract methods. This
allows us to work with different file systems using virtually the same code. For
example, following this approach makes reading a file from an FTP server or an S3 file
system virtually identical:

.. code-block:: python

    from airflow_fs.hooks import FtpHook, S3Hook

    # Reading a file from FTP.
    with FtpHook(conn_id="ftp_default") as ftp_hook:
        with ftp_hook.open("some_file.txt") as file_:
            content = file_.read()

    # Reading a file from S3.
    with S3Hook(conn_id="s3_default") as s3_hook:
        with s3_hook.open("some_file.txt") as file_:
            content = file_.read()

For more details on the methods provided by file system hooks, see the Usage and
API sections of this documentation.

Out of the box, airflow-fs provides hooks for a number of frequently used file systems
such as FTP, S3, SFTP and HDFS. Support for additional file systems can be added by
implementing additional `FsHook` subclasses, which provide file system-specific
implementations for the following methods:

- `open` - Opens a file for reading writing, similar to the builtin `open` function.
- `exists` - Checks if a given file or directory exists, similar to `os.path.exists`.
- `isdir` - Checks if a given path points to a directory, similar to `os.path.isdir`.
- `listdir` - Lists files and subdirectories in a given directory, similar to `os.listdir`.
- `mkdir` - Creates a new directory, similar to `os.mkdir`.
- `rm` - Deletes a file, similar to `os.unlink`.
- `rmtree` - Deletes a directory tree, similar to `shutil.rmtree`.

Additional methods for more complex operations such as copying files, etc. are readily
provided by the `FsHook` base class (see the `FSHook` API for more details). These
methods are generally implemented using the base methods above and do not have to
implemented for each specific hook.

File system operators
---------------------

Besides hooks, airflow-fs also provides several file system operators for performing
common tasks such as copying and deleting files. These operators are built on top of
file system hooks to make their implementation independent of the underlying file
system.

For example, copying two files between any two file systems can be achieved
using the `CopyFileOperator`:

.. code-block:: python

    from airflow_fs.hooks import S3Hook, FtpHook
    from airflow_fs.operators import CopyFileOperator

    copy_file_task = CopyFileOperator(
        src_path="my-bucket/example.txt",
        dest_path="example.txt",
        src_hook=S3Hook(conn_id="s3_default"),
        dest_hook=FtpHook(conn_id="ftp_default")
    )

For more details on the different file system operators, see the Usage and
API sections.
