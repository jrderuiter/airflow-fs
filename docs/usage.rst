=====
Usage
=====

Hooks
-----

Reading/writing files
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from airflow_fs.hooks import FtpHook

    with FtpHook(conn_id="ftp_default") as ftp_hook:
        with ftp_hook.open("some_file.txt") as file_:
            content = file_.read()

.. code-block:: python

    with FtpHook(conn_id="ftp_default") as ftp_hook:
        with ftp_hook.open("some_file.txt", "wb") as file_:
            file_.write("data\n")

Checking for existence
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    with FtpHook(conn_id="ftp_default") as ftp_hook:
        ftp_hook.exists("some_file.txt")

Deleting files or directories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    with FtpHook(conn_id="ftp_default") as ftp_hook:
        ftp_hook.rm("some_file.txt")


.. code-block:: python

    with FtpHook(conn_id="ftp_default") as ftp_hook:
        ftp_hook.rmtree("some_directory")


Creating directories
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    with FtpHook(conn_id="ftp_default") as ftp_hook:
        csv_paths = ftp_hook.mkdir("some_directory", exist_ok=True)

.. code-block:: python

    with FtpHook(conn_id="ftp_default") as ftp_hook:
        csv_paths = ftp_hook.makedirs("some/nested/directory", exist_ok=True)

Listing directories
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    with FtpHook(conn_id="ftp_default") as ftp_hook:
        csv_paths = ftp_hook.listdir("some_directory")

.. code-block:: python

    with FtpHook(conn_id="ftp_default") as ftp_hook:
        for root, dirs, files in ftp_hook.walk("some_directory"):
            pass

.. code-block:: python

    with FtpHook(conn_id="ftp_default") as ftp_hook:
        csv_paths = ftp_hook.glob("some_directory/*.csv")

Copying files
~~~~~~~~~~~~~

.. code-block:: python

    from airflow_fs.hooks import FtpHook, SftpHook

    with SftpHook(conn_id="sftp_default") as src_hook:
        with FtpHook(conn_id="ftp_default") as dest_hook:
            dest_hook.copy_file(
                "src_file.txt",
                "dest_file.txt",
                src_hook=src_hook)


.. code-block:: python

    with FtpHook(conn_id="ftp_default") as ftp_hook:
        with open("local.txt") as file_:
            ftp_hook.copy_fileobj(file_, "dest_file.txt")

Note that this can also be achieved using the `LocalHook` for accessing the local
file system.

Operators
---------

Copying files
~~~~~~~~~~~~~

.. code-block:: python

    from airflow_fs.hooks import S3Hook, FtpHook
    from airflow_fs.operators import CopyFileOperator

    copy_task = CopyFileOperator(
        src_path="my-bucket/example.txt",
        dest_path="example.txt",
        src_hook=S3Hook(conn_id="s3_default"),
        dest_hook=FtpHook(conn_id="ftp_default")
    )

.. code-block:: python

    copy_task = CopyFileOperator(
        src_path="my-bucket/*.csv",
        dest_path="dest_directory",
        src_hook=S3Hook(conn_id="s3_default"),
        dest_hook=FtpHook(conn_id="ftp_default")
    )

Deleting files or directories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from airflow_fs.operators import DeleteFileOperator

    delete_task = DeleteFileOperator(
        "example.txt",
        hook=FtpHook(conn_id="ftp_default")
    )

.. code-block:: python

    delete_task = DeleteFileOperator(
        "*.csv",
        hook=FtpHook(conn_id="ftp_default")
    )

.. code-block:: python

    from airflow_fs.operators import DeleteTreeOperator

    delete_task = DeleteTreeOperator(
        "some_directory",
        hook=FtpHook(conn_id="ftp_default")
    )
