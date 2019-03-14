.. highlight:: shell

============
Installation
============

Stable release
--------------

To install airflow-fs, run this command in your terminal:

.. code-block:: console

    $ pip install airflow-fs

This is the preferred method to install airflow-fs, as it will always install the most
recent stable release. If you don't have `pip`_ installed, the
`Python installation guide`_ can guide you through the process.

To minimize its dependencies, airflow-fs is installed without file system-specific
dependencies by default. To install file system-specific dependencies, you need to
install airflow-fs with the extra requirements for the respective file system.

For example, to install airflow-fs with FTP-specific dependencies use:

.. code-block:: console

    $ pip install airflow-fs[ftp]

Other available extra's are `hdfs`, `s3` and `sftp`:

.. code-block:: console

    $ pip install airflow-fs[hdfs]  # or s3, sftp (or a combination)

You can also use the extra `all` to install dependencies for all supported file systems:

.. code-block:: console

    $ pip install airflow-fs[all]


.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/


From sources
------------

The sources for airflow-fs can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://github.com/jrderuiter/airflow-fs

Or download the `tarball`_:

.. code-block:: console

    $ curl  -OL https://github.com/jrderuiter/airflow-fs/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ pip install .


.. _Github repo: https://github.com/jrderuiter/airflow-fs
.. _tarball: https://github.com/jrderuiter/airflow-fs/tarball/master
