=========
AirflowFS
=========

.. image:: https://circleci.com/gh/jrderuiter/airflow-fs.svg
        :target: https://circleci.com/gh/jrderuiter/airflow-fs

.. image:: https://img.shields.io/pypi/v/airflow_fs.svg
        :target: https://pypi.python.org/pypi/airflow-fs

AirflowFS is Python package that provides hooks and operators for manipulating
files across a variety of file systems using Apache Airflow.

Why AirflowFS?
--------------

In contrast to Airflows builtin file system hooks/operators, AirflowFS implements a
single interface for different file system hooks. This approach allows us to interact
with files independently of the underlying file system, using a common set of operators
for performing general operations such as copying and deleting files.

Currently, AirflowFS supports the following file systems: local, FTP, HDFS, S3 and SFTP.
Support for additional file systems can be added by implementing an additional file
system hook that adheres to the required hook interface. See the documentation for more
details.

Documentation
-------------

Detailed documentation is available at: https://jrderuiter.github.io/airflow-fs.

License
-------

This software is freely available under the MIT license.
