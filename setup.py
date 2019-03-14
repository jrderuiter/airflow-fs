#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import setuptools

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = ["apache-airflow", "future"]

setup_requirements = ["pytest-runner"]

test_requirements = ["pytest", "pytest-mock", "pytest-cov"]
dev_requirements = [
    "moto",
    "pylint",
    "sphinx",
    "sphinx_rtd_theme",
    "watchdog",
    "black; python_version>'3'",
    "pytest-helpers-namespace",
    "bump2version"
]

extra_requirements = {
    "ftp": ["ftputil"],
    # Pyarrow issue on 2.7: https://issues.apache.org/jira/browse/ARROW-4413
    "hdfs": ["pyarrow<0.12; python_version<'3'", "pyarrow; python_version>='3'"],
    "s3": ["s3fs"],
    "sftp": ["pysftp"],
    "dev": dev_requirements + test_requirements,
}

extra_requirements["all"] = (
    extra_requirements["ftp"]
    + extra_requirements["hdfs"]
    + extra_requirements["s3"]
    + extra_requirements["sftp"]
)

setuptools.setup(
    author="Julian de Ruiter",
    author_email="julianderuiter@gmail.com",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    description="Composable filesystem hooks and operators for Airflow.",
    install_requires=requirements,
    extras_require=extra_requirements,
    license="MIT license",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="airflow_fs",
    name="airflow_fs",
    version="0.1.0",
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/jrderuiter/airflow-fs",
    zip_safe=False,
)
