#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import setuptools

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = []

extra_requirements = {
    "ftp": ["ftputil"],
    "hdfs": ["hdfs3"],
    "s3": ["s3fs"],
    "sftp": ["pysftp"],
    "dev": ["moto", "sphinx", "sphinx_rtd_theme"],
}

setup_requirements = ["pytest-runner", "setuptools_scm"]

test_requirements = ["pytest", "pytest-mock", "pytest-helpers-namespace"]

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
    description="Filesystem hooks and operators for Airflow.",
    install_requires=requirements,
    extras_require=extra_requirements,
    license="MIT license",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="airflow_fs",
    name="airflow_fs",
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    use_scm_version=True,
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/jrderuiter/airflow-fs",
    zip_safe=False,
)
