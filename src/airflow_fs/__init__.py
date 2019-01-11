# -*- coding: utf-8 -*-

"""Top-level package for airflow-fs."""

from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

__author__ = """Julian de Ruiter"""
__email__ = 'julianderuiter@gmail.com'
