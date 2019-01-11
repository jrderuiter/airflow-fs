"""Tests for the `dataflows.utils.fnmatch` module."""

import pytest

from airflow_fs.utils import fnmatch

# pylint: disable=redefined-outer-name,no-self-use


class TestFnmatch:
    """Tests for the fnmatch function."""

    def test_basic(self):
        """Tests basic glob."""
        assert fnmatch.fnmatch("test.csv", "*.csv")
        assert fnmatch.fnmatch("test.txt", "*.txt")
        assert not fnmatch.fnmatch("test.txt", "*.csv")

    def test_recursive(self):
        """Tests recursive glob."""
        assert not fnmatch.fnmatch("test.csv", "**/*.csv")
        assert fnmatch.fnmatch("nested/test.csv", "**/*.csv")
        assert fnmatch.fnmatch("deep/nested/test.csv", "**/*.csv")


class TestFilter:
    """Tests for the fnmatch function."""

    @pytest.fixture
    def example_paths(self):
        """A set of example paths."""
        return ["test.csv", "nested/test.csv", "deep/nested/test.csv", "test.txt"]

    def test_basic(self, example_paths):
        """Tests basic glob."""
        assert fnmatch.filter(example_paths, "*.csv") == ["test.csv"]

    def test_recursive(self, example_paths):
        """Tests recursive glob."""
        assert fnmatch.filter(example_paths, "**/*.csv") == [
            "nested/test.csv",
            "deep/nested/test.csv",
        ]
