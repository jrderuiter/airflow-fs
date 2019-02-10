from os import path

import pytest


@pytest.fixture(scope="session")
def mock_data_dir():
    """Directory containing test data files."""
    return path.join(path.dirname(__file__), "data")
