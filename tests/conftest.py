"""Shared test fixtures."""

import pytest
from pathlib import Path


@pytest.fixture
def tmp_project(tmp_path: Path):
    """Create a minimal project directory tree for testing."""
    for d in ("raw_html", "manifests", "datasets"):
        (tmp_path / d).mkdir()
    return tmp_path
