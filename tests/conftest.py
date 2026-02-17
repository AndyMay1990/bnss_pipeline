"""Shared test fixtures for BNSS Pipeline tests."""

import pytest


@pytest.fixture
def sample_index_html() -> str:
    """Minimal BNSS index HTML with 2 chapters and 3 sections."""
    return """
    <html><body>
    <div>
        CHAPTER I PRELIMINARY
        1. Short title, commencement and application.
        2. Definitions.
        CHAPTER II CONSTITUTION OF CRIMINAL COURTS AND OFFICES
        3. Classes of Criminal Courts.
    </div>
    </body></html>
    """


@pytest.fixture
def sample_crosswalk_html() -> str:
    """Minimal crosswalk HTML with a table mapping BNSS to CrPC sections."""
    return """
    <html><body>
    <table>
        <tr><th>BNSS</th><th>CrPC</th><th>Remarks</th></tr>
        <tr><td>1. Short title</td><td>1. Short title</td><td>No change</td></tr>
        <tr><td>2. Definitions</td><td>2. Definitions</td><td>Modified</td></tr>
        <tr><td>3. Classes of Criminal Courts</td><td>6. Classes of Criminal Courts</td><td>Renumbered</td></tr>
    </table>
    </body></html>
    """


@pytest.fixture
def empty_html() -> str:
    """Empty HTML document."""
    return "<html><body></body></html>"


@pytest.fixture
def no_table_html() -> str:
    """HTML with no table element."""
    return "<html><body><p>No tables here.</p></body></html>"


@pytest.fixture
def crosswalk_empty_rows_html() -> str:
    """Crosswalk HTML where all rows have empty cells."""
    return """
    <html><body>
    <table>
        <tr><th>BNSS</th><th>CrPC</th></tr>
        <tr><td></td><td></td></tr>
        <tr><td>  </td><td>  </td></tr>
    </table>
    </body></html>
    """
