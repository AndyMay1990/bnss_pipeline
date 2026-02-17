"""Unit tests for configuration."""

import os
from pathlib import Path
from unittest.mock import patch

from bnss_pipeline.config import Settings


class TestSettings:
    """Tests for Settings."""

    def test_defaults(self) -> None:
        s = Settings()
        assert s.project_root == Path(".")
        assert s.raw_html_dir == Path("raw_html")
        assert s.manifests_dir == Path("manifests")
        assert s.datasets_dir == Path("datasets")
        assert s.min_delay_seconds == 1.0
        assert s.timeout_total == 30.0
        assert s.max_attempts == 5

    def test_env_prefix(self) -> None:
        with patch.dict(os.environ, {"BNSS_TIMEOUT_TOTAL": "60.0"}):
            s = Settings()
            assert s.timeout_total == 60.0

    def test_source_urls_present(self) -> None:
        s = Settings()
        assert "cytrain.ncrb.gov.in" in s.cytrain_index_bnss
        assert "cytrain.ncrb.gov.in" in s.cytrain_section_table_bnss

    def test_ensure_dirs(self, tmp_path: Path) -> None:
        s = Settings(
            project_root=tmp_path,
            raw_html_dir=Path("test_raw"),
            manifests_dir=Path("test_manifests"),
            datasets_dir=Path("test_datasets"),
        )
        s.ensure_dirs()
        assert (tmp_path / "test_raw").is_dir()
        assert (tmp_path / "test_manifests").is_dir()
        assert (tmp_path / "test_datasets").is_dir()

    def test_user_agent_default(self) -> None:
        s = Settings()
        assert "bnss-pipeline" in s.user_agent
