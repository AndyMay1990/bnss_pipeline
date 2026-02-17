"""Tests for bnss_pipeline.config."""

from pathlib import Path

from bnss_pipeline.config import Settings


class TestSettings:
    def test_defaults(self):
        s = Settings()
        assert s.max_attempts == 5
        assert s.min_delay_seconds == 1.0
        assert s.timeout_total == 30.0
        assert "cytrain.ncrb.gov.in" in s.cytrain_index_bnss

    def test_ensure_dirs_creates_directories(self, tmp_path: Path):
        s = Settings(project_root=tmp_path)
        s.ensure_dirs()
        assert (tmp_path / "raw_html").is_dir()
        assert (tmp_path / "manifests").is_dir()
        assert (tmp_path / "datasets").is_dir()

    def test_env_prefix(self):
        """Verify that the env prefix is set correctly."""
        assert Settings.model_config["env_prefix"] == "BNSS_"
