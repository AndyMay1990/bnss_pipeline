"""Settings driven by environment variables with the BNSS_ prefix."""

from __future__ import annotations

import logging
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from env vars / .env file."""

    model_config = SettingsConfigDict(
        env_prefix="BNSS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Directories
    project_root: Path = Path(".")
    raw_html_dir: Path = Path("raw_html")
    manifests_dir: Path = Path("manifests")
    datasets_dir: Path = Path("datasets")

    # HTTP behaviour
    user_agent: str = "bnss-pipeline/0.1 (contact: your-email@example.com)"
    accept_language: str = "en-IN,en;q=0.9"
    min_delay_seconds: float = 1.0
    timeout_total: float = 30.0

    # Retry policy
    max_attempts: int = 5
    backoff_multiplier: float = 1.0
    backoff_min: float = 1.0
    backoff_max: float = 30.0

    # Upstream source URLs
    cytrain_index_bnss: str = (
        "https://cytrain.ncrb.gov.in/staticpage/web_pages/IndexBNSS.html"
    )
    cytrain_section_table_bnss: str = (
        "https://cytrain.ncrb.gov.in/staticpage/web_pages/SectionTableBNSS.html"
    )

    def ensure_dirs(self) -> None:
        """Create project directories if they don't exist."""
        for d in (self.raw_html_dir, self.manifests_dir, self.datasets_dir):
            full = self.project_root / d
            full.mkdir(parents=True, exist_ok=True)
            logger.debug("Ensured directory: %s", full)


def get_settings() -> Settings:
    """Return a *Settings* instance with directories created."""
    s = Settings()
    s.ensure_dirs()
    return s
