"""Pydantic models shared across the pipeline."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class RawDocument(BaseModel):
    """Represents the result of fetching a single URL."""

    source_url: str
    fetched_at: datetime
    status: int
    headers: Dict[str, Any] = Field(default_factory=dict)
    as_of: Optional[str] = None

    content_hash: Optional[str] = None
    raw_html_path: Optional[str] = None
    raw_meta_path: Optional[str] = None

    etag: Optional[str] = None
    last_modified: Optional[str] = None
    cached_content_hash: Optional[str] = None

    error: Optional[str] = None

    @property
    def is_success(self) -> bool:
        """Return True when the fetch was successful (2xx or 304)."""
        return self.status < 400

    @property
    def effective_hash(self) -> Optional[str]:
        """Return whichever content hash is available."""
        return self.content_hash or self.cached_content_hash
