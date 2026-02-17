from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class RawDocument(BaseModel):
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
