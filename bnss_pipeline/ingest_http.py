from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import Settings, get_settings
from .models import RawDocument


URL_CACHE_NAME = "url_cache.json"
RETRY_STATUS = {408, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class CacheEntry:
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    last_hash: Optional[str] = None
    last_seen_at: Optional[str] = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _load_url_cache(manifests_dir: Path) -> Dict[str, CacheEntry]:
    p = manifests_dir / URL_CACHE_NAME
    raw = _read_json(p)
    out: Dict[str, CacheEntry] = {}
    for url, entry in raw.items():
        out[url] = CacheEntry(
            etag=entry.get("etag"),
            last_modified=entry.get("last_modified"),
            last_hash=entry.get("last_hash"),
            last_seen_at=entry.get("last_seen_at"),
        )
    return out


def _save_url_cache(manifests_dir: Path, cache: Dict[str, CacheEntry]) -> None:
    p = manifests_dir / URL_CACHE_NAME
    payload = {
        url: {
            "etag": ce.etag,
            "last_modified": ce.last_modified,
            "last_hash": ce.last_hash,
            "last_seen_at": ce.last_seen_at,
        }
        for url, ce in cache.items()
    }
    _write_json_atomic(p, payload)


def _normalize_headers(headers: httpx.Headers) -> Dict[str, str]:
    return {k.lower(): v for k, v in headers.items()}


def _raise_for_retryable_status(resp: httpx.Response) -> None:
    if resp.status_code in RETRY_STATUS:
        raise httpx.HTTPStatusError(
            f"Retryable HTTP status {resp.status_code} for {resp.request.url}",
            request=resp.request,
            response=resp,
        )


def _build_conditional_headers(cache_entry: Optional[CacheEntry]) -> Dict[str, str]:
    h: Dict[str, str] = {}
    # Conditional GET using If-None-Match (ETag). [web:55]
    if cache_entry and cache_entry.etag:
        h["if-none-match"] = cache_entry.etag
    if cache_entry and cache_entry.last_modified:
        h["if-modified-since"] = cache_entry.last_modified
    return h


def _client(settings: Settings) -> httpx.Client:
    # Explicit timeouts to avoid hanging requests. [web:73]
    return httpx.Client(
        timeout=settings.timeout_total,
        follow_redirects=True,
        headers={
            "user-agent": settings.user_agent,
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": settings.accept_language,
        },
    )


def _retry_decorator(settings: Settings):
    # Exponential backoff retry. [web:94]
    return retry(
        stop=stop_after_attempt(settings.max_attempts),
        wait=wait_exponential(
            multiplier=settings.backoff_multiplier,
            min=settings.backoff_min,
            max=settings.backoff_max,
        ),
        reraise=True,
    )


def fetch_url(url: str, *, settings: Optional[Settings] = None, as_of: Optional[str] = None) -> RawDocument:
    s = settings or get_settings()
    s.ensure_dirs()

    raw_dir = s.project_root / s.raw_html_dir
    manifests_dir = s.project_root / s.manifests_dir

    time.sleep(max(0.0, s.min_delay_seconds))

    url_cache = _load_url_cache(manifests_dir)
    ce = url_cache.get(url)
    cond_headers = _build_conditional_headers(ce)

    @_retry_decorator(s)
    def _do_request() -> httpx.Response:
        with _client(s) as client:
            resp = client.get(url, headers=cond_headers)
            _raise_for_retryable_status(resp)
            return resp

    fetched_at = _utc_now()
    resp = _do_request()
    headers = _normalize_headers(resp.headers)

    etag = headers.get("etag")
    last_modified = headers.get("last-modified")

    # 304 Not Modified is success. [web:52]
    if resp.status_code == 304:
        if not ce or not ce.last_hash:
            raise RuntimeError(f"Received 304 for {url} but no cached content hash exists.")
        url_cache[url] = CacheEntry(
            etag=etag or (ce.etag if ce else None),
            last_modified=last_modified or (ce.last_modified if ce else None),
            last_hash=ce.last_hash if ce else None,
            last_seen_at=fetched_at.isoformat(),
        )
        _save_url_cache(manifests_dir, url_cache)

        doc = RawDocument(
            source_url=url,
            fetched_at=fetched_at,
            status=304,
            headers=headers,
            as_of=as_of,
            etag=url_cache[url].etag,
            last_modified=url_cache[url].last_modified,
            cached_content_hash=url_cache[url].last_hash,
        )
        mf = manifests_dir / f"fetch_{_safe_ts(fetched_at)}.json"
        _write_json_atomic(mf, doc.model_dump(mode="json"))
        return doc

    body = resp.content
    content_hash = _sha256_bytes(body)

    html_path = raw_dir / f"{content_hash}.html"
    meta_path = raw_dir / f"{content_hash}.json"

    if not html_path.exists():
        html_path.write_bytes(body)

    if not meta_path.exists():
        meta_payload = {
            "source_url": url,
            "fetched_at": fetched_at.isoformat(),
            "status": resp.status_code,
            "headers": headers,
            "content_hash": content_hash,
            "raw_html_path": str(html_path).replace("\\", "/"),
        }
        _write_json_atomic(meta_path, meta_payload)

    if resp.status_code >= 400:
        doc = RawDocument(
            source_url=url,
            fetched_at=fetched_at,
            status=resp.status_code,
            headers=headers,
            as_of=as_of,
            content_hash=content_hash,
            raw_html_path=str(html_path).replace("\\", "/"),
            raw_meta_path=str(meta_path).replace("\\", "/"),
            etag=etag,
            last_modified=last_modified,
            error=f"HTTP {resp.status_code} for {url}",
        )
        mf = manifests_dir / f"fetch_{_safe_ts(fetched_at)}.json"
        _write_json_atomic(mf, doc.model_dump(mode="json"))
        raise httpx.HTTPStatusError(
            f"HTTP {resp.status_code} for {url}",
            request=resp.request,
            response=resp,
        )

    url_cache[url] = CacheEntry(
        etag=etag,
        last_modified=last_modified,
        last_hash=content_hash,
        last_seen_at=fetched_at.isoformat(),
    )
    _save_url_cache(manifests_dir, url_cache)

    doc = RawDocument(
        source_url=url,
        fetched_at=fetched_at,
        status=resp.status_code,
        headers=headers,
        as_of=as_of,
        content_hash=content_hash,
        raw_html_path=str(html_path).replace("\\", "/"),
        raw_meta_path=str(meta_path).replace("\\", "/"),
        etag=etag,
        last_modified=last_modified,
    )
    mf = manifests_dir / f"fetch_{_safe_ts(fetched_at)}.json"
    _write_json_atomic(mf, doc.model_dump(mode="json"))
    return doc


def fetch_many(urls: list[str], *, settings: Optional[Settings] = None, as_of: Optional[str] = None) -> list[RawDocument]:
    return [fetch_url(u, settings=settings, as_of=as_of) for u in urls]
