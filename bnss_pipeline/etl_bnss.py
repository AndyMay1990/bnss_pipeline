"""ETL: parse cached BNSS HTML into structured JSONL datasets."""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from bs4 import BeautifulSoup
from pydantic import BaseModel

from .config import get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Output models
# ---------------------------------------------------------------------------

class BnssSectionIndexRow(BaseModel):
    """One row in the BNSS sections-index dataset."""

    canonical_id: str
    law: str = "BNSS"
    chapter_no: int
    chapter_title: str
    section_no: int
    section_title: str
    source_url: str
    content_hash: str
    version: str


class CrosswalkRow(BaseModel):
    """One row mapping a BNSS section to the old CrPC section."""

    bnss_section_no: str
    bnss_section_title: Optional[str] = None
    crpc_section_no: Optional[str] = None
    crpc_section_title: Optional[str] = None
    remarks: Optional[str] = None
    source_url: str
    content_hash: str
    version: str


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> Dict[str, Any]:
    """Read a JSON file (tolerates UTF-8 BOM)."""
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _latest_hash_for(url_cache: Dict[str, Any], url: str) -> str:
    """Return the most recent content hash for *url* from the URL cache."""
    entry = url_cache.get(url)
    if not entry or not entry.get("last_hash"):
        raise ValueError(f"No last_hash in url_cache for url={url}")
    return entry["last_hash"]


def _load_html_by_hash(raw_html_dir: Path, content_hash: str) -> str:
    """Load cached HTML by its content hash."""
    p = raw_html_dir / f"{content_hash}.html"
    if not p.exists():
        raise FileNotFoundError(f"Missing raw HTML for hash {content_hash}: {p}")
    return p.read_text(encoding="utf-8", errors="replace")


def _write_jsonl_atomic(path: Path, rows: Iterable[BaseModel]) -> int:
    """Write rows to a JSONL file atomically. Returns the row count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    count = 0
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(r.model_dump_json())
            f.write("\n")
            count += 1
    tmp.replace(path)
    return count


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _roman_to_int(roman: str) -> int:
    """Convert a Roman numeral string to an integer."""
    roman = roman.upper().strip()
    vals = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    total = 0
    prev = 0
    for ch in reversed(roman):
        v = vals[ch]
        if v < prev:
            total -= v
        else:
            total += v
            prev = v
    return total


def _validate_as_of(as_of: str) -> str:
    """Validate that *as_of* is in YYYY-MM-DD format."""
    try:
        date.fromisoformat(as_of)
    except ValueError as exc:
        raise ValueError("as_of must be in YYYY-MM-DD format") from exc
    return as_of


def canonical_id_bnss(chapter_no: int, section_no: int) -> str:
    """Build a canonical ID like ``BNSS:CH01:S001``."""
    return f"BNSS:CH{chapter_no:02d}:S{section_no:03d}"


CHAPTER_RE = re.compile(
    r"\bCHAPTER\s+([IVXLCDM]+)\s+(.+?)(?=\s+\d{1,3}\s*\.|\s+CHAPTER\s+|$)",
    re.IGNORECASE,
)

SECTION_RE = re.compile(
    r"\b(\d{1,3})\s*\.+\s*(.+?)(?=\s+\d{1,3}\s*\.+\s*|\s+CHAPTER\s+|$)",
    re.IGNORECASE,
)

CROSSWALK_CELL_RE = re.compile(
    r"^\s*(\d{1,4}(?:\.\d+)?(?:\s*\(\d+\))?(?:\s*[A-Z])?)\s*\.?\s*(.*)$"
)


def _clean_cell_text(text: str) -> str:
    """Collapse whitespace and strip artefacts from a table-cell string."""
    cleaned = " ".join(text.split())
    cleaned = re.sub(r"\s*\(Change\)\s*", " ", cleaned, flags=re.IGNORECASE)
    return cleaned.strip().rstrip(".")


def _plain_url(u: str) -> str:
    """Unwrap a Markdown link ``[text](href)`` to just *href*."""
    u = (u or "").strip()
    if u.startswith("[") and "](" in u and u.endswith(")"):
        return u.split("](", 1)[1][:-1].strip()
    return u


def _split_section_cell(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Split a crosswalk table cell into (section_no, section_title)."""
    cleaned = _clean_cell_text(text)
    if not cleaned:
        return None, None
    m = CROSSWALK_CELL_RE.match(cleaned)
    if not m:
        return None, cleaned
    sec_no = m.group(1).strip()
    title = m.group(2).strip()
    return sec_no, title or None


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_index_bnss(
    html: str, *, source_url: str, content_hash: str, version: str
) -> List[BnssSectionIndexRow]:
    """Parse the BNSS index HTML into a list of section-index rows."""
    soup = BeautifulSoup(html, "lxml")
    source_url = _plain_url(source_url)

    text = soup.get_text(" ", strip=True)
    text = " ".join(text.split())

    chapters: List[Tuple[int, int, int, str]] = []
    for m in CHAPTER_RE.finditer(text):
        ch_no = _roman_to_int(m.group(1))
        ch_title = m.group(2).strip()
        chapters.append((m.start(), m.end(), ch_no, ch_title))

    if not chapters:
        raise ValueError("No CHAPTER headings found in IndexBNSS text (HTML changed).")

    chapters.sort(key=lambda x: x[0])
    logger.info("Found %d chapters in index HTML", len(chapters))

    rows: List[BnssSectionIndexRow] = []
    for i, (_, end, ch_no, ch_title) in enumerate(chapters):
        next_start = chapters[i + 1][0] if i + 1 < len(chapters) else len(text)
        chunk = text[end:next_start]

        for sm in SECTION_RE.finditer(chunk):
            sec_no = int(sm.group(1))
            title = sm.group(2).strip()
            title = _clean_cell_text(title)
            if not title or len(title) < 3:
                continue

            rows.append(
                BnssSectionIndexRow(
                    canonical_id=canonical_id_bnss(ch_no, sec_no),
                    chapter_no=ch_no,
                    chapter_title=ch_title,
                    section_no=sec_no,
                    section_title=title,
                    source_url=source_url,
                    content_hash=content_hash,
                    version=version,
                )
            )

    if not rows:
        raise ValueError("parse_index_bnss produced 0 rows (section pattern not found).")

    logger.info("Parsed %d section-index rows", len(rows))
    return rows


def _pick_main_table(soup: BeautifulSoup):
    """Select the table with the most rows from the soup."""
    tables = soup.find_all("table")
    if not tables:
        raise ValueError("No <table> found in crosswalk HTML.")
    return max(tables, key=lambda t: len(t.find_all("tr")))


def parse_crosswalk_bnss_crpc(
    html: str, *, source_url: str, content_hash: str, version: str
) -> List[CrosswalkRow]:
    """Parse the BNSS↔CrPC crosswalk HTML table."""
    soup = BeautifulSoup(html, "lxml")
    source_url = _plain_url(source_url)
    table = _pick_main_table(soup)
    trs = table.find_all("tr")

    out: List[CrosswalkRow] = []
    for tr in trs:
        tds = tr.find_all("td")
        if not tds:
            continue

        cells = [td.get_text(" ", strip=True) for td in tds]
        if len(cells) < 2:
            continue

        bnss_no, bnss_title = _split_section_cell(cells[0])
        crpc_no, crpc_title = _split_section_cell(cells[1])
        remarks = _clean_cell_text(" ".join(cells[2:])) if len(cells) > 2 else None

        if bnss_no is None:
            continue

        out.append(
            CrosswalkRow(
                bnss_section_no=bnss_no,
                bnss_section_title=bnss_title or None,
                crpc_section_no=crpc_no or None,
                crpc_section_title=crpc_title or None,
                remarks=remarks or None,
                source_url=source_url,
                content_hash=content_hash,
                version=version,
            )
        )

    if not out:
        raise ValueError(
            "parse_crosswalk_bnss_crpc produced 0 rows (HTML structure likely changed)."
        )

    logger.info("Parsed %d crosswalk rows", len(out))
    return out


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_etl_bnss(*, as_of: str) -> Tuple[Path, Path]:
    """Run the full ETL: read cached HTML → parse → write JSONL datasets.

    Returns ``(sections_path, crosswalk_path)``.
    """
    s = get_settings()
    as_of = _validate_as_of(as_of)
    version = f"bnss@{as_of}"

    url_cache_path = s.project_root / s.manifests_dir / "url_cache.json"
    if not url_cache_path.exists():
        raise FileNotFoundError(
            f"Missing {url_cache_path}. Run the fetch step before ETL."
        )
    url_cache = _read_json(url_cache_path)

    index_url = s.cytrain_index_bnss
    table_url = s.cytrain_section_table_bnss

    index_hash = _latest_hash_for(url_cache, index_url)
    table_hash = _latest_hash_for(url_cache, table_url)

    logger.info("Loading index HTML (hash=%s)", index_hash[:12])
    index_html = _load_html_by_hash(s.project_root / s.raw_html_dir, index_hash)

    logger.info("Loading crosswalk HTML (hash=%s)", table_hash[:12])
    table_html = _load_html_by_hash(s.project_root / s.raw_html_dir, table_hash)

    sections = parse_index_bnss(
        index_html, source_url=index_url, content_hash=index_hash, version=version
    )
    crosswalk = parse_crosswalk_bnss_crpc(
        table_html, source_url=table_url, content_hash=table_hash, version=version
    )

    ds_dir = s.project_root / s.datasets_dir
    sections_path = ds_dir / "bnss_sections_index.jsonl"
    crosswalk_path = ds_dir / "bnss_crosswalk.jsonl"

    n_sec = _write_jsonl_atomic(sections_path, sections)
    n_cw = _write_jsonl_atomic(crosswalk_path, crosswalk)
    logger.info("Wrote %d sections → %s", n_sec, sections_path)
    logger.info("Wrote %d crosswalk rows → %s", n_cw, crosswalk_path)

    return sections_path, crosswalk_path
