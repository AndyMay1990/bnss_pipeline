from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from bs4 import BeautifulSoup
from pydantic import BaseModel

from .config import get_settings


class BnssSectionIndexRow(BaseModel):
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
    bnss_section_no: str
    bnss_section_title: Optional[str] = None
    crpc_section_no: Optional[str] = None
    crpc_section_title: Optional[str] = None
    remarks: Optional[str] = None
    source_url: str
    content_hash: str
    version: str


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _latest_hash_for(url_cache: Dict[str, Any], url: str) -> str:
    entry = url_cache.get(url)
    if not entry or not entry.get("last_hash"):
        raise ValueError(f"No last_hash in url_cache for url={url}")
    return entry["last_hash"]


def _load_html_by_hash(raw_html_dir: Path, content_hash: str) -> str:
    p = raw_html_dir / f"{content_hash}.html"
    if not p.exists():
        raise FileNotFoundError(f"Missing raw HTML for hash {content_hash}: {p}")
    return p.read_text(encoding="utf-8", errors="replace")


def _write_jsonl(path: Path, rows: Iterable[BaseModel]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(r.model_dump_json())
            f.write("\n")

def _roman_to_int(roman: str) -> int:
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
    try:
        date.fromisoformat(as_of)
    except ValueError as exc:
        raise ValueError("as_of must be in YYYY-MM-DD format") from exc
    return as_of


def canonical_id_bnss(chapter_no: int, section_no: int) -> str:
    return f"BNSS:CH{chapter_no:02d}:S{section_no:03d}"


CHAPTER_RE = re.compile(
    r"\bCHAPTER\s+([IVXLCDM]+)\s+(.+?)(?=\s+\d{1,3}\s*\.|\s+CHAPTER\s+|$)",
    re.IGNORECASE,
)

SECTION_RE = re.compile(
    r"\b(\d{1,3})\s*\.+\s*(.+?)(?=\s+\d{1,3}\s*\.+\s*|\s+CHAPTER\s+|$)",
    re.IGNORECASE,
)

# Crosswalk cell: section number plus optional title. Supports sub-sections like 497(2).
CROSSWALK_CELL_RE = re.compile(
    r"^\s*(\d{1,4}(?:\.\d+)?(?:\s*\(\d+\))?(?:\s*[A-Z])?)\s*\.?\s*(.*)$"
)


def _clean_cell_text(text: str) -> str:
    cleaned = " ".join(text.split())
    cleaned = re.sub(r"\s*\(Change\)\s*", " ", cleaned, flags=re.IGNORECASE)
    return cleaned.strip().rstrip(".")


def _plain_url(u: str) -> str:
    u = (u or "").strip()
    if u.startswith("[") and "](" in u and u.endswith(")"):
        return u.split("](", 1)[1][:-1].strip()
    return u


def _split_section_cell(text: str) -> Tuple[Optional[str], Optional[str]]:
    cleaned = _clean_cell_text(text)
    if not cleaned:
        return None, None
    m = CROSSWALK_CELL_RE.match(cleaned)
    if not m:
        return None, cleaned
    sec_no = m.group(1).strip()
    title = m.group(2).strip()
    return sec_no, title or None


def parse_index_bnss(
    html: str, *, source_url: str, content_hash: str, version: str
) -> List[BnssSectionIndexRow]:
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

    return rows


def _pick_main_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    if not tables:
        raise ValueError("No <table> found in crosswalk HTML.")
    return max(tables, key=lambda t: len(t.find_all("tr")))


def parse_crosswalk_bnss_crpc(
    html: str, *, source_url: str, content_hash: str, version: str
) -> List[CrosswalkRow]:
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
        raise ValueError("parse_crosswalk_bnss_crpc produced 0 rows (HTML structure likely changed).")

    return out


def run_etl_bnss(*, as_of: str) -> Tuple[Path, Path]:
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

    index_html = _load_html_by_hash(s.project_root / s.raw_html_dir, index_hash)
    table_html = _load_html_by_hash(s.project_root / s.raw_html_dir, table_hash)

    sections = parse_index_bnss(index_html, source_url=index_url, content_hash=index_hash, version=version)
    crosswalk = parse_crosswalk_bnss_crpc(table_html, source_url=table_url, content_hash=table_hash, version=version)

    ds_dir = s.project_root / s.datasets_dir
    sections_path = ds_dir / "bnss_sections_index.jsonl"
    crosswalk_path = ds_dir / "bnss_crosswalk.jsonl"

    _write_jsonl(sections_path, sections)
    _write_jsonl(crosswalk_path, crosswalk)

    return sections_path, crosswalk_path








