"""BNSS Pipeline — ETL pipeline for Bharatiya Nagarik Suraksha Sanhita legal data."""

from .config import Settings, get_settings
from .etl_bnss import parse_crosswalk_bnss_crpc, parse_index_bnss, run_etl_bnss
from .ingest_http import fetch_many, fetch_url
from .validate import ValidationReport, ValidationResult, run_validation

__all__ = [
    "Settings",
    "get_settings",
    "fetch_url",
    "fetch_many",
    "parse_index_bnss",
    "parse_crosswalk_bnss_crpc",
    "run_etl_bnss",
    "run_validation",
    "ValidationReport",
    "ValidationResult",
]
