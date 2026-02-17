"""Unit tests for data models."""

from datetime import datetime, timezone

from bnss_pipeline.models import RawDocument


class TestRawDocument:
    """Tests for RawDocument model."""

    def test_minimal_creation(self) -> None:
        doc = RawDocument(
            source_url="https://example.com",
            fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            status=200,
        )
        assert doc.source_url == "https://example.com"
        assert doc.status == 200
        assert doc.error is None
        assert doc.content_hash is None

    def test_full_creation(self) -> None:
        doc = RawDocument(
            source_url="https://example.com",
            fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            status=200,
            content_hash="abc123",
            etag='"etag-value"',
            last_modified="Mon, 01 Jan 2026 00:00:00 GMT",
            as_of="2026-01-01",
            raw_html_path="raw_html/abc123.html",
            raw_meta_path="raw_html/abc123.json",
        )
        assert doc.content_hash == "abc123"
        assert doc.etag == '"etag-value"'
        assert doc.as_of == "2026-01-01"

    def test_error_document(self) -> None:
        doc = RawDocument(
            source_url="https://example.com/bad",
            fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            status=500,
            error="HTTP 500 for https://example.com/bad",
        )
        assert doc.error is not None
        assert doc.status == 500

    def test_serialization_roundtrip(self) -> None:
        doc = RawDocument(
            source_url="https://example.com",
            fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            status=200,
            content_hash="abc123",
        )
        json_str = doc.model_dump_json()
        restored = RawDocument.model_validate_json(json_str)
        assert restored.source_url == doc.source_url
        assert restored.content_hash == doc.content_hash

    def test_defaults(self) -> None:
        doc = RawDocument(
            source_url="https://example.com",
            fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            status=200,
        )
        assert doc.headers == {}
        assert doc.as_of is None
        assert doc.cached_content_hash is None
