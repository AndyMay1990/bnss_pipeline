"""Tests for bnss_pipeline.models."""

from datetime import datetime, timezone

from bnss_pipeline.models import RawDocument


def _make_doc(**overrides) -> RawDocument:
    defaults = {
        "source_url": "https://example.com/page.html",
        "fetched_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "status": 200,
    }
    defaults.update(overrides)
    return RawDocument(**defaults)


class TestRawDocument:
    def test_is_success_for_200(self):
        doc = _make_doc(status=200)
        assert doc.is_success is True

    def test_is_success_for_304(self):
        doc = _make_doc(status=304)
        assert doc.is_success is True

    def test_is_success_false_for_404(self):
        doc = _make_doc(status=404)
        assert doc.is_success is False

    def test_is_success_false_for_error(self):
        doc = _make_doc(status=-1, error="connection failed")
        assert doc.is_success is False

    def test_effective_hash_prefers_content_hash(self):
        doc = _make_doc(content_hash="abc", cached_content_hash="def")
        assert doc.effective_hash == "abc"

    def test_effective_hash_falls_back_to_cached(self):
        doc = _make_doc(content_hash=None, cached_content_hash="def")
        assert doc.effective_hash == "def"

    def test_effective_hash_none_when_both_empty(self):
        doc = _make_doc(content_hash=None, cached_content_hash=None)
        assert doc.effective_hash is None

    def test_json_roundtrip(self):
        doc = _make_doc(content_hash="abc123")
        data = doc.model_dump(mode="json")
        restored = RawDocument(**data)
        assert restored.source_url == doc.source_url
        assert restored.content_hash == "abc123"
