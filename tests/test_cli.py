"""Unit tests for CLI argument parsing."""

import pytest

from bnss_pipeline.cli import build_parser


class TestBuildParser:
    """Tests for CLI argument parser."""

    def test_fetch_command(self) -> None:
        args = build_parser().parse_args(["fetch"])
        assert args.cmd == "fetch"
        assert args.source == "cytrain"
        assert args.as_of is None

    def test_fetch_with_options(self) -> None:
        args = build_parser().parse_args(["fetch", "--source", "cytrain", "--as-of", "2026-01-15"])
        assert args.source == "cytrain"
        assert args.as_of == "2026-01-15"

    def test_etl_command(self) -> None:
        args = build_parser().parse_args(["etl"])
        assert args.cmd == "etl"
        assert args.as_of is None

    def test_etl_with_as_of(self) -> None:
        args = build_parser().parse_args(["etl", "--as-of", "2026-02-01"])
        assert args.as_of == "2026-02-01"

    def test_all_command(self) -> None:
        args = build_parser().parse_args(["all"])
        assert args.cmd == "all"

    def test_verbose_flag(self) -> None:
        args = build_parser().parse_args(["-v", "fetch"])
        assert args.verbose is True

    def test_no_command_raises(self) -> None:
        with pytest.raises(SystemExit):
            build_parser().parse_args([])

    def test_invalid_source_raises(self) -> None:
        with pytest.raises(SystemExit):
            build_parser().parse_args(["fetch", "--source", "invalid"])
