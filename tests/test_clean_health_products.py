"""
tests/test_clean_health_products.py
Unit tests for scripts/clean_health_products.py.
Uses pytest + tmp_path fixture – no real files required.
"""

import textwrap
from pathlib import Path

import pandas as pd
import pytest

# Adjust import path so tests run from repo root
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from clean_health_products import (
    _clean_dataframe,
    _detect_data_start,
    _extract_metadata,
    process,
)


# ---------------------------------------------------------------------------
# _detect_data_start
# ---------------------------------------------------------------------------

class TestDetectDataStart:
    def test_finds_header_after_metadata(self):
        lines = [
            "# Source: ALIGND Health\n",
            "# Version: 1.0\n",
            "\n",
            "product_code|product_name|category\n",
            "P001|Basic Cover|Hospital\n",
        ]
        assert _detect_data_start(lines, "|") == 3

    def test_no_metadata_first_line_is_header(self):
        lines = [
            "code|name|type\n",
            "A1|Product A|HMO\n",
        ]
        assert _detect_data_start(lines, "|") == 0

    def test_raises_when_no_header_found(self):
        lines = ["# just a comment\n", "\n", "no delimiter here\n"]
        with pytest.raises(ValueError, match="Could not locate"):
            _detect_data_start(lines, "|")

    def test_skips_blank_lines(self):
        lines = ["\n", "\n", "a|b|c\n", "1|2|3\n"]
        assert _detect_data_start(lines, "|") == 2


# ---------------------------------------------------------------------------
# _extract_metadata
# ---------------------------------------------------------------------------

class TestExtractMetadata:
    def test_colon_separated(self):
        lines = ["# Source: ALIGND\n", "# Date: 2024-01-01\n", "col1|col2\n"]
        meta = _extract_metadata(lines, data_start=2)
        assert meta["Source"] == "ALIGND"
        assert meta["Date"] == "2024-01-01"

    def test_equals_separated(self):
        lines = ["version=2.5\n", "col1|col2\n"]
        meta = _extract_metadata(lines, data_start=1)
        assert meta["version"] == "2.5"

    def test_empty_header(self):
        lines = ["col1|col2\n"]
        assert _extract_metadata(lines, data_start=0) == {}


# ---------------------------------------------------------------------------
# _clean_dataframe
# ---------------------------------------------------------------------------

class TestCleanDataframe:
    def _make_df(self, data: dict) -> pd.DataFrame:
        return pd.DataFrame(data)

    def test_strips_whitespace_from_strings(self):
        df = self._make_df({"name": ["  Alice  ", " Bob"], "age": ["30 ", "25"]})
        result = _clean_dataframe(df)
        assert list(result["name"]) == ["Alice", "Bob"]

    def test_replaces_null_sentinels(self):
        df = self._make_df({"val": ["N/A", "NULL", "n/a", "-", "?", "real"]})
        result = _clean_dataframe(df)
        assert result["val"].isna().sum() == 5
        assert result["val"].dropna().iloc[0] == "real"

    def test_drops_all_null_rows(self):
        df = self._make_df({"a": ["1", None], "b": ["x", None]})
        result = _clean_dataframe(df)
        assert len(result) == 1

    def test_drops_exact_duplicates(self):
        df = self._make_df({"a": ["1", "1", "2"], "b": ["x", "x", "y"]})
        result = _clean_dataframe(df)
        assert len(result) == 2

    def test_normalises_column_names(self):
        df = self._make_df({"  Product Name ": ["a"], "PRICE ($)": ["10"]})
        result = _clean_dataframe(df)
        assert "product_name" in result.columns
        assert "price" in result.columns or "price_" in result.columns

    def test_numeric_columns_inferred(self):
        df = self._make_df({"id": ["1", "2", "3"], "amount": ["100.5", "200.0", "300.25"]})
        result = _clean_dataframe(df)
        assert pd.api.types.is_numeric_dtype(result["amount"])

    def test_idempotent_double_run(self):
        """Running _clean_dataframe twice should produce the same result."""
        df = self._make_df({"name": ["  Alice  ", " Bob "], "age": ["30", "25"]})
        first  = _clean_dataframe(df.copy())
        second = _clean_dataframe(first.copy())
        pd.testing.assert_frame_equal(first.reset_index(drop=True),
                                      second.reset_index(drop=True))


# ---------------------------------------------------------------------------
# process() – integration tests using tmp_path
# ---------------------------------------------------------------------------

class TestProcess:
    def _write_txt(self, path: Path, content: str) -> None:
        path.write_text(textwrap.dedent(content), encoding="utf-8")

    def test_full_pipeline(self, tmp_path):
        src = tmp_path / "products.txt"
        out = tmp_path / "products_clean.csv"
        self._write_txt(src, """\
            # Source: Test
            # Version: 1.0
            product_code|product_name|category|price
            P001|Basic Cover|Hospital|500
            P002|Extended Cover|Dental|750
        """)
        df = process(src, out)
        assert out.exists()
        assert len(df) == 2
        assert "product_code" in df.columns

    def test_idempotent_same_checksum(self, tmp_path):
        src = tmp_path / "products.txt"
        out = tmp_path / "products_clean.csv"
        self._write_txt(src, """\
            code|name
            A|Alpha
            B|Beta
        """)
        df1 = process(src, out)
        df2 = process(src, out)   # second run – should be a no-op
        pd.testing.assert_frame_equal(df1.reset_index(drop=True),
                                      df2.reset_index(drop=True))

    def test_force_reprocesses(self, tmp_path):
        src = tmp_path / "products.txt"
        out = tmp_path / "products_clean.csv"
        self._write_txt(src, "code|name\nA|Alpha\n")
        process(src, out)
        # Modify the source
        self._write_txt(src, "code|name\nA|Alpha\nB|Beta\n")
        df = process(src, out, force=True)
        assert len(df) == 2

    def test_raises_on_missing_input(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            process(tmp_path / "nonexistent.txt", tmp_path / "out.csv")

    def test_handles_empty_strings_as_null(self, tmp_path):
        src = tmp_path / "products.txt"
        out = tmp_path / "products_clean.csv"
        self._write_txt(src, "code|name|category\nA|Alpha|\nB||Hospital\n")
        df = process(src, out)
        assert df["category"].isna().sum() >= 1

    def test_atomic_write_no_partial_file(self, tmp_path, monkeypatch):
        """If to_csv raises, no output file should be left behind."""
        src = tmp_path / "products.txt"
        out = tmp_path / "products_clean.csv"
        self._write_txt(src, "code|name\nA|Alpha\n")

        import clean_health_products as mod

        original = pd.DataFrame.to_csv
        def _failing_to_csv(self, *args, **kwargs):
            raise RuntimeError("Simulated disk failure")

        monkeypatch.setattr(pd.DataFrame, "to_csv", _failing_to_csv)
        with pytest.raises(RuntimeError):
            process(src, out)

        assert not out.exists(), "Partial output file must not exist after failure"
        monkeypatch.setattr(pd.DataFrame, "to_csv", original)
