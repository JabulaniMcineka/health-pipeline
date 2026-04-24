"""
Task 2: Production-Grade Python – Data Cleaning
Processes health_products.txt (metadata header + pipe-delimited data)
into a clean CSV. Idempotent: safe to run multiple times.
"""

import csv
import hashlib
import logging
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_INPUT = Path("data_files/health_products.txt")
DEFAULT_OUTPUT = Path("data_files/health_products_clean.csv")

COLUMN_NAMES: list[str] = ["product_code", "product_name", "tier", "status"]
REQUIRED_COLUMNS: list[str] = ["product_code", "product_name", "tier", "status"]
DELIMITER = "|"
ENCODING = "utf-8-sig"


def _detect_data_start(lines: list[str], delimiter: str) -> int:
    """Skip lines without the delimiter (metadata). First line with delimiter = data."""
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        if delimiter not in stripped:
            log.info("Skipping metadata line %d: %r", idx, stripped[:80])
            continue
        log.info("Data starts at line %d (0-indexed): %r", idx, stripped[:80])
        return idx
    raise ValueError("Could not locate data rows in the file.")


def _extract_metadata(lines: list[str], data_start: int) -> dict[str, str]:
    meta: dict[str, str] = {}
    for line in lines[:data_start]:
        stripped = line.strip().lstrip("#").strip()
        if ":" in stripped:
            k, _, v = stripped.partition(":")
            meta[k.strip()] = v.strip()
    return meta


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    original_rows = len(df)

    # Strip whitespace from all string values
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    # Standardise product_code to uppercase
    if "product_code" in df.columns:
        df["product_code"] = df["product_code"].str.upper()

    # Replace null sentinels
    null_sentinels = {"", "N/A", "NA", "n/a", "NULL", "null", "None", "-", "?"}
    df.replace(null_sentinels, pd.NA, inplace=True)

    # Drop fully null rows
    before_drop = len(df)
    df.dropna(how="all", inplace=True)
    log.info("Dropped %d fully-null rows", before_drop - len(df))

    # Deduplicate
    before_dedup = len(df)
    df.drop_duplicates(inplace=True)
    log.info("Dropped %d exact-duplicate rows", before_dedup - len(df))

    log.info("Cleaning complete: %d rows in → %d rows out", original_rows, len(df))
    return df


def _file_checksum(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=output_path.parent, prefix=".tmp_", suffix=".csv")
    try:
        os.close(fd)
        df.to_csv(tmp_path, index=False, quoting=csv.QUOTE_MINIMAL)
        os.replace(tmp_path, output_path)
        log.info("Written %d rows to %s", len(df), output_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def process(
    input_path: Path = DEFAULT_INPUT,
    output_path: Path = DEFAULT_OUTPUT,
    delimiter: str = DELIMITER,
    encoding: str = ENCODING,
    force: bool = False,
) -> Optional[pd.DataFrame]:
    input_path = Path(input_path)
    output_path = Path(output_path)
    checksum_path = output_path.with_suffix(".sha256")

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    current_checksum = _file_checksum(input_path)

    if not force and output_path.exists() and checksum_path.exists():
        stored = checksum_path.read_text().strip()
        if stored == current_checksum:
            log.info("Output is up-to-date (checksum match). Use force=True to re-process. Skipping.")
            return pd.read_csv(output_path)
        log.info("Source file changed (checksum mismatch). Re-processing.")

    with open(input_path, encoding=encoding, errors="replace") as fh:
        raw_lines = fh.readlines()

    log.info("Read %d raw lines from %s", len(raw_lines), input_path)

    data_start = _detect_data_start(raw_lines, delimiter)
    metadata = _extract_metadata(raw_lines, data_start)
    if metadata:
        log.info("Metadata header: %s", metadata)

    data_lines = raw_lines[data_start:]

    df = pd.read_csv(
        pd.io.common.StringIO("".join(data_lines)),
        sep=re.escape(delimiter),
        engine="python",
        dtype=str,
        skip_blank_lines=True,
        header=None,
        names=COLUMN_NAMES,
    )

    log.info("Parsed %d rows × %d columns", *df.shape)

    df = _clean_dataframe(df)

    # Validate required columns
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Required columns missing: {missing}")

    _atomic_write(df, output_path)
    checksum_path.write_text(current_checksum)

    return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Clean health_products.txt → CSV")
    parser.add_argument("--input",  default=str(DEFAULT_INPUT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--force",  action="store_true")
    args = parser.parse_args()

    try:
        df = process(Path(args.input), Path(args.output), force=args.force)
        print(f"\nSuccess – {len(df)} rows written to {args.output}")
        print(df.to_string(index=False))
        sys.exit(0)
    except Exception as e:
        log.exception("Processing failed: %s", e)
        sys.exit(1)