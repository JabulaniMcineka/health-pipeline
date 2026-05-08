"""
Task 2: Production-Grade Python – Data Cleaning
Processes health_products.txt (metadata header + pipe-delimited data)
into a clean CSV. Idempotent: safe to run multiple times.

Design decisions:
- Programmatically discovers the header/data boundary (no hardcoded line numbers).
- Detects delimiter automatically as a fallback, but expects pipe '|'.
- Writes output atomically (temp file → rename) so partial runs leave no corrupt state.
- Validates schema after parsing so column drift is caught immediately.
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
from io import StringIO

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_INPUT = Path("data_files/health_products.txt")
DEFAULT_OUTPUT = Path("data_files/health_products_clean.csv")

# Columns we expect after cleaning; used for schema validation.
COLUMN_NAMES = ["product_code","product_name","product_category","product_type"]
EXPECTED_COLUMNS = COLUMN_NAMES
REQUIRED_COLUMNS = ["product_code", "product_name"]

DELIMITER = "|"
ENCODING = "utf-8-sig"                    # handles BOM if present

# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _detect_data_start(lines: list[str], delimiter: str) -> int:
    """
    Scan lines to find where the actual tabular data begins.
    Skips lines that don't contain the delimiter (metadata/comments).
    """
    for idx, line in enumerate(lines): # enumerate gives you both the index number AND the line content at the same time.
        stripped = line.strip()
        if not stripped:
            continue
        # Skip lines that look like metadata (no delimiter)
        if delimiter not in stripped:
            log.info("Skipping metadata line %d: %r", idx, stripped[:80])
            continue
        log.info("Data starts at line %d (0-indexed): %r", idx, stripped[:80])
        return idx
    raise ValueError("Could not locate data rows in the file.")


def _extract_metadata(lines: list[str], data_start: int) -> dict[str, str]:
    """Pull key=value pairs from the metadata header for logging/audit."""
    meta: dict[str, str] = {}
    for line in lines[:data_start]:
        stripped = line.strip().lstrip("#").strip()
        if ":" in stripped:
            k, _, v = stripped.partition(":")
            meta[k.strip()] = v.strip()
        elif "=" in stripped:
            k, _, v = stripped.partition("=")
            meta[k.strip()] = v.strip()
    return meta


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply a reproducible sequence of cleaning steps.
    Each step is logged so the transformation is auditable.
    """
    original_rows = len(df)

    # 1. Normalise column names: lowercase, strip, replace spaces/special chars
    df.columns = [
        re.sub(r"[^\w]", "_", col.strip().lower()).strip("_")
        for col in df.columns
    ]
    log.info("Normalised columns: %s", list(df.columns))

    # 2. Strip whitespace from all string values
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    # Standardise product_code to uppercase
    if "product_code" in df.columns:
        df["product_code"] = df["product_code"].str.upper()

    # 3. Replace empty strings and common null sentinels with NaN
    null_sentinels = {"", "N/A", "NA", "n/a", "NULL", "null", "None", "-", "?"}
    df.replace(null_sentinels, pd.NA, inplace=True)

    # 4. Drop rows that are entirely null
    before_drop = len(df)
    df.dropna(how="all", inplace=True)
    log.info("Dropped %d fully-null rows", before_drop - len(df))

    # 5. Deduplicate on the full row (keeps first occurrence)
    before_dedup = len(df)
    df.drop_duplicates(inplace=True)
    log.info("Dropped %d exact-duplicate rows", before_dedup - len(df))

    # 6. Infer better dtypes (numeric columns stored as strings → int/float)
    df = df.infer_objects(copy=False)
    for col in df.columns:
        try:
            converted = pd.to_numeric(df[col], errors="raise")
            df[col] = converted
            log.debug("Converted column '%s' to numeric", col)
        except (ValueError, TypeError):
            pass

    log.info(
        "Cleaning complete: %d rows in → %d rows out (removed %d)",
        original_rows, len(df), original_rows - len(df),
    )
    return df


def _validate_schema(df: pd.DataFrame) -> None:
    """Raise if required columns are missing after cleaning."""
    if REQUIRED_COLUMNS:
        missing = set(REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Required columns missing after cleaning: {missing}")
    if EXPECTED_COLUMNS:
        extra = set(df.columns) - set(EXPECTED_COLUMNS)
        if extra:
            log.warning("Unexpected columns (schema drift?): %s", extra)


def _file_checksum(path: Path) -> str:
    """SHA-256 of a file for idempotency checks."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write(df: pd.DataFrame, output_path: Path) -> None:
    """Write CSV atomically via a temp file to avoid partial outputs."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=output_path.parent, prefix=".tmp_", suffix=".csv"
    )
    try:
        os.close(fd)
        df.to_csv(tmp_path, index=False, quoting=csv.QUOTE_MINIMAL)
        os.replace(tmp_path, output_path)   # atomic on POSIX; best-effort on Windows
        log.info("Written %d rows to %s", len(df), output_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def process(
    input_path: Path = DEFAULT_INPUT,
    output_path: Path = DEFAULT_OUTPUT,
    delimiter: str = DELIMITER,
    encoding: str = ENCODING,
    force: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Convert a pipe-delimited .txt file with a metadata header to a clean CSV.

    Idempotency: if the output already exists and its source checksum matches
    the stored one, the function returns early (no-op) unless force=True.

    Returns the cleaned DataFrame for testing/inspection.
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    checksum_path = output_path.with_suffix(".sha256")

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    current_checksum = _file_checksum(input_path)

    # --- Idempotency gate ---
    if not force and output_path.exists() and checksum_path.exists():
        stored = checksum_path.read_text().strip()
        if stored == current_checksum:
            log.info(
                "Output is up-to-date (checksum match). "
                "Use force=True to re-process. Skipping."
            )
            return pd.read_csv(output_path)
        log.info("Source file changed (checksum mismatch). Re-processing.")

    # --- Read raw lines ---
    with open(input_path, encoding=encoding, errors="replace") as fh:
        raw_lines = fh.readlines()

    log.info("Read %d raw lines from %s", len(raw_lines), input_path)

    # --- Discover structure ---
    data_start = _detect_data_start(raw_lines, delimiter)
    metadata = _extract_metadata(raw_lines, data_start)
    if metadata:
        log.info("Metadata header: %s", metadata)

    # --- Parse tabular section ---
    data_lines = raw_lines[data_start:]   # ← add this line
    df = pd.read_csv(
        StringIO("".join(data_lines)),
        sep=re.escape(delimiter),
        engine="python",
        dtype=str,
        skip_blank_lines=True,
        header=None,           # no header row in file
        names=COLUMN_NAMES,    # assign column names manually
    )

    log.info("Parsed %d rows × %d columns", *df.shape)

    # --- Clean ---
    df = _clean_dataframe(df)

    # --- Validate ---
    _validate_schema(df)

    # --- Write atomically ---
    _atomic_write(df, output_path)

    # --- Store checksum for future idempotency checks ---
    checksum_path.write_text(current_checksum)

    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clean health_products.txt → CSV")
    parser.add_argument("--input",  default=str(DEFAULT_INPUT),  help="Source .txt file")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output .csv file")
    parser.add_argument("--force",  action="store_true",          help="Re-process even if up-to-date")
    args = parser.parse_args()

    try:
        df = process(Path(args.input), Path(args.output), force=args.force)
        print(f"\nSuccess – {len(df)} rows written to {args.output}")
        print(df.head(3).to_string(index=False))
        sys.exit(0)
    except Exception as e:
        log.exception("Processing failed: %s", e)
        sys.exit(1)
