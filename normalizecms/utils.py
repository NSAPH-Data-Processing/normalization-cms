from __future__ import annotations

import glob
import logging

LOGGER = logging.getLogger(__name__)


def glob_parquet_files(pattern: str) -> list[str]:
    """Glob for parquet files matching pattern; raises FileNotFoundError if none match."""
    matched = glob.glob(pattern)
    if not matched:
        raise FileNotFoundError(f"No files matched pattern: {pattern}")
    return sorted(matched)