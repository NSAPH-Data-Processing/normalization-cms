from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import duckdb

from normalizecms.utils import glob_parquet_files


def _describe_parquet(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> dict[str, str]:
    """Return {column_name: column_type} for a parquet file."""
    df = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}') LIMIT 0"
    ).fetchdf()
    return {row["column_name"]: str(row["column_type"]) for _, row in df.iterrows()}


def _qident(name: str) -> str:
    """Quote a SQL identifier safely for DuckDB."""
    return '"' + str(name).replace('"', '""') + '"'


def run_qc(
    table_config: dict,
    output_parquet_path: str,
    year: int,
    qc_dir: Optional[str] = None,
    sample_n: int = 100,
) -> dict:
    """
    Run QC checks on a normalized parquet file and write artifacts to qc_dir.

    table_config must be a plain dict with all OmegaConf interpolations already resolved
    (same dict passed to process_year). Required keys: name, path_pattern, columns.

    Checks performed:
      1. Output file exists
      2. Row count (zero rows is a warning)
      3. Schema before (input) and after (output), written to schema_before/after.json
      4. Null counts per column
      5. Duplicate check:
         - tables with primary_key: checks key uniqueness
         - tables without primary_key: checks full row uniqueness
      6. Sample parquet (first sample_n rows)
      7. Runtime metrics

    Returns a qc dict with status='ok', 'warn', or 'error'.
    Never raises on QC failure — errors are captured in qc['errors'].
    """
    t0 = time.perf_counter()
    started_utc = datetime.now(timezone.utc).isoformat()

    table_name = table_config.get("name", "unknown")

    if qc_dir is None:
        qc_dir = os.path.dirname(output_parquet_path) or "."
    os.makedirs(qc_dir, exist_ok=True)

    qc_json_path       = os.path.join(qc_dir, "qc.json")
    schema_before_path = os.path.join(qc_dir, "schema_before.json")
    schema_after_path  = os.path.join(qc_dir, "schema_after.json")
    sample_path        = os.path.join(qc_dir, "sample.parquet")

    qc: dict[str, Any] = {
        "status":         "ok",
        "table":          table_name,
        "year":           year,
        "started_utc":    started_utc,
        "duration_sec":   None,
        "output_parquet": output_parquet_path,
        "warnings":       [],
        "errors":         [],
    }

    try:
        if not os.path.exists(output_parquet_path):
            raise FileNotFoundError(f"Output parquet not found: {output_parquet_path}")

        conn = duckdb.connect(database=":memory:")

        # Schema before (from first matched input file; path_pattern is already resolved)
        schema_before: dict[str, str] = {}
        try:
            input_files = glob_parquet_files(table_config.get("path_pattern", ""))
            input_first = input_files[0]
            if os.path.exists(input_first):
                schema_before = _describe_parquet(conn, input_first)
        except (FileNotFoundError, ValueError):
            qc["warnings"].append(
                f"Could not locate an input parquet to describe schema_before "
                f"(pattern: {table_config.get('path_pattern', '')})"
            )

        with open(schema_before_path, "w") as f:
            json.dump(schema_before, f, indent=2)

        # Schema after
        schema_after = _describe_parquet(conn, output_parquet_path)
        with open(schema_after_path, "w") as f:
            json.dump(schema_after, f, indent=2)

        qc["n_columns"] = len(schema_after)

        # Row count
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{output_parquet_path}')"
        ).fetchone()[0]
        qc["row_count"] = int(row_count)

        if row_count == 0:
            qc["warnings"].append("Output row_count is 0.")
            qc["status"] = "warn"

        # Null counts per column
        cols = list(schema_after.keys())
        if cols:
            null_exprs = ", ".join(
                f"SUM(CASE WHEN {_qident(c)} IS NULL THEN 1 ELSE 0 END) AS {_qident(c)}"
                for c in cols
            )
            nulls      = conn.execute(
                f"SELECT {null_exprs} FROM read_parquet('{output_parquet_path}')"
            ).fetchdf()
            null_counts = {c: int(nulls.iloc[0][c]) for c in cols}
        else:
            null_counts = {}
        qc["null_counts"] = null_counts

        # Duplicate check
        # Tables with primary_key: check key uniqueness.
        # Tables without primary_key: check full row uniqueness.
        primary_key = table_config.get("primary_key")
        if primary_key:
            pk_cols        = list(primary_key)
            pk_expr        = ", ".join(_qident(c) for c in pk_cols)
            distinct_count = conn.execute(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT DISTINCT {pk_expr} FROM read_parquet('{output_parquet_path}')"
                f")"
            ).fetchone()[0]
            duplicate_count         = int(row_count) - int(distinct_count)
            qc["primary_key"]       = pk_cols
            qc["distinct_pk_count"] = int(distinct_count)
            qc["duplicate_count"]   = duplicate_count

            if duplicate_count > 0:
                qc["warnings"].append(
                    f"Duplicate primary key rows detected: {duplicate_count:,} "
                    f"({duplicate_count / row_count:.2%} of rows)"
                )
                qc["status"] = "warn"

        else:
            distinct_count = conn.execute(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT DISTINCT * FROM read_parquet('{output_parquet_path}')"
                f")"
            ).fetchone()[0]
            duplicate_count          = int(row_count) - int(distinct_count)
            qc["primary_key"]        = None
            qc["distinct_row_count"] = int(distinct_count)
            qc["duplicate_count"]    = duplicate_count

            if duplicate_count > 0:
                qc["warnings"].append(
                    f"Duplicate rows detected: {duplicate_count:,} "
                    f"({duplicate_count / row_count:.2%} of rows)"
                )
                qc["status"] = "warn"

        # Sample
        conn.execute(
            f"COPY (SELECT * FROM read_parquet('{output_parquet_path}') LIMIT {int(sample_n)}) "
            f"TO '{sample_path}' (FORMAT 'parquet')"
        )

        # Column presence check — warn if expected columns are missing
        expected_cols   = [
            c if isinstance(c, str) else list(c.keys())[0]
            for c in table_config.get("columns", [])
        ]
        schema_after_lc = {k.lower() for k in schema_after}
        missing_cols    = [c for c in expected_cols if c.lower() not in schema_after_lc]
        if missing_cols:
            qc["warnings"].append(f"Missing expected output columns: {missing_cols}")
            qc["status"] = "warn"

        conn.close()

    except Exception as e:
        qc["status"] = "error"
        qc["errors"].append(str(e))

    qc["duration_sec"] = round(time.perf_counter() - t0, 3)

    with open(qc_json_path, "w") as f:
        json.dump(qc, f, indent=2)

    return qc