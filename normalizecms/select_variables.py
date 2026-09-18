from __future__ import annotations

import os
import re
import time
import logging

import duckdb

from normalizecms.utils import glob_parquet_files

LOGGER = logging.getLogger(__name__)

# Stage 1 of the CMS normalization pipeline.
# Processes one table for a single year — designed to be run in parallel across years via SLURM.
# Reads raw parquet files, selects columns defined in the per-table YAML config,
# and optionally explodes monthly array columns into long format (one row per period).
# Paths (input glob, output dir) are resolved by Hydra/OmegaConf before this module is called.


def build_select_expressions(config: dict) -> list[str]:
    # Columns are bare strings — selected as-is, types inferred from parquet schema.
    expressions = []
    for col_def in config["columns"]:
        if not isinstance(col_def, str):
            raise ValueError(f"Column definition must be a bare string, got: {col_def}")
        expressions.append(col_def)
    return expressions


def load_parquet_files(con: duckdb.DuckDBPyConnection, parquet_files: list[str]) -> None:
    # Load all matched parquet files into DuckDB table 'raw' in a single parallel scan.
    parquet_expr = "ARRAY[" + ", ".join(f"'{f}'" for f in parquet_files) + "]"
    con.execute(f"""
        CREATE OR REPLACE TABLE raw AS
        SELECT *
        FROM read_parquet({parquet_expr}, filename=true)
    """)
    row_count = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    LOGGER.info(f"Loaded {len(parquet_files)} file(s) -> {row_count:,} rows")


def build_working_table(con: duckdb.DuckDBPyConnection, config: dict) -> None:
    # Apply column selection to 'raw', write result to 'working'.
    expressions = build_select_expressions(config)
    expressions_sql = ",\n            ".join(expressions)
    con.execute(f"""
        CREATE OR REPLACE TABLE working AS
        SELECT
            {expressions_sql},
            filename
        FROM raw
    """)
    LOGGER.info(f"Selected {len(expressions)} column(s)")


def explode_arrays(con: duckdb.DuckDBPyConnection, period_col: str = "month", n_periods: int = 12) -> None:
    # Explode array-typed columns in 'working' from wide to long format.
    cols = con.execute("PRAGMA table_info('working')").fetchall()
    static_cols = [col[1] for col in cols if not col[2].endswith("[]")]
    array_cols  = [col[1] for col in cols if col[2].endswith("[]")]

    if not array_cols:
        raise ValueError(
            "'explode_arrays' is set to true but no array-typed columns were found in 'working'. "
            "Check that the source parquet columns for this table return array types."
        )

    LOGGER.info(f"Static columns : {static_cols}")
    LOGGER.info(f"Array columns  : {array_cols}")

    # Index into each array using the cross-joined period value (1-indexed).
    period_values   = ", ".join(f"({i})" for i in range(1, n_periods + 1))
    select_static   = ",\n            ".join(static_cols)
    select_exploded = ",\n            ".join(f"{col}[{period_col}] AS {col}" for col in array_cols)

    con.execute(f"""
        CREATE OR REPLACE TABLE exploded AS
        SELECT
            {select_static},
            {period_col},
            {select_exploded}
        FROM working
        CROSS JOIN (VALUES {period_values}) AS periods({period_col})
    """)

    row_count = con.execute("SELECT COUNT(*) FROM exploded").fetchone()[0]
    LOGGER.info(f"Exploded to {row_count:,} rows (working rows x {n_periods} periods)")


def unpivot_monthly(
    con: duckdb.DuckDBPyConnection,
    month_col: str = "month",
    n_months: int = 12,
    pattern: str = r"^(.+)_mo_(\d{2})$",
) -> None:
    # Unpivot wide monthly columns into long format.
    # `pattern` must be a regex with two capture groups: (base_name, month_number).
    # Default matches *_mo_NN (e.g. dual_eligibility_mo_01).
    cols = [row[1] for row in con.execute("PRAGMA table_info('working')").fetchall()]
    pat  = re.compile(pattern)

    base_cols: dict[str, dict[int, str]] = {}  # base_name → {month: column_name}
    static_cols: list[str] = []
    for c in cols:
        m = pat.match(c)
        if m:
            base_cols.setdefault(m.group(1), {})[int(m.group(2))] = c
        elif c != "filename":
            static_cols.append(c)

    if not base_cols:
        raise ValueError(
            "'monthly_columns' is set but no *_mo_NN columns were found in 'working'."
        )

    LOGGER.info(f"Static columns  : {static_cols}")
    LOGGER.info(f"Monthly groups  : {list(base_cols)}")

    static_sql = ", ".join(static_cols)
    parts = []
    for mo in range(1, n_months + 1):
        monthly_sql = ", ".join(
            f"{base_cols[b].get(mo, 'NULL')} AS {b}" for b in base_cols
        )
        parts.append(f"SELECT {static_sql}, {mo} AS {month_col}, {monthly_sql} FROM working")

    con.execute("CREATE OR REPLACE TABLE unpivoted AS " + " UNION ALL ".join(parts))
    row_count = con.execute("SELECT COUNT(*) FROM unpivoted").fetchone()[0]
    LOGGER.info(f"Unpivoted to {row_count:,} rows (working rows x {n_months} months)")


def process_year(year: int, table_name: str, config: dict) -> str:
    """
    Main orchestration: glob resolved input files -> load -> transform -> (optionally) explode -> write.

    config must be a plain dict with all OmegaConf interpolations already resolved:
      path_pattern  — fully resolved glob string (e.g. /data/input/2018/taf_*/part-*.parquet)
      output_name   — fully resolved output filename stem (e.g. beneficiaries_2018)
      output_dir    — fully resolved output directory path
      columns       — list of column name strings
      explode_arrays, period_col, n_periods — optional
    """
    start = time.time()
    LOGGER.info(f"\n{'='*60}")
    LOGGER.info(f"Table: {table_name} | Year: {year}")
    LOGGER.info(f"{'='*60}")

    output_name       = config.get("output_name", f"{table_name}_{year}")
    output_dir        = config["output_dir"]
    should_explode    = config.get("explode_arrays", False)
    should_unpivot    = config.get("monthly_columns", False)
    period_col        = config.get("period_col", "month")
    n_periods         = config.get("n_periods", 12)
    monthly_pattern   = config.get("monthly_pattern", r"^(.+)_mo_(\d{2})$")

    LOGGER.info("\n[1/4] Resolving input files...")
    parquet_files = glob_parquet_files(config["path_pattern"])
    for f in parquet_files:
        LOGGER.info(f"  • {f}")

    with duckdb.connect() as con:
        LOGGER.info("\n[2/4] Loading raw parquet files...")
        load_parquet_files(con, parquet_files)

        LOGGER.info("\n[3/4] Applying column transformations...")
        build_working_table(con, config)

        if should_explode:
            LOGGER.info(f"\n[4/4] Exploding arrays → one row per {period_col}...")
            explode_arrays(con, period_col=period_col, n_periods=n_periods)
            final_table = "exploded"
        elif should_unpivot:
            LOGGER.info(f"\n[4/4] Unpivoting monthly columns → one row per {period_col}...")
            unpivot_monthly(con, month_col=period_col, n_months=n_periods, pattern=monthly_pattern)
            final_table = "unpivoted"
        else:
            LOGGER.info("\n[4/4] No reshape configured (explode_arrays/monthly_columns: false)")
            final_table = "working"

        row_count = con.execute(f"SELECT COUNT(*) FROM {final_table}").fetchone()[0]

        preview_query = f"SELECT * FROM {final_table} LIMIT 5"
        LOGGER.info(f"\nPreview (first 5 rows):\n{con.execute(preview_query).fetchdf().to_string()}")
        LOGGER.info(f"Total rows: {row_count:,}")

        os.makedirs(output_dir, exist_ok=True)
        full_output_path = os.path.join(output_dir, f"{output_name}.parquet")

        con.execute(f"COPY {final_table} TO '{full_output_path}' (FORMAT PARQUET)")
        LOGGER.info(f"\nWritten to: {full_output_path}")

    LOGGER.info(f"Runtime: {time.time() - start:.2f}s")
    return full_output_path
