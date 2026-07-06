from __future__ import annotations

import os
import time
import logging
from pathlib import Path

import duckdb

LOGGER = logging.getLogger(__name__)

# Stage 2 of the CMS normalization pipeline.
# Loads per-year parquet files produced by select_variables.py (Stage 1), concatenates them across
# all years in a single DuckDB scan, and aggregates rows by primary_key columns. Always writes a
# flat .parquet file; if primary_key is configured, also writes a _array.parquet with conflict indicators.

# Conflict indicators (n_distinct_*):
#   For aggregated tables, each non-primary-key column gets a paired n_distinct_{col} column
#   counting distinct non-null values across grouped rows. A value > 1 means the same
#   entity has conflicting values for that column, which downstream cleaning can resolve.


def validate_table_conf(table_name: str, table_conf: dict) -> None:
    # Check all required config fields are present before doing any work.
    required = ["year_min", "year_max", "path_pattern", "output_name"]
    missing  = [f for f in required if f not in table_conf]
    if missing:
        raise ValueError(f"Missing required config fields for '{table_name}': {missing}")


def verify_files_exist(table_conf: dict, basepath: str) -> None:
    # Check that a per-year file exists for every year in [year_min, year_max]. Lists all missing at once.
    year_min, year_max = table_conf["year_min"], table_conf["year_max"]
    missing = [
        table_conf["path_pattern"].format(basepath=basepath, year=year)
        for year in range(year_min, year_max + 1)
        if not Path(table_conf["path_pattern"].format(basepath=basepath, year=year)).exists()
    ]
    if missing:
        raise FileNotFoundError("Missing required input files:\n" + "\n".join(f"  • {f}" for f in missing))


def get_glob_pattern(table_conf: dict, basepath: str) -> str:
    # Build a glob pattern covering all per-year files by substituting '*' for year.
    return table_conf["path_pattern"].format(basepath=basepath, year="*")


def format_output_name(table_conf: dict) -> str:
    # Resolve output filename template, e.g. 'beneficiaries_{year_min}_{year_max}' -> 'beneficiaries_2014_2018'.
    return table_conf["output_name"].format(
        year_min=table_conf["year_min"],
        year_max=table_conf["year_max"],
    )


def load_parquet_glob(con: duckdb.DuckDBPyConnection, table_name: str, glob_path: str) -> int:
    # Load all per-year parquet files in a single DuckDB parallel scan into '{table_name}_raw'.
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name}_raw AS
        SELECT * FROM read_parquet('{glob_path}')
    """)
    return con.execute(f"SELECT COUNT(*) FROM {table_name}_raw").fetchone()[0]


def build_conflict_indicators(agg_cols: list[str]) -> list[str]:
    # For each non-key column, produce two SQL expressions:
    #   array_agg(col)     — collects all values across grouped rows
    #   n_distinct_{col}   — counts distinct non-null values (> 1 = conflicting records)
    # Nulls excluded from distinct count via array_filter to avoid inflating conflict counts.
    exprs = []
    for col in agg_cols:
        exprs.append(f"array_agg({col}) AS {col}")
        exprs.append(
            f"array_length(array_distinct(array_filter(array_agg({col}), x -> x IS NOT NULL))) AS n_distinct_{col}"
        )
    return exprs


def aggregate_table(con: duckdb.DuckDBPyConnection, table_name: str, table_conf: dict, output_path: str) -> None:
    # Group by primary_key, aggregate all others into arrays with conflict indicators, write _array.parquet.
    group_cols = list(table_conf["primary_key"])
    group_expr = ", ".join(group_cols)

    all_cols = [row[1] for row in con.execute(f"PRAGMA table_info('{table_name}_raw')").fetchall()]
    agg_cols = [col for col in all_cols if col not in group_cols]

    LOGGER.info(f"Group-by columns: {group_cols}")
    LOGGER.info(f"Aggregating: {len(agg_cols)} columns")

    agg_exprs = build_conflict_indicators(agg_cols)
    agg_cols = ",\n            ".join(agg_exprs)

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name}_agg AS
        SELECT
            {group_expr},
            {agg_cols}
        FROM {table_name}_raw
        GROUP BY {group_expr}
        """
    )

    agg_count = con.execute(f"SELECT COUNT(*) FROM {table_name}_agg").fetchone()[0]
    LOGGER.info(f"Aggregated to: {agg_count:,} unique groups")

    output_name  = format_output_name(table_conf)
    array_output = os.path.join(output_path, f"{output_name}_array.parquet")
    con.execute(f"COPY {table_name}_agg TO '{array_output}' (FORMAT PARQUET)")
    LOGGER.info(f"Written (array): {array_output}")


def write_flat_table(con: duckdb.DuckDBPyConnection, table_name: str, table_conf: dict, output_path: str) -> None:
    # Write the concatenated raw table directly to parquet.
    output_name = format_output_name(table_conf)
    output_file = os.path.join(output_path, f"{output_name}.parquet")
    con.execute(f"COPY {table_name}_raw TO '{output_file}' (FORMAT PARQUET)")
    LOGGER.info(f"Written (flat): {output_file}")


def configure_duckdb(con: duckdb.DuckDBPyConnection, table_conf: dict) -> None:
    # Apply DuckDB performance settings from config (or defaults).
    threads      = table_conf.get("threads", 8)
    memory_limit = table_conf.get("memory_limit", "64GB")
    con.execute(f"PRAGMA threads={threads}")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    LOGGER.info(f"Threads: {threads} | Memory limit: {memory_limit}")


def build_table(con: duckdb.DuckDBPyConnection, table_name: str, table_conf: dict, basepath: str, output_path: str) -> None:
    # Main orchestration: validate → verify files → load → (optionally) aggregate → write flat.
    start = time.time()
    LOGGER.info(f"\n{'='*60}")
    LOGGER.info(f"Table: {table_name} | Years: {table_conf['year_min']}–{table_conf['year_max']}")
    LOGGER.info(f"{'='*60}")

    validate_table_conf(table_name, table_conf)

    LOGGER.info("\n[1/3] Verifying input files...")
    verify_files_exist(table_conf, basepath)
    LOGGER.info("All input files present")

    LOGGER.info("\n[2/3] Loading data...")
    glob_path = get_glob_pattern(table_conf, basepath)
    LOGGER.info(f"Pattern: {glob_path}")
    raw_count = load_parquet_glob(con, table_name, glob_path)
    LOGGER.info(f"Loaded: {raw_count:,} rows")

    LOGGER.info("\n[3/3] Writing output...")
    if "primary_key" in table_conf:
        LOGGER.info("Aggregation: enabled")
        aggregate_table(con, table_name, table_conf, output_path)
    else:
        LOGGER.info("Aggregation: disabled (no primary_key in config)")

    write_flat_table(con, table_name, table_conf, output_path)
    LOGGER.info(f"\nRuntime: {time.time() - start:.2f}s")
