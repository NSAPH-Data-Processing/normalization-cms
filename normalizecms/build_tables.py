from __future__ import annotations

import glob
import os
import time
import logging
from pathlib import Path

import duckdb

LOGGER = logging.getLogger(__name__)

def _esc(path: str) -> str:
    return path.replace("'", "''")

# Stage 2 of the CMS normalization pipeline.
# Loads per-year parquet files produced by select_variables.py (Stage 1), concatenates them across
# all years in a single DuckDB scan. If primary_key is configured, aggregates rows and writes a
# _array.parquet with conflict indicators; otherwise writes a flat .parquet. Both output types
# support optional sharding via shard_by in the table config — sharded tables are written via
# DuckDB's native PARTITION_BY (single-pass, avoids one filtered re-scan per shard value) and
# then flattened back to one flat file per shard value ({table}_{shard}.parquet) directly in
# the output directory, matching the non-sharded naming convention.

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
    if "shard_by" in table_conf and "primary_key" in table_conf:
        if table_conf["shard_by"] not in table_conf["primary_key"]:
            raise ValueError(
                f"'shard_by' column '{table_conf['shard_by']}' must be in 'primary_key' "
                f"for table '{table_name}'"
            )


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


def get_year_paths(table_conf: dict, basepath: str) -> list[str]:
    # Explicit list of per-year files for [year_min, year_max]. Deliberately NOT a '*' wildcard
    # glob — that would silently pick up every year's file present in basepath regardless of
    # the configured year range, since select_variables output for other years/tables commonly
    # already exists on disk from prior runs.
    year_min, year_max = table_conf["year_min"], table_conf["year_max"]
    return [
        table_conf["path_pattern"].format(basepath=basepath, year=year)
        for year in range(year_min, year_max + 1)
    ]


def format_output_name(table_conf: dict) -> str:
    # Resolve output filename template, e.g. 'beneficiaries_{year_min}_{year_max}' -> 'beneficiaries_2014_2018'.
    return table_conf["output_name"].format(
        year_min=table_conf["year_min"],
        year_max=table_conf["year_max"],
    )


def load_parquet_glob(con: duckdb.DuckDBPyConnection, table_name: str, paths: list[str]) -> int:
    # Load exactly the given per-year parquet files (not a filesystem glob) in a single
    # DuckDB parallel scan into '{table_name}_raw'. select_variables.py's own output always
    # carries a 'filename' lineage column today, but DuckDB has no "EXCLUDE IF EXISTS" — a bare
    # EXCLUDE (filename) errors outright ("not found in FROM clause") against source files that
    # don't have it, so check the schema first rather than assuming the column is always there.
    files_expr = "[" + ", ".join(f"'{_esc(p)}'" for p in paths) + "]"
    read_expr = f"read_parquet({files_expr}, union_by_name=True)"
    schema_cols = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {read_expr}").fetchall()]
    select_expr = "* EXCLUDE (filename)" if "filename" in schema_cols else "*"
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name}_raw AS
        SELECT {select_expr} FROM {read_expr}
    """)
    return con.execute(f"SELECT COUNT(*) FROM {table_name}_raw").fetchone()[0]

def build_conflict_indicators(agg_cols: list[str], col_types: dict[str, str]) -> tuple[list[str], list[str]]:
    unique_exprs = []
    duplicate_exprs = []

    for col in agg_cols:
        is_array_col = "[]" in col_types[col].upper()
        # Unique rows: wrap value in array so schema matches duplicate rows
        unique_exprs.append(f"[{col}] AS {col}")
        unique_exprs.append(f"CASE WHEN {col} IS NULL THEN 0 ELSE 1 END AS n_distinct_{col}")

        # Duplicate rows: aggregate values and count distinct non-null values
        duplicate_exprs.append(f"array_agg({col}) AS {col}")
        distinct_col = f"CAST({col} AS VARCHAR)" if is_array_col else col
        duplicate_exprs.append(f"array_length(array_distinct(array_filter(array_agg({distinct_col}), x -> x IS NOT NULL))) AS n_distinct_{col}")

    return unique_exprs, duplicate_exprs

def write_sharded_parquet(
    con: duckdb.DuckDBPyConnection,
    source_table: str,
    shard_col: str,
    table_name: str,
    output_path: str,
    suffix: str = "",
) -> None:
    # Single-pass write via DuckDB's native PARTITION_BY (one scan of source_table, DuckDB
    # buckets rows by shard_col internally) instead of re-scanning source_table once per
    # distinct shard value — then flatten the resulting Hive directory structure back to one
    # flat file per shard directly in output_path, matching the flat-file convention the
    # Snakefile/cleaning-cms expect. The flatten step is a same-filesystem rename (os.replace),
    # not a data re-write, so it keeps the single-pass write's I/O win while keeping the
    # on-disk layout flat. WRITE_PARTITION_COLUMNS keeps shard_col as a real column in each
    # file, matching the original per-shard COPY's behavior (SELECT * included it too).
    #
    # `suffix` (e.g. "_array") marks aggregated output, mirroring the non-sharded branches in
    # aggregate_table()/write_flat_table() — callers must pass it explicitly since this function
    # doesn't otherwise know whether source_table went through aggregation.
    partition_dir = os.path.join(output_path, table_name)
    con.execute(f"""
        COPY (SELECT * FROM {source_table})
        TO '{partition_dir}'
        (FORMAT PARQUET, PARTITION_BY ({shard_col}), WRITE_PARTITION_COLUMNS true,
         OVERWRITE_OR_IGNORE true, FILENAME_PATTERN 'data_{{i}}')
    """)

    # Flatten: move each partition's freshly-written data_*.parquet file(s) up into
    # output_path directly, named {table_name}_{shard_val}{suffix}.parquet, then remove the
    # now-empty Hive partition directories. Stale flat file(s) from a previous run are removed
    # first so reruns cleanly replace rather than accumulate (mirrors the same concern as the
    # now-removed per-shard-value COPY loop this replaced). The stale-file glob intentionally
    # matches any suffix, not just the current one, so switching a table between aggregated and
    # flat doesn't leave an old-suffix file behind alongside the new one.
    for shard_dir in sorted(glob.glob(os.path.join(partition_dir, f"{shard_col}=*"))):
        shard_val = os.path.basename(shard_dir).split("=", 1)[1]
        for stale in glob.glob(os.path.join(output_path, f"{table_name}_{shard_val}*.parquet")):
            os.remove(stale)
        new_files = sorted(glob.glob(os.path.join(shard_dir, "data_*.parquet")))
        for i, f in enumerate(new_files):
            idx = f"_{i}" if i > 0 else ""  # in the rare case a partition splits into >1 file
            os.replace(f, os.path.join(output_path, f"{table_name}_{shard_val}{suffix}{idx}.parquet"))
        os.rmdir(shard_dir)
    if os.path.isdir(partition_dir):
        os.rmdir(partition_dir)

    LOGGER.info(f"Written (flat, via single-pass Hive-partitioned write): {output_path}/{table_name}_<value>{suffix}.parquet")


def aggregate_table(con: duckdb.DuckDBPyConnection, table_name: str, table_conf: dict, output_path: str) -> None:
    # Group by primary_key, aggregate all others into arrays with conflict indicators, write _array.parquet.
    group_cols = list(table_conf["primary_key"])
    group_expr = ", ".join(group_cols)

    table_info = con.execute(f"PRAGMA table_info('{table_name}_raw')").fetchall()
    all_cols = [row[1] for row in table_info]
    col_types = {row[1]: row[2] for row in table_info}

    agg_cols = [col for col in all_cols if col not in group_cols]

    LOGGER.info(f"Group-by columns: {group_cols}")
    LOGGER.info(f"Checking duplicates for: {len(agg_cols)} columns")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {table_name}_dup_keys AS
        SELECT {group_expr}
        FROM {table_name}_raw
        GROUP BY {group_expr}
        HAVING COUNT(*) > 1
    """)

    LOGGER.info(f"Built duplicate-key table")

    dup_count = con.execute(f"SELECT COUNT(*) FROM {table_name}_dup_keys").fetchone()[0]
    LOGGER.info(f"Duplicate groups: {dup_count:,}")
    unique_exprs, duplicate_exprs = build_conflict_indicators(agg_cols, col_types)

    unique_cols_sql = ",\n            ".join(unique_exprs)
    duplicate_cols_sql = ",\n            ".join(duplicate_exprs)



    LOGGER.info("Starting final aggregation query")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name}_agg AS

        SELECT
            {group_expr},
            {unique_cols_sql}
        FROM {table_name}_raw
        ANTI JOIN {table_name}_dup_keys
        USING ({group_expr})

        UNION ALL

        SELECT
            {group_expr},
            {duplicate_cols_sql}
        FROM {table_name}_raw
        JOIN {table_name}_dup_keys
        USING ({group_expr})
        GROUP BY {group_expr}
    """)

    LOGGER.info(f"Created final aggregated table")

    agg_count = con.execute(f"SELECT COUNT(*) FROM {table_name}_agg").fetchone()[0]
    LOGGER.info(f"Final aggregated rows: {agg_count:,}")
    
    output_name = format_output_name(table_conf)
    shard_col = table_conf.get("shard_by")
    if shard_col:
        write_sharded_parquet(con, f"{table_name}_agg", shard_col, table_name, output_path, suffix="_array")
    else:
        array_output = os.path.join(output_path, f"{output_name}_array.parquet")
        con.execute(f"COPY {table_name}_agg TO '{array_output}' (FORMAT PARQUET)")
        LOGGER.info(f"Written (array): {array_output}")


def write_flat_table(con: duckdb.DuckDBPyConnection, table_name: str, table_conf: dict, output_path: str) -> None:
    output_name = format_output_name(table_conf)
    shard_col = table_conf.get("shard_by")
    if shard_col:
        write_sharded_parquet(con, f"{table_name}_raw", shard_col, table_name, output_path)
    else:
        output_file = os.path.join(output_path, f"{output_name}.parquet")
        con.execute(f"COPY {table_name}_raw TO '{output_file}' (FORMAT PARQUET)")
        LOGGER.info(f"Written (flat): {output_file}")


def configure_duckdb(con: duckdb.DuckDBPyConnection, table_conf: dict, table_name: str) -> None:
    # Apply DuckDB performance settings from config (or defaults).
    threads      = table_conf.get("threads", 4)
    memory_limit = table_conf.get("memory_limit", "400GB")
    temp_dir     = f".tmp_{table_name}"
    con.execute(f"PRAGMA threads={threads}")
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute("PRAGMA preserve_insertion_order=false")
    # Give each table's connection its own spill directory. Without this, concurrent
    # build_tables runs (e.g. two tables under `snakemake --cores 2+`) share DuckDB's
    # default relative .tmp/ directory and can delete each other's spill files out from
    # under a still-running query once a large table's aggregation needs to spill to disk.
    con.execute(f"PRAGMA temp_directory='{temp_dir}'")
    LOGGER.info(f"Threads: {threads} | Memory limit: {memory_limit} | Temp dir: {temp_dir}")


def build_table(table_name: str, table_conf: dict, basepath: str, output_path: str) -> None:
    # Main orchestration: validate → verify files → load → (optionally) aggregate → write flat.
    #
    # Deliberately always loads and aggregates the table's full year_min..year_max range in one
    # pass, even for sharded tables where per-year splitting would be mathematically equivalent
    # (shard_by is required by validate_table_conf to be part of primary_key, so aggregation
    # never merges rows across years anyway). A per-year build was tried and reverted: loading
    # all years together via read_parquet(..., union_by_name=True) in load_parquet_glob acts as
    # an implicit schema-drift check across years (a column type change or rename in some year's
    # Stage-1 output surfaces immediately), which per-year loading would silently miss since each
    # year would never be compared against the others.
    start = time.time()
    LOGGER.info(f"\n{'='*60}")
    LOGGER.info(f"Table: {table_name} | Years: {table_conf['year_min']}–{table_conf['year_max']}")
    LOGGER.info(f"{'='*60}")

    validate_table_conf(table_name, table_conf)

    LOGGER.info("\n[1/3] Verifying input files...")
    verify_files_exist(table_conf, basepath)
    LOGGER.info("All input files present")

    with duckdb.connect() as con:
        configure_duckdb(con, table_conf, table_name)

        LOGGER.info("\n[2/3] Loading data...")
        year_paths = get_year_paths(table_conf, basepath)
        LOGGER.info(f"Files: {len(year_paths)} ({year_paths[0]} .. {year_paths[-1]})")
        raw_count = load_parquet_glob(con, table_name, year_paths)
        LOGGER.info(f"Loaded: {raw_count:,} rows")

        LOGGER.info("\n[3/3] Writing output...")
        if "primary_key" in table_conf:
            LOGGER.info("Aggregation: enabled")
            aggregate_table(con, table_name, table_conf, output_path)
        else:
            LOGGER.info("Aggregation: disabled (no primary_key in config)")
            write_flat_table(con, table_name, table_conf, output_path)

    LOGGER.info(f"\nRuntime: {time.time() - start:.2f}s")
    