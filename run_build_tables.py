"""
run_build_tables.py — entry point for Stage 2 of the normalization pipeline.

Reads per-year parquet files produced by select_variables (Stage 1), concatenates
them across all years, and writes one of four output types depending on table config:

  non-sharded + aggregated:  {table}_{year_min}_{year_max}_array.parquet
  sharded     + aggregated:  {table}_{shard_val}_array.parquet   (one per shard value)
  non-sharded + flat:        {table}_{year_min}_{year_max}.parquet
  sharded     + flat:        {table}_{shard_val}.parquet          (one per shard value)

Table configs live in conf/build_tables/{dataset}.yaml, under a top-level 'tables' map.

Usage:
  python run_build_tables.py table=beneficiaries          # non-sharded + aggregated
  python run_build_tables.py table=eligibility            # sharded + aggregated
  python run_build_tables.py table=ip_header              # non-sharded + flat
  python run_build_tables.py table=ip_line                # sharded + flat
  python run_build_tables.py table=beneficiaries dataset=medicaid_taf
"""

from __future__ import annotations

import logging
import yaml

import hydra
from omegaconf import DictConfig

from normalizecms.build_tables import build_table

LOGGER = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    table_name = str(cfg.table)
    dataset    = str(cfg.dataset)

    config_path = f"conf/build_tables/{dataset}.yaml"
    LOGGER.info("Loading build_tables config: %s", config_path)
    with open(config_path) as f:
        dataset_conf = yaml.safe_load(f)

    if table_name not in dataset_conf["tables"]:
        raise KeyError(f"Table '{table_name}' not found in {config_path}")
    table_conf = dataset_conf["tables"][table_name]

    basepath    = str(cfg.datapaths.dirs.intermediate)
    output_path = str(cfg.datapaths.dirs.output)

    LOGGER.info("Table      : %s", table_name)
    LOGGER.info("Years      : %s – %s", table_conf["year_min"], table_conf["year_max"])
    LOGGER.info("Aggregated : %s", "primary_key" in table_conf)
    LOGGER.info("Sharded    : %s", "shard_by" in table_conf)
    LOGGER.info("Basepath   : %s", basepath)
    LOGGER.info("Output dir : %s", output_path)

    build_table(
        table_name=table_name,
        table_conf=table_conf,
        basepath=basepath,
        output_path=output_path,
    )


if __name__ == "__main__":
    main()
