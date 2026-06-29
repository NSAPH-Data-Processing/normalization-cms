from __future__ import annotations

import logging
import os

import duckdb
import hydra
import yaml
from omegaconf import DictConfig, OmegaConf

from normalizecms.build_tables import build_table, configure_duckdb

LOGGER = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    """
    Build a multi-year normalized table from per-year CMS parquet files.

    Example:
      python run_build_tables.py dataset=medicaid_taf table=beneficiaries
    """
    LOGGER.info("Config:\n%s", OmegaConf.to_yaml(cfg))

    table_name = str(cfg.table)

    with open(f"conf/build_tables/{cfg.dataset}.yaml", "r") as f:
        build_tables_cfg = yaml.safe_load(f)

    if table_name not in build_tables_cfg["tables"]:
        available = list(build_tables_cfg["tables"].keys())
        raise ValueError(f"Table '{table_name}' not found in build_tables.yml. Available: {available}")

    table_conf  = build_tables_cfg["tables"][table_name]
    basepath    = str(cfg.datapaths.dirs.intermediate)
    output_path = str(cfg.datapaths.dirs.output)

    os.makedirs(output_path, exist_ok=True)

    with duckdb.connect() as con:
        configure_duckdb(con, table_conf)
        build_table(
            con=con,
            table_name=table_name,
            table_conf=table_conf,
            basepath=basepath,
            output_path=output_path,
        )


if __name__ == "__main__":
    main()