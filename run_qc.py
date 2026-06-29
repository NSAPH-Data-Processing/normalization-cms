from __future__ import annotations

import logging
import os

import hydra
from omegaconf import DictConfig, OmegaConf

from normalizecms.qc import run_qc

LOGGER = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    """
    Run QC on a normalized parquet file for a single table and year.

    Example:
      python run_qc.py table_config=medicaid_taf/ip_header year=2018
    """
    LOGGER.info("Config:\n%s", OmegaConf.to_yaml(cfg))

    year = int(cfg.year)

    # Resolve all OmegaConf interpolations using the full cfg as context.
    table_config = OmegaConf.to_container(cfg, resolve=True)["table_config"]

    out_parquet = os.path.join(table_config["output_dir"], f"{table_config['output_name']}.parquet")
    qc_dir      = os.path.join(table_config["output_dir"], f"{table_config['output_name']}.qc")

    qc = run_qc(
        table_config=table_config,
        output_parquet_path=out_parquet,
        year=year,
        qc_dir=qc_dir,
        sample_n=100,
    )

    LOGGER.info(
        "QC status=%s | table=%s | year=%s | Wrote %s",
        qc.get("status"),
        table_config["name"],
        year,
        os.path.join(qc_dir, "qc.json"),
    )


if __name__ == "__main__":
    main()