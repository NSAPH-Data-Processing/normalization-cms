from __future__ import annotations

import logging

import hydra
from omegaconf import DictConfig, OmegaConf

from normalizecms.select_variables import process_year

LOGGER = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    """
    Select variables for a single CMS table and year.

    Example:
      python run_select_variables.py table_config=medicaid_taf/ip_header year=2018
    """
    LOGGER.info("Config:\n%s", OmegaConf.to_yaml(cfg))

    # Resolve all OmegaConf interpolations (${datapaths.dirs.input}, ${year}, etc.)
    # using the full cfg as context so cross-group references resolve correctly.
    table_cfg = OmegaConf.to_container(cfg, resolve=True)["table_config"]

    process_year(
        year=int(cfg.year),
        table_name=table_cfg["name"],
        config=table_cfg,
    )


if __name__ == "__main__":
    main()
