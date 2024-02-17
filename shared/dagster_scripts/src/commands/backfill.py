import pathlib as plb
import typing
import logging

from dagster_scripts.utils import load_config
from dagster_scripts.configs.backfill import BackfillConfig

logger = logging.getLogger("dagster_scripts.commands.backfill")


def load_backfill_config(path: typing.Union[str, plb.Path]) -> BackfillConfig:
    """Load a backfill configuration from disk"""
    logger.debug(f"Loading backfill configuration from {path}")
    return load_config(path, BackfillConfig)


def backfill(config: BackfillConfig) -> None:
    """Backfill a set of runs"""
    logger.info(f"Backfilling runs for {config.pipeline_name}")
    # ... do the backfilling
