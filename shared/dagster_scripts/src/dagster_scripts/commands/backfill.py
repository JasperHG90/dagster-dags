import pathlib as plb
import typing
import logging

from dagster_graphql import DagsterGraphQLClient

from dagster_scripts.configs.backfill import BackfillConfig
from dagster_scripts.commands.utils import load_config, submit_backfill_jobs, await_backfill_status

logger = logging.getLogger("dagster_scripts.commands.backfill")


def load_backfill_config(path: typing.Union[str, plb.Path]) -> BackfillConfig:
    """Load a backfill configuration from disk"""
    logger.debug(f"Loading backfill configuration from {path}")
    return load_config(path, BackfillConfig)


def backfill(config: BackfillConfig) -> None:
    """Backfill a set of runs"""
    backfill_run_ids = submit_backfill_jobs(config)
    return await_backfill_status(backfill_run_ids)
