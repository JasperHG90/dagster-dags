import logging
import pathlib as plb
import typing

from dagster_scripts.commands.utils import (
    await_backfill_status,
    filter_asset_partitions,
    get_materialized_partitions,
    load_config,
    submit_backfill_jobs,
)
from dagster_scripts.configs.backfill import BackfillConfig
from dagster_scripts.configs.base import PolicyEnum
from dagster_scripts.configs.utils import generate_partition_configs

logger = logging.getLogger("dagster_scripts.commands.backfill")


def load_backfill_config(path: typing.Union[str, plb.Path]) -> BackfillConfig:
    """Load a backfill configuration from disk"""
    logger.debug(f"Loading backfill configuration from {path}")
    return load_config(path, BackfillConfig)


def backfill(config: BackfillConfig) -> None:
    """Backfill a set of runs"""
    partition_configs = generate_partition_configs(config.tags.partitions)
    if config.backfill_policy.policy == PolicyEnum.missing:
        logger.debug(
            f"Backfill policy is '{PolicyEnum.missing}'. Only backfilling missing partitions."
        )
        logger.debug("Filtering partitions for missing materializations")
        materialized_partitions = get_materialized_partitions(
            asset_key=config.backfill_policy.asset_key
        )
        _partition_configs = filter_asset_partitions(
            partition_configs=partition_configs,
            materialized_partitions=materialized_partitions,
        )
    else:
        _partition_configs = partition_configs
    backfill_run_ids = submit_backfill_jobs(
        job_name=config.job_name,
        repository_name=config.repository_name,
        backfill_name=config.tags.name,
        partition_configs=_partition_configs,
    )
    return await_backfill_status(backfill_run_ids)
