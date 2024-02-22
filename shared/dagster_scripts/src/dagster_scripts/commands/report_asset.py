import logging
import pathlib as plb
import typing

import pandas as pd
from dagster_scripts.commands.utils import (
    get_materialized_partitions,
    report_asset_status_for_partitions,
)
from dagster_scripts.configs.report_asset import ReportAssetConfig, StorageTypeEnum
from dagster_scripts.configs.utils import load_config

logger = logging.getLogger("dagster_scripts.commands.report_asset")


def get_files(dir):
    _path = plb.Path(dir)
    globs = _path.glob("*.parquet")
    return pd.DataFrame(
        [{"path": f, "file": f.name, "partition_key": f.with_suffix("").name} for f in globs]
    )


def load_report_asset_config(path: typing.Union[str, plb.Path]) -> ReportAssetConfig:
    """Load a backfill configuration from disk"""
    logger.debug(f"Loading backfill configuration from {path}")
    return load_config(path, ReportAssetConfig)


def report_asset_status(config: ReportAssetConfig) -> None:
    """Report a set of assets"""
    for asset in config.assets:
        logger.debug(f"Asset key: {asset.key}")
        if asset.storage_location.type == StorageTypeEnum.gcs:
            raise NotImplementedError("GCS storage not yet supported")
        files = get_files(asset.storage_location.path)
        materialized_partitions = get_materialized_partitions(
            asset.key,
            skip_checks=asset.skip_checks,
            asset_partitions=files["partition_key"].tolist(),
        )
        logger.debug(
            f"Found {len(materialized_partitions)} materialized partitions for asset {asset.key}"
        )
        missing_partitions = list(
            set(files["partition_key"].tolist()) - set(materialized_partitions)
        )
        logger.debug(
            f"Reporting {len(missing_partitions)} missing partitions for asset {asset.key}"
        )
        report_asset_status_for_partitions(
            asset.key, missing_partitions, skip_checks=asset.skip_checks
        )
