import logging
import pathlib as plb
import typing

from dagster_scripts.configs.report_asset import ReportAssetConfig
from dagster_scripts.configs.utils import load_config

logger = logging.getLogger("dagster_scripts.commands.report_asset")


def load_report_asset_config(path: typing.Union[str, plb.Path]) -> ReportAssetConfig:
    """Load a backfill configuration from disk"""
    logger.debug(f"Loading backfill configuration from {path}")
    return load_config(path, ReportAssetConfig)
