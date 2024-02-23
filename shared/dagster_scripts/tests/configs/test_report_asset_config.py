import pytest

from dagster_scripts.configs.report_asset import ReportAssetConfig, AssetConfig
from dagster_scripts.configs.base import StorageTypeEnum


def test_report_asset_storage_config(asset_config):
    conf = AssetConfig(**asset_config)
    assert conf.storage_type == StorageTypeEnum.gcs
    assert conf.bucket == "my-bucket"
    assert conf.prefix == "my-path"


def test_report_asset_storage_config_unknown_scheme(asset_config):
    with pytest.raises(ValueError):
        asset_config["storage_location"] = "minio://my-bucket/my-path"
        AssetConfig(**asset_config)


def test_load_backfill_config(report_asset_config):
    ReportAssetConfig(**report_asset_config)
