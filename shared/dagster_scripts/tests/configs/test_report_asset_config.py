import pytest

from dagster_scripts.configs.report_asset import StorageConfig


def test_report_asset_storage_config(report_asset_storage_config):
    conf = StorageConfig(**report_asset_storage_config)
    assert conf.type == "gcs"


def test_report_asset_storage_config_unknown_scheme(report_asset_storage_config_unknown_scheme):
    with pytest.raises(ValueError):
        StorageConfig(**report_asset_storage_config_unknown_scheme)
