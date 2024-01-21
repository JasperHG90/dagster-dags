import pandas as pd
from dagster import AssetCheckResult, asset_check
from luchtmeetnet_ingestion.assets import air_quality_data


@asset_check(
    asset=air_quality_data,
)
def values_above_zero(air_quality_data: pd.DataFrame):
    check = air_quality_data.min()["value"] >= 0
    return AssetCheckResult(passed=bool(check))
