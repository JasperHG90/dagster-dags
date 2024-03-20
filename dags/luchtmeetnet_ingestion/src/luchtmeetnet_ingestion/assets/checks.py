import pandas as pd
from dagster import AssetCheckResult, asset_check
from luchtmeetnet_ingestion.assets import air_quality_data


# Currently not supported on a per-partition basis, but can do this for entire asset
#  https://github.com/dagster-io/dagster/discussions/17194
#
# Issue for adding per-partition checks here:
#  https://github.com/dagster-io/dagster/issues/17005
@asset_check(
    asset=air_quality_data,
)
def values_above_zero(air_quality_data: pd.DataFrame):
    check = air_quality_data.min()["value"] >= 0
    return AssetCheckResult(passed=bool(check))


# Abuse asset checks to add hooks for assets
#  https://github.com/dagster-io/dagster/issues/20471
