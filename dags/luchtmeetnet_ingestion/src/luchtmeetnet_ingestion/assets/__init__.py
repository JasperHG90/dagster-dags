import hashlib
import os

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    Backoff,
    DataVersion,
    FreshnessPolicy,
    Jitter,
    MultiToSingleDimensionPartitionMapping,
    Output,
    RetryPolicy,
    asset,
)
from luchtmeetnet_ingestion.assets.utils import get_air_quality_data_for_partition_key
from luchtmeetnet_ingestion.IO.resources import LuchtMeetNetResource
from luchtmeetnet_ingestion.partitions import (
    daily_partition,
    daily_station_partition,
    stations_partition,
)
from pandas.util import hash_pandas_object


@asset(
    description="Air quality data from the Luchtmeetnet API",
    compute_kind="duckdb",
    io_manager_key="landing_zone",
    partitions_def=daily_station_partition,
    retry_policy=RetryPolicy(
        max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS
    ),
    # NB: setting this to a higher value will disallow use of context.partition_key because
    #  will be supplied a range. See https://docs.dagster.io/_apidocs/libraries/dagster-duckdb
    # Higher values currently not supported because reading in ranges is not supported in the
    # I/O manager
    # backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=None)
    .with_rules(
        AutoMaterializeRule.materialize_on_cron("0 3 * * *", all_partitions=False),
    )
    .without_rules(
        AutoMaterializeRule.skip_on_parent_outdated(),
        AutoMaterializeRule.skip_on_parent_missing(),
        AutoMaterializeRule.materialize_on_parent_updated(),
        AutoMaterializeRule.materialize_on_missing(),
    ),
    code_version="v1",
    group_name="measurements",
)
def air_quality_data(
    context: AssetExecutionContext, luchtmeetnet_api: LuchtMeetNetResource
) -> Output[pd.DataFrame]:
    df = get_air_quality_data_for_partition_key(context.partition_key, context, luchtmeetnet_api)
    df_hash = hashlib.sha256(hash_pandas_object(df, index=True).values).hexdigest()
    return Output(df, data_version=DataVersion(df_hash))


@asset(
    description="Copy air quality data from ingestion to bronze",
    compute_kind="duckdb",
    io_manager_key="data_lake_bronze",
    partitions_def=daily_partition,
    ins={
        "ingested_data": AssetIn(
            "air_quality_data",
            # NB: need this to control which downstream asset partitions are materialized
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="daily"
            ),
            input_manager_key="landing_zone",
        )
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=None)
    .with_rules(
        AutoMaterializeRule.skip_on_not_all_parents_updated(
            require_update_for_all_parent_partitions=False
        ),
        AutoMaterializeRule.materialize_on_required_for_freshness(),
    )
    .without_rules(
        AutoMaterializeRule.skip_on_parent_outdated(),
        AutoMaterializeRule.skip_on_parent_missing(),
    ),
    code_version="v1",
    group_name="measurements",
)
def daily_air_quality_data(ingested_data: pd.DataFrame) -> pd.DataFrame:
    return ingested_data


@asset(
    io_manager_key="landing_zone",
    compute_kind="duckdb",
    description="Luchtmeetnet API stations",
    partitions_def=stations_partition,
    # Setting max_materializations_per_minute disables the dagster rate limiter
    auto_materialize_policy=AutoMaterializePolicy.eager(
        max_materializations_per_minute=None
    ).with_rules(
        AutoMaterializeRule.materialize_on_missing(),
        AutoMaterializeRule.materialize_on_cron("0 0 1 * *", all_partitions=True),
    ),
    group_name="stations",
)
def station_names(
    context: AssetExecutionContext, luchtmeetnet_api: LuchtMeetNetResource
) -> pd.DataFrame:
    return pd.DataFrame(
        luchtmeetnet_api.request(
            os.path.join("stations", context.partition_key), context=context, paginate=False
        )
    )


@asset(
    io_manager_key="data_lake_bronze",
    description="Copy station names from ingestion to bronze",
    compute_kind="duckdb",
    auto_materialize_policy=AutoMaterializePolicy.eager().with_rules(
        AutoMaterializeRule.materialize_on_missing(),
        AutoMaterializeRule.materialize_on_parent_updated(),
        AutoMaterializeRule.materialize_on_required_for_freshness(),
    ),
    freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=10,
        cron_schedule="0 0 1 * *",
    ),
    group_name="stations",
)
def air_quality_station_names(station_names: pd.DataFrame) -> pd.DataFrame:
    return station_names
