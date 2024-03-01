import hashlib
import os

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    BackfillPolicy,
    Backoff,
    DataVersion,
    Failure,
    FreshnessPolicy,
    Jitter,
    Output,
    RetryPolicy,
    asset,
)
from luchtmeetnet_ingestion.IO.resources import LuchtMeetNetResource
from luchtmeetnet_ingestion.partitions import (
    daily_partition,
    daily_station_partition,
    stations_partition,
)
from pandas.util import hash_pandas_object
from requests import HTTPError


@asset(
    description="Air quality data from the Luchtmeetnet API",
    compute_kind="duckdb",
    io_manager_key="landing_zone",
    partitions_def=daily_station_partition,
    retry_policy=RetryPolicy(
        max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS
    ),
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    code_version="v1",
    group_name="measurements",
)
def air_quality_data(
    context: AssetExecutionContext, luchtmeetnet_api: LuchtMeetNetResource
) -> Output[pd.DataFrame]:
    date, station = context.partition_key.split("|")
    context.log.debug(date)
    context.log.debug(f"Fetching data for {date}")
    start, end = f"{date}T00:00:00", f"{date}T23:59:59"
    rp = {"start": start, "end": end, "station_number": station}
    try:
        df = pd.DataFrame(
            luchtmeetnet_api.request("measurements", context=context, request_params=rp)
        )
    # We don't want to keep retrying for a station that is rasing code 500
    except HTTPError as e:
        if e.response.status_code == 500:
            raise Failure(
                description=f"Received HTTP status code 500 Failed to fetch data for {date} and station {station}. Skipping retries ...",
                allow_retries=False,
            ) from e
        else:
            raise e
    df["station_number"] = station
    df["start"] = start
    df["end"] = end
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
            input_manager_key="landing_zone",
        )
    },
    code_version="v1",
    group_name="measurements",
)
def daily_air_quality_data(
    context: AssetExecutionContext, ingested_data: pd.DataFrame
) -> pd.DataFrame:
    return ingested_data


@asset(
    io_manager_key="landing_zone",
    compute_kind="duckdb",
    description="Luchtmeetnet API stations",
    partitions_def=stations_partition,
    # Setting max_materializations_per_minute disables the rate limiter
    auto_materialize_policy=AutoMaterializePolicy.eager(
        max_materializations_per_minute=None
    ).with_rules(
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
def air_quality_station_names(
    context: AssetExecutionContext, station_names: pd.DataFrame
) -> pd.DataFrame:
    return station_names
