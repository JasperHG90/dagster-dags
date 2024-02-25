import hashlib

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    BackfillPolicy,
    Backoff,
    DataVersion,
    Jitter,
    Output,
    RetryPolicy,
    asset,
)
from luchtmeetnet_ingestion.IO.resources import LuchtMeetNetResource
from luchtmeetnet_ingestion.partitions import daily_partition, daily_station_partition
from pandas.util import hash_pandas_object


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
)
def air_quality_data(
    context: AssetExecutionContext, luchtmeetnet_api: LuchtMeetNetResource
) -> Output[pd.DataFrame]:
    date, station = context.partition_key.split("|")
    context.log.debug(date)
    context.log.debug(f"Fetching data for {date}")
    start, end = f"{date}T00:00:00", f"{date}T23:59:59"
    rp = {"start": start, "end": end, "station_number": station}
    df = pd.DataFrame(luchtmeetnet_api.request("measurements", context=context, request_params=rp))
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
        # Missing upstream partitions are
        "ingested_data": AssetIn(
            "air_quality_data",
            input_manager_key="landing_zone",
        )
    },
    code_version="v1",
)
def daily_air_quality_data(
    context: AssetExecutionContext, ingested_data: pd.DataFrame
) -> pd.DataFrame:
    return ingested_data
