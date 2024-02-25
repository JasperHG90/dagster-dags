import pandas as pd
from dagster import (  # AutoMaterializePolicy,; AutoMaterializeRule,
    AssetExecutionContext,
    AssetIn,
    BackfillPolicy,
    Backoff,
    Jitter,
    RetryPolicy,
    asset,
)
from luchtmeetnet_ingestion.IO.resources import LuchtMeetNetResource
from luchtmeetnet_ingestion.partitions import daily_partition, daily_station_partition


@asset(
    description="Air quality data from the Luchtmeetnet API",
    compute_kind="duckdb",
    io_manager_key="landing_zone",
    partitions_def=daily_station_partition,
    retry_policy=RetryPolicy(
        max_retries=3, delay=15, backoff=Backoff.LINEAR, jitter=Jitter.PLUS_MINUS
    ),
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
)
def air_quality_data(
    context: AssetExecutionContext, luchtmeetnet_api: LuchtMeetNetResource
) -> pd.DataFrame:
    date, station = context.partition_key.split("|")
    context.log.debug(date)
    context.log.debug(f"Fetching data for {date}")
    start, end = f"{date}T00:00:00", f"{date}T23:59:59"
    rp = {"start": start, "end": end, "station_number": station}
    df = pd.DataFrame(
        luchtmeetnet_api.request(
            "measurements", partition_key=context.partition_key, request_params=rp
        )
    )
    df["station_number"] = station
    df["start"] = start
    df["end"] = end
    context.log.debug(df.head())
    return df


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
    # This asset automatically materializes when the upstream asset is materialized
    #  even if only a subset of the upstream partitions are materialized
    # auto_materialize_policy=AutoMaterializePolicy.eager(
    #     max_materializations_per_minute=None
    # ).without_rules(
    #     # If a partition is missing, this will still run
    #     AutoMaterializeRule.skip_on_parent_missing(),
    # ),
)
def daily_air_quality_data(context, ingested_data: pd.DataFrame) -> pd.DataFrame:
    return ingested_data
