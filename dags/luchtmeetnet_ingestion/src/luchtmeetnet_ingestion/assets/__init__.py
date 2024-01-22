import pandas as pd
from dagster import Backoff, Jitter, RetryPolicy, asset
from luchtmeetnet_ingestion.IO.resources import LuchtMeetNetResource
from luchtmeetnet_ingestion.partitions import daily_station_partition


@asset(
    description="Air quality data from the Luchtmeetnet API",
    compute_kind="duckdb",
    io_manager_key="landing_zone",
    partitions_def=daily_station_partition,
    retry_policy=RetryPolicy(
        max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS
    ),
    # auto_materialize_policy=AutoMaterializePolicy.eager()#max_materializations_per_minute=None),
)
def air_quality_data(context, luchtmeetnet_api: LuchtMeetNetResource) -> pd.DataFrame:
    date, station = context.partition_key.split("|")
    context.log.debug(date)
    context.log.debug(f"Fetching data for {date}")
    rp = {"start": f"{date}T00:00:00", "end": f"{date}T23:59:59", "station_number": station}
    df = pd.DataFrame(luchtmeetnet_api.request("measurements", request_params=rp))
    context.log.debug(df.head())
    return df
