import pandas as pd
from dagster import Backoff, Jitter, RetryPolicy, asset
from luchtmeetnet_ingestion.IO.resources import LuchtMeetNetResource
from luchtmeetnet_ingestion.partitions import daily_partition


@asset(
    description="Air quality data from the Luchtmeetnet API",
    compute_kind="duckdb",
    io_manager_key="landing_zone",
    partitions_def=daily_partition,
    retry_policy=RetryPolicy(
        max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS
    ),
    # auto_materialize_policy=AutoMaterializePolicy.eager()#max_materializations_per_minute=None),
)
def air_quality_data(context, luchtmeetnet_api: LuchtMeetNetResource):
    date = context.partition_key
    context.log.debug(f"Fetching data for {date}")
    rp = {
        "start": f"{date}T00:00:00",
        "end": f"{date}T23:59:59",
        "station_number": "NL01494",
    }
    df = pd.DataFrame(luchtmeetnet_api.request("measurements", request_params=rp))
    return df
