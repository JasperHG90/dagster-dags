import pandas as pd
from dagster import AssetExecutionContext, Failure
from luchtmeetnet_ingestion.IO.resources import LuchtMeetNetResource
from requests import HTTPError


def get_air_quality_data_for_partition_key(
    partition_key: str, context: AssetExecutionContext, luchtmeetnet_api: LuchtMeetNetResource
) -> pd.DataFrame:
    date, station = partition_key.split("|")
    context.log.debug(date)
    context.log.debug(f"Fetching data for {date}")
    start, end = f"{date}T00:00:00", f"{date}T23:59:59"
    rp = {"start": start, "end": end, "station_number": station}
    try:
        df = pd.DataFrame(
            luchtmeetnet_api.request("measurements", context=context, request_params=rp)
        )
    # We don't want to keep retrying for a station that is raising code 500
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
    return df
