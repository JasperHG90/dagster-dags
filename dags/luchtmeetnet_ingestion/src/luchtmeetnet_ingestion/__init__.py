from importlib import metadata

from dagster import Definitions
from luchtmeetnet_ingestion.assets import (
    air_quality_data,
    air_quality_station_names,
    daily_air_quality_data,
    station_names,
)
from luchtmeetnet_ingestion.jobs import copy_to_data_lake_job, ingestion_job
from luchtmeetnet_ingestion.resource_definitions import env_resources, environment
from luchtmeetnet_ingestion.schedules import daily_schedule
from luchtmeetnet_ingestion.sensors import (
    run_copy_to_data_lake_after_ingestion,
    slack_message_on_failure,
    slack_message_on_success,
)

try:
    __version__ = metadata.version("luchtmeetnet_ingestion")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"


definition = Definitions(
    assets=[air_quality_data, daily_air_quality_data, station_names, air_quality_station_names],
    resources=env_resources[environment],
    jobs=[ingestion_job, copy_to_data_lake_job],
    sensors=[
        slack_message_on_failure,
        slack_message_on_success,
        run_copy_to_data_lake_after_ingestion,
    ],
    schedules=[daily_schedule],
)
