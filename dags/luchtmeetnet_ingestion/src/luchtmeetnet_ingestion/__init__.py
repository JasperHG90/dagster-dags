from importlib import metadata

from dagster import Definitions
from luchtmeetnet_ingestion.assets import air_quality_data, daily_air_quality_data
from luchtmeetnet_ingestion.jobs import copy_to_data_lake_job, ingestion_job
from luchtmeetnet_ingestion.resource_definitions import env_resources, environment
from luchtmeetnet_ingestion.schedules import daily_schedule
from luchtmeetnet_ingestion.sensors import (
    slack_message_on_failure,
    slack_message_on_success,
)

try:
    __version__ = metadata.version("luchtmeetnet_ingestion")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"


definition = Definitions(
    assets=[air_quality_data, daily_air_quality_data],
    resources=env_resources[environment],
    jobs=[ingestion_job, copy_to_data_lake_job],
    sensors=[slack_message_on_failure, slack_message_on_success],
    schedules=[daily_schedule],
)
