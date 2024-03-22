from importlib import metadata

from dagster import Definitions
from luchtmeetnet_ingestion.assets import (
    air_quality_data,
    air_quality_station_names,
    daily_air_quality_data,
    station_names,
)
from luchtmeetnet_ingestion.resource_definitions import env_resources, environment

try:
    __version__ = metadata.version("luchtmeetnet_ingestion")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"


definition = Definitions(
    assets=[air_quality_data, daily_air_quality_data, station_names, air_quality_station_names],
    resources=env_resources[environment],
)
