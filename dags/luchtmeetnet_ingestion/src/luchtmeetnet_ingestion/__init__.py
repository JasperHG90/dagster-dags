from importlib import metadata

from dagster import Definitions, FilesystemIOManager

from luchtmeetnet_ingestion.assets import air_quality_data
from luchtmeetnet_ingestion.IO.duckdb_io_manager import duckdb_parquet_io_manager
from luchtmeetnet_ingestion.IO.resources import LuchtMeetNetResource
from luchtmeetnet_ingestion.jobs import ingestion_job

try:
    __version__ = metadata.version("luchtmeetnet_ingestion")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"


shared_resources = {"luchtmeetnet_api": LuchtMeetNetResource()}

env_resources = {
    "dev": shared_resources
    | {"landing_zone": FilesystemIOManager()} #{"landing_zone": duckdb_parquet_io_manager.configured({"path": ".tmp/landing_zone"})}
}

definition = Definitions(
    assets=[
        air_quality_data,
    ],
    resources=env_resources["dev"],
    jobs=[ingestion_job],
)
