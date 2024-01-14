import os
from importlib import metadata

from dagster import Definitions, EnvVar
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
    | {"landing_zone": duckdb_parquet_io_manager.configured({"path": ".tmp/landing_zone"})},
    "prd": shared_resources
    | {
        "landing_zone": duckdb_parquet_io_manager.configured(
            {
                "path": "s3://inge-cst-euw4-jgdag-prd",
                "aws_access_key": EnvVar("GCS_ACCESS_KEY_ID"),
                "aws_secret_key": EnvVar("GCS_SECRET_ACCESS_KEY"),
                "aws_endpoint": "https://storage.googleapis.com",
            }
        )
    },
}

environment = os.getenv("ENVIRONMENT", "dev")

definition = Definitions(
    assets=[
        air_quality_data,
    ],
    resources=env_resources[environment],
    jobs=[ingestion_job],
)
