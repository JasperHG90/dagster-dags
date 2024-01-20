import os
from importlib import metadata

from dagster import Definitions, EnvVar
from dagster_slack import SlackResource
from dagster_utils.IO.duckdb_io_manager import duckdb_parquet_io_manager
from luchtmeetnet_ingestion.assets import air_quality_data
from luchtmeetnet_ingestion.IO.resources import LuchtMeetNetResource
from luchtmeetnet_ingestion.jobs import ingestion_job
from luchtmeetnet_ingestion.sensors import (
    slack_message_on_failure,
    slack_message_on_success,
)

try:
    __version__ = metadata.version("luchtmeetnet_ingestion")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"


shared_resources = {
    "luchtmeetnet_api": LuchtMeetNetResource(),
    # NB: on dev, this hook is not used. See 'sensors.py' for implementation
    #  since the hooks depend on a SlackResource, we need to define it here
    "slack": SlackResource(token=EnvVar("DAGSTER_SECRET_SLACK_BOT_OAUTH_TOKEN")),
}

env_resources = {
    "dev": shared_resources
    | {"landing_zone": duckdb_parquet_io_manager.configured({"path": ".tmp/landing_zone"})},
    "prd": shared_resources
    | {
        "landing_zone": duckdb_parquet_io_manager.configured(
            {
                "path": "s3://inge-cst-euw4-jgdag-prd",
                "aws_access_key": {"env": "GCS_ACCESS_KEY_ID"},
                "aws_secret_key": {"env": "GCS_SECRET_ACCESS_KEY"},
                "aws_endpoint": "storage.googleapis.com",
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
    sensors=[slack_message_on_failure, slack_message_on_success],
)
