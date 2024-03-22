import os

from dagster import EnvVar
from dagster_utils.IO.duckdb_io_manager import duckdb_parquet_io_manager
from dagster_utils.IO.gcp_metrics import GcpMetricsResource
from luchtmeetnet_ingestion.IO.resources import (
    LuchtMeetNetResource,
    RateLimiterResource,
    RedisResource,
)

environment = os.getenv("ENVIRONMENT", "dev")

shared_resources = {
    "luchtmeetnet_api": LuchtMeetNetResource(
        rate_limiter=RateLimiterResource(  # See https://api-docs.luchtmeetnet.nl/ for rate limits
            rate_calls=100,
            rate_minutes=5,
            bucket_key=f"luchtmeetnet_api_{environment}",
            redis=RedisResource(
                host=EnvVar("DAGSTER_SECRET_REDIS_HOST"),
                port=16564,
                password=EnvVar("DAGSTER_SECRET_REDIS_PASSWORD"),
                username=EnvVar("DAGSTER_SECRET_REDIS_USERNAME"),
            ),
        )
    ),
    "gcp_metrics": GcpMetricsResource(environment=environment, project_id="jasper-ginn-dagster"),
}

env_resources = {
    "dev": shared_resources
    | {
        # When loading data from landing zone, we ignore any missing partitions
        "landing_zone": duckdb_parquet_io_manager.configured(
            {"path": ".tmp/landing_zone", "ignore_missing_partitions_on_load": True}
        ),
        "data_lake_bronze": duckdb_parquet_io_manager.configured({"path": ".tmp/data_lake/bronze"}),
    },
    "prd": shared_resources
    | {
        "landing_zone": duckdb_parquet_io_manager.configured(
            {
                "path": "s3://inge-cst-euw4-jgdag-prd",
                "aws_access_key": {"env": "GCS_ACCESS_KEY_ID"},
                "aws_secret_key": {"env": "GCS_SECRET_ACCESS_KEY"},
                "aws_endpoint": "storage.googleapis.com",
                "ignore_missing_partitions_on_load": True,
            }
        ),
        "data_lake_bronze": duckdb_parquet_io_manager.configured(
            {
                "path": "s3://dala-cst-euw4-jgdag-prd/bronze",
                "aws_access_key": {"env": "GCS_ACCESS_KEY_ID"},
                "aws_secret_key": {"env": "GCS_SECRET_ACCESS_KEY"},
                "aws_endpoint": "storage.googleapis.com",
            }
        ),
        "gcp_metrics": GcpMetricsResource(
            environment=environment, project_id="jasper-ginn-dagster"
        ),
    },
}
