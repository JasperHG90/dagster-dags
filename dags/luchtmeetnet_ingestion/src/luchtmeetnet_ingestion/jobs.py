import os

from dagster import define_asset_job, multiprocess_executor

# from dagster_k8s import k8s_job_executor
# from dagster_celery_k8s import celery_k8s_job_executor
from luchtmeetnet_ingestion.assets import air_quality_data
from luchtmeetnet_ingestion.partitions import daily_station_partition

environment = os.getenv("ENVIRONMENT", "dev")


ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=[air_quality_data],
    description="Ingestion job for air quality data",
    partitions_def=daily_station_partition,
    executor_def=multiprocess_executor,
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "100m", "memory": "64Mi"},
                    "limits": {"cpu": "100m", "memory": "64Mi"},
                },
            },
            "job_spec_config": {"ttl_seconds_after_finished": 7200},
        }
    },
)
