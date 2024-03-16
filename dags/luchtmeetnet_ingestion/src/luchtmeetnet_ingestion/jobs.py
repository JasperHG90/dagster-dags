import os

from dagster import (
    HookContext,
    define_asset_job,
    failure_hook,
    multiprocess_executor,
    success_hook,
)
from luchtmeetnet_ingestion.assets import air_quality_data, daily_air_quality_data
from luchtmeetnet_ingestion.partitions import daily_partition, daily_station_partition

environment = os.getenv("ENVIRONMENT", "dev")


@success_hook(
    name="job_success_gcp_metric",
    required_resource_keys={"gcp_metrics"},
)
def gcp_metric_on_success(context: HookContext):
    context.resources.gcp_metrics.post_time_series(
        series_type="custom.googleapis.com/dagster/job_success",
        value={"bool_value": 1},
        metric_labels={
            "job_name": context.job_name,
            "run_id": context.run_id,
        },
    )


@failure_hook(
    name="job_failure_gcp_metric",
    required_resource_keys={"gcp_metrics"},
)
def gcp_metric_on_failure(context: HookContext):
    context.resources.gcp_metrics.post_time_series(
        series_type="custom.googleapis.com/dagster/job_success",
        value={"bool_value": 0},
        metric_labels={
            "job_name": context.job_name,
            "run_id": context.run_id,
        },
    )


ingestion_job = define_asset_job(
    name="ingestion",
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
    hooks={gcp_metric_on_success, gcp_metric_on_failure},
)


copy_to_data_lake_job = define_asset_job(
    name="copy_to_data_lake",
    selection=[daily_air_quality_data],
    description="Copy ingested air quality data to bronze layer of the data lake",
    partitions_def=daily_partition,
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
    hooks={gcp_metric_on_success, gcp_metric_on_failure},
)
