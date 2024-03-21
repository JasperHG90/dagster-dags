import os
import typing

from dagster import HookContext, define_asset_job, multiprocess_executor
from dagster_utils.factories.hooks import metric_job_success_hook_factory
from luchtmeetnet_ingestion.assets import air_quality_data, daily_air_quality_data
from luchtmeetnet_ingestion.partitions import daily_partition, daily_station_partition

environment = os.getenv("ENVIRONMENT", "dev")


def post_metric(context: HookContext, value: int, labels: typing.Dict[str, str]):
    context.log.debug(labels)
    if environment == "dev":
        context.log.info("Skipping posting metric in dev environment")
    else:
        context.resources.gcp_metrics.post_time_series(
            series_type="custom.googleapis.com/dagster/job_success",
            value={"bool_value": value},
            metric_labels=labels,
        )


gcp_metric_on_success = metric_job_success_hook_factory(
    name="gcp_metric_on_success",
    description="GCP metric hook for success",
    on_success=True,
    callable_fn=post_metric,
    required_resource_keys={"gcp_metrics"},
)


gcp_metric_on_failure = metric_job_success_hook_factory(
    name="gcp_metric_on_failure",
    description="GCP metric hook for failure",
    callable_fn=post_metric,
    required_resource_keys={"gcp_metrics"},
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
