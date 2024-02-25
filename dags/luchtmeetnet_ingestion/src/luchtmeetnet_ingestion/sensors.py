import os

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunStatusSensorContext,
    run_failure_sensor,
    run_status_sensor,
)
from dagster_slack import SlackResource
from dagster_utils.factories.sensors.partitioned_asset_sensor import (
    PartitionedAssetSensorFactory,
)
from luchtmeetnet_ingestion.jobs import copy_to_data_lake_job, ingestion_job
from luchtmeetnet_ingestion.partitions import daily_station_partition

environment = os.getenv("ENVIRONMENT", "dev")


def my_message_fn(slack: SlackResource, message: str) -> str:
    slack.get_client().chat_postMessage(channel="#dagster-notifications", text=message)


run_copy_to_data_lake_after_ingestion = PartitionedAssetSensorFactory(
    name="run_copy_to_data_lake_after_ingestion",
    monitored_asset="air_quality_data",
    downstream_asset="daily_air_quality_data",
    job=copy_to_data_lake_job,
    partitions_def_monitored_asset=daily_station_partition,
    require_all_partitions_monitored_asset=False,
    default_status=DefaultSensorStatus.RUNNING,
)()


@run_status_sensor(
    description="Slack message on success",
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[ingestion_job],
    default_status=DefaultSensorStatus.RUNNING,
)
def slack_message_on_success(context: RunStatusSensorContext, slack: SlackResource):
    message = f'Job "{context.dagster_run.job_name}" with run ID "{context.dagster_run.run_id}" succeeded!'
    if environment == "dev":
        context.log.info(message)
    else:
        my_message_fn(slack, message)


@run_failure_sensor(
    description="Slack message on failure",
    monitored_jobs=[ingestion_job],
    default_status=DefaultSensorStatus.RUNNING,
)
def slack_message_on_failure(context: RunFailureSensorContext, slack: SlackResource):
    message = f'Job "{context.dagster_run.job_name}" with ID "{context.dagster_run.run_id}" failed. Error Message: {context.failure_event.message}"'
    if environment == "dev":
        context.log.info(message)
    else:
        my_message_fn(slack, message)
