import os

from dagster import DagsterRunStatus, DefaultSensorStatus, SensorEvaluationContext
from dagster_slack import SlackResource
from dagster_utils.factories.sensors.trigger_job_run_status_sensor import (
    multi_to_single_partition_job_trigger_sensor,
)
from luchtmeetnet_ingestion.jobs import copy_to_data_lake_job, ingestion_job
from luchtmeetnet_ingestion.partitions import daily_partition, daily_station_partition

environment = os.getenv("ENVIRONMENT", "dev")


def my_message_fn(slack: SlackResource, message: str) -> str:
    slack.get_client().chat_postMessage(channel="#dagster-notifications", text=message)


def slack_message(context: SensorEvaluationContext, **kwargs):
    message = f"""
    Job "{kwargs.get('monitored_job_name')}" in repository "luchtmeetnet_ingestion", environment "{environment}"
and run key "{kwargs.get('run_key')}" has completed for {kwargs.get('partitions_total')} partitions:

    Successful partitions       => {kwargs.get('partitions_successful')}
    Failed partitions           => {kwargs.get('partitions_failed')}
    Upstream partition keys     => {kwargs.get('all_upstream_partitions')[:5]}... (truncated)
    Downstream partition key    => {kwargs.get('downstream_partition_key')}

Sensor "{context.sensor_name}" triggered downstream job "{kwargs.get('downstream_job_name')}"
with partition key "{kwargs.get('downstream_partition_key')}".
    """
    if environment == "dev":
        context.log.info(message)
    else:
        my_message_fn(context.resources.slack, message)


run_copy_to_data_lake_after_ingestion = multi_to_single_partition_job_trigger_sensor(
    name="run_copy_to_data_lake_after_ingestion",
    description="""Sensor that monitors the completion of the ingestion job and triggers the copy to data lake job.
This sensor only triggers after *all* partitions of the upstream job have finished, and sends a slack message
upon completion.""",
    monitored_asset="air_quality_data",
    monitored_job=ingestion_job,
    downstream_job=copy_to_data_lake_job,
    partitions_def_monitored_asset=daily_station_partition,
    partitions_def_downstream_asset=daily_partition,
    run_status=[DagsterRunStatus.SUCCESS],
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"slack"},
    time_window_seconds=90,
    skip_when_unfinished_count=30,
    callable_fn=slack_message,
)
