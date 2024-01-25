import os

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunStatusSensorContext,
    SensorResult,
    run_status_sensor,
    sensor,
)
from dagster_slack import SlackResource
from luchtmeetnet_ingestion.IO.resources import LuchtMeetNetResource
from luchtmeetnet_ingestion.jobs import ingestion_job
from luchtmeetnet_ingestion.partitions import stations_partition

environment = os.getenv("ENVIRONMENT", "dev")


def my_message_fn(slack: SlackResource, message: str) -> str:
    slack.get_client().chat_postMessage(channel="#dagster-notifications", text=message)


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[ingestion_job],
    default_status=DefaultSensorStatus.RUNNING,
)
def slack_message_on_success(context: RunStatusSensorContext, slack: SlackResource):
    message = f"Job {context.dagster_run.job_name} succeeded!"
    if environment == "dev":
        context.log.info(message)
    else:
        my_message_fn(slack, message)


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    monitored_jobs=[ingestion_job],
    default_status=DefaultSensorStatus.RUNNING,
)
def slack_message_on_failure(context: RunFailureSensorContext, slack: SlackResource):
    message = (
        f"Job {context.dagster_run.job_name} failed!" f"Error: {context.failure_event.message}"
    )
    if environment == "dev":
        context.log.info(message)
    else:
        my_message_fn(slack, message)


@sensor(job=ingestion_job)
def stations_sensor(context, luchtmeetnet_api: LuchtMeetNetResource):
    # Only take first three stations for demo purposes
    stations_request = luchtmeetnet_api.request("stations")[:10]
    context.log.debug(stations_request)
    stations = [
        f["number"]
        for f in stations_request
        if not context.instance.has_dynamic_partition(stations_partition.name, f["number"])
    ]
    context.log.debug(stations)
    return SensorResult(
        run_requests=None,
        dynamic_partitions_requests=[stations_partition.build_add_request(stations)],
    )
