from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunStatusSensorContext,
    run_status_sensor,
)
from dagster_slack import SlackResource
from luchtmeetnet_ingestion.jobs import ingestion_job


def my_message_fn(slack: SlackResource, message: str) -> str:
    slack.get_client().chat_postMessage(channel="#dagster-notifications", text=message)


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[ingestion_job],
    default_status=DefaultSensorStatus.RUNNING,
)
def slack_message_on_success(context: RunStatusSensorContext, slack: SlackResource):
    message = f"Job {context.dagster_run.job_name} succeeded!"
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
    my_message_fn(slack, message)
