import os

from dagster import (
    AssetKey,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunRequest,
    RunStatusSensorContext,
    SensorResult,
    SkipReason,
    run_failure_sensor,
    run_status_sensor,
    sensor,
)
from dagster_slack import SlackResource
from luchtmeetnet_ingestion.IO.resources import LuchtMeetNetResource
from luchtmeetnet_ingestion.jobs import copy_to_data_lake_job, ingestion_job
from luchtmeetnet_ingestion.partitions import (
    daily_station_partition,
    stations_partition,
)

environment = os.getenv("ENVIRONMENT", "dev")


def my_message_fn(slack: SlackResource, message: str) -> str:
    slack.get_client().chat_postMessage(channel="#dagster-notifications", text=message)


@run_status_sensor(
    description="Run copy to data lake job after ingestion",
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[ingestion_job],
    request_job=copy_to_data_lake_job,
    default_status=DefaultSensorStatus.RUNNING,
)
def run_copy_to_data_lake_after_ingestion(context: RunStatusSensorContext):
    # Also check for tag of registering known materializations
    context.log.debug(f"Dagster event: {context.dagster_event}")
    context.log.debug(f"Dagster run: {context.dagster_run}")
    is_backfill = False if context.dagster_run.tags.get("dagster/backfill") is None else True
    context.log.debug(f"Is backfill: {is_backfill}")
    if is_backfill:
        backfill = context.instance.get_backfill(context.dagster_run.tags.get("dagster/backfill"))
        context.log.debug(f"Backfill: {backfill}")
        partitions = backfill.partition_names
        context.log.debug(f"Backfill partitions: {partitions}")
    else:
        # Ordered by partition name
        partition_key = context.partition_key
        date, _ = partition_key.split("|")
        partitions = [f"{date}|{s}" for s in stations_partition.get_partition_keys()]
        context.log.debug(f"Partitions: {partitions}")
    status_by_partition = context.instance.get_status_by_partition(
        AssetKey("air_quality_data"),
        partition_keys=partitions,
        partitions_def=daily_station_partition,
    )
    context.log.debug(f"Status by partition: {status_by_partition}")
    num_total = len(status_by_partition)
    status_by_partition_filtered = {k: v for k, v in status_by_partition.items() if v is not None}
    num_done = sum(
        [
            True if status.value in ["FAILED", "MATERIALIZED"] else False
            for _, status in status_by_partition_filtered.items()
        ]
    )
    num_failed = sum(
        [
            True if status.value in ["FAILED"] else False
            for _, status in status_by_partition_filtered.items()
        ]
    )
    context.log.debug(f"Total: {num_total}, Done: {num_done}, Failed: {num_failed}")
    successful_partitions = [
        key
        for key, status in status_by_partition_filtered.items()
        if status.value == "MATERIALIZED"
    ]
    context.log.debug(f"Successful partitions: {successful_partitions}")
    if num_done < num_total:
        context.log.debug(
            f"Only {num_done} out of {num_total} partitions have been materialized. Skipping . . ."
        )
        yield SkipReason(
            f"Only {num_done} out of {num_total} partitions have been materialized. Waiting until all partitions have been materialized."
        )
    else:
        if num_failed > 0:
            context.log.warning(
                f"{num_failed} partitions failed to materialize. Will still run downstream task."
            )
        if is_backfill:
            unique_dates = list(set([key.split("|")[0] for key in successful_partitions]))
            context.log.debug(f"Requesting run for dates {unique_dates}")
            for date in unique_dates:
                run_key = f"{date}_{context.dagster_run.tags.get('dagster/backfill')}"
                context.log.debug(f"Run key: {run_key}")
                yield RunRequest(run_key=run_key, partition_key=date)
        else:
            date, _ = context.partition_key.split("|")
            context.log.debug(f"Requesting run for date {date}")
            context.log.debug(f"Run key: {date}")
            yield RunRequest(run_key=date, partition_key=date)


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


@sensor(job=ingestion_job)
def stations_sensor(context, luchtmeetnet_api: LuchtMeetNetResource):
    # Only take first three stations for demo purposes
    stations_request = luchtmeetnet_api.request("stations")
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
