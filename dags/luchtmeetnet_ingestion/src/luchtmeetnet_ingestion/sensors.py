import os
import typing

import pendulum
from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunsFilter,
    SensorEvaluationContext,
    sensor,
)
from dagster_utils.IO.gcp_metrics import GcpMetricsResource

environment = os.getenv("ENVIRONMENT", "dev")


def parse_run_trigger(tags: typing.Dict[str, str]) -> typing.Dict[str, str]:
    if tags.get("dagster/backfill") is not None:
        trigger_type = "backfill"
        trigger_name = tags["dagster/backfill"]
    elif tags.get("dagster/schedule_name") is not None:
        trigger_type = "schedule"
        trigger_name = tags["dagster/schedule_name"]
    elif tags.get("dagster/auto_materialize"):
        trigger_type = "auto_materialize"
        trigger_name = "N/A"
    else:
        trigger_type = "manual"
        trigger_name = "N/A"
    return {
        "trigger_type": trigger_type,
        "trigger_name": trigger_name,
    }


@sensor(
    description="This sensor retrieves recent run records and posts metrics to GCP.",
    default_status=DefaultSensorStatus.RUNNING,
)
def post_gcp_metrics(context: SensorEvaluationContext, gcp_metrics: GcpMetricsResource):
    cursor = float(context.cursor) if context.cursor else float(0)
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE],
            updated_after=pendulum.from_timestamp(cursor, tz="UTC"),
        ),
    )
    context.log.info(f"Found {len(run_records)} runs")

    max_run_ts = cursor
    for run_record in run_records:
        labels = {
            "asset_name": list(run_record.dagster_run.asset_selection)[0].to_user_string(),
            "run_id": run_record.dagster_run.run_id,
            "partition_key": run_record.dagster_run.tags.get("dagster/partition", "N/A"),
            "environment": environment,
            **parse_run_trigger(run_record.dagster_run.tags),
        }
        gcp_metrics.post_time_series(
            series_type="custom.googleapis.com/dagster/job_success",
            value={"bool_value": 1 if run_record.dagster_run.is_success else 0},
            metric_labels=labels,
            timestamp=run_record.end_time,
        )
        max_run_ts = max(max_run_ts, run_record.end_time)
    if max_run_ts != cursor:
        context.log.debug(f"Setting cursor to {max_run_ts}")
        context.update_cursor(str(max_run_ts))
