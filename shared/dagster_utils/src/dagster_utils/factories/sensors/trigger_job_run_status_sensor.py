import typing

import pendulum
from dagster import (  # RunStatusSensorContext,; RunStatusSensorDefinition,; run_status_sensor,
    AssetKey,
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    EventRecordsFilter,
    JobDefinition,
    PartitionsDefinition,
    RunRequest,
    RunsFilter,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from dagster_utils.factories.base import DagsterObjectFactory
from dagster_utils.factories.sensors.utils import PartitionResolver


class PartitionedJobSensorFactory(DagsterObjectFactory):
    def __init__(
        self,
        name: str,
        monitored_asset: str,
        monitored_job: JobDefinition,
        downstream_job: JobDefinition,
        partitions_def_monitored_asset: PartitionsDefinition,
        partitions_def_downstream_asset: PartitionsDefinition,
        run_status: DagsterRunStatus,
        minimum_interval_seconds: typing.Optional[int] = None,
        default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED,
        description: typing.Optional[str] = None,
    ):
        """A sensor that monitors a partitioned asset in a job and triggers another job when the assets are materialized.

        This works for assets that have the same partition, or when the downstream asset shares a partition. This sensor
        always triggers downstream jobs even when some upstream partitions have failed.

        Args:
            name (str): name of the sensor
            monitored_asset (str): name of the asset in the job that is monitored
            monitored_job (JobDefinition): name of the job that is monitored
            downstream_job (JobDefinition): name of the downstream job that should be monitored
            partitions_def_monitored_asset (PartitionsDefinition): Partitions definition of the monitored asset.
            minimum_interval_seconds (typing.Optional[int], optional): The minimum number of seconds that will elapse between sensor evaluations. Defaults to None.
            default_status (DefaultSensorStatus, optional): Default status of the sensor. Defaults to DefaultSensorStatus.STOPPED.
            run_status (DagsterRunStatus): the condition that triggers the sensor
            description (typing.Optional[str], optional): Description of this sensor. Defaults to None.
        """
        super().__init__(name, description)
        self.monitored_asset = monitored_asset
        self.monitored_job = monitored_job
        self.downstream_job = downstream_job
        self.partitions_def_monitored_asset = partitions_def_monitored_asset
        self.partitions_def_downstream_asset = partitions_def_downstream_asset
        self.run_status = run_status
        self.minimum_interval_seconds = minimum_interval_seconds
        self.default_status = default_status
        self.partition_mapper = PartitionResolver(
            upstream_partition=partitions_def_monitored_asset,
            downstream_partition=partitions_def_downstream_asset,
        )

    def __call__(self) -> SensorDefinition:
        # @run_status_sensor(
        #     name=self.name,
        #     description=self.description,
        #     run_status=self.run_status,
        #     monitored_jobs=[self.monitored_job],
        #     request_job=self.downstream_job,
        #     minimum_interval_seconds=self.minimum_interval_seconds,
        #     default_status=self.default_status,
        # )

        @sensor(
            name=self.name,
            description=self.description,
            job=self.downstream_job,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=self.default_status,
        )
        def _sensor(context: SensorEvaluationContext):
            run_requests = 0
            time_window_start = pendulum.now() - pendulum.duration(seconds=120)  # Make configurable
            run_records = context.instance.get_run_records(
                filters=RunsFilter(
                    job_name=self.monitored_job,
                    statuses=[DagsterRunStatus.SUCCESS],
                    updated_after=time_window_start,
                ),
                order_by="update_timestamp",
                ascending=False,
            )
            for run_record in run_records:
                partition_key = run_record.dagster_run.tags.get("dagster/partition")
                # From the partition key, get the upstream partition key
                downstream_partition_key = (
                    self.partition_mapper.map_upstream_to_downstream_partitions([partition_key])[
                        partition_key
                    ]
                )
                # Now, map the upstream key back to all downstream partitions
                all_upstream_partitions = (
                    self.partition_mapper.map_downstream_to_upstream_partitions(
                        [downstream_partition_key]
                    )[downstream_partition_key]
                )
                is_backfill = (
                    False if run_record.dagster_run.tags.get("dagster/backfill") is None else True
                )
                is_scheduled_runs = (
                    False
                    if run_record.dagster_run.tags.get("dagster/schedule_name") is None
                    else True
                )
                context.log.debug(f"Is backfill: {is_backfill}")
                context.log.debug(f"Is scheduled run: {is_scheduled_runs}")
                if is_backfill:
                    backfill = context.instance.get_backfill(
                        run_record.dagster_run.tags.get("dagster/backfill")
                    )
                    # Backfill partitions for all upstream partitions of this run
                    all_upstream_partitions = [
                        partition
                        for partition in backfill.partition_names
                        if partition in all_upstream_partitions
                    ]
                    context.log.debug(f"Backfill: {backfill.backfill_id}")
                    group_name = run_record.dagster_run.tags.get("dagster/backfill")
                    context.log.debug(f"Backfill: {group_name}")
                else:
                    group_name = run_record.dagster_run.tags.get("dagster/schedule_name")
                    context.log.debug(f"Schedule: {group_name}")
                run_key = f"{downstream_partition_key}_{group_name}"
                run_key_completed = context.instance.get_runs(
                    filters=RunsFilter(tags={"dagster/run_key": run_key})
                )
                if len(run_key_completed) > 0:
                    context.log.debug(
                        f"Run for partition {downstream_partition_key} already exists. Skipping . . ."
                    )
                    continue
                else:
                    # Retrieve materialization & failed runs information for partitions
                    filter_materialized_assets = EventRecordsFilter(
                        event_type=DagsterEventType.ASSET_MATERIALIZATION,
                        asset_key=AssetKey(self.monitored_asset),
                        asset_partitions=all_upstream_partitions,
                    )
                    events_successful_materializations = context.instance.get_event_records(
                        filter_materialized_assets
                    )
                    # NB: cannot filter for asset
                    filter_failed_pipelines = EventRecordsFilter(
                        event_type=DagsterEventType.PIPELINE_FAILURE,
                        after_timestamp=(
                            pendulum.now() - pendulum.duration(minutes=60)
                        ).timestamp(),  # Make configurable
                    )
                    filter_events_failed = context.instance.get_event_records(
                        filter_failed_pipelines
                    )
                    filter_runs_failed = RunsFilter(
                        [event.run_id for event in filter_events_failed]
                    )
                    runs_failed = context.instance.get_runs(filters=filter_runs_failed)
                    if is_backfill:
                        runs_failed_run_key = [
                            run
                            for run in runs_failed
                            if run.tags.get("dagster/backfill") == group_name
                        ]
                    else:
                        runs_failed_run_key = [
                            run
                            for run in runs_failed
                            if run.tags.get("dagster/schedule_name") == group_name
                        ]
                    num_total = len(all_upstream_partitions)
                    num_successful = len(events_successful_materializations)
                    num_failed = len(runs_failed_run_key)
                    num_done = num_successful + num_failed
                    num_unfinished = num_total - num_done
                if num_unfinished > 0:
                    context.log.debug(
                        f"Only {num_done} out of {num_total} partitions have been materialized for partition {downstream_partition_key}. Skipping . . ."
                    )
                    continue
                else:
                    if num_failed > 0:
                        context.log.warning(
                            f"{num_failed} partitions failed to materialize for partition {downstream_partition_key}. Will still run downstream task."
                        )
                    yield RunRequest(run_key=run_key, partition_key=downstream_partition_key)
                    run_requests += 1

            if run_requests == 0:
                yield SkipReason("No runs requested")

        return _sensor
