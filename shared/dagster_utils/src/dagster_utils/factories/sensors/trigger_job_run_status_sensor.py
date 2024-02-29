import logging
import typing

import pendulum
from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    JobDefinition,
    MultiPartitionsDefinition,
    PartitionsDefinition,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from dagster_utils.factories.base import DagsterObjectFactory
from dagster_utils.factories.sensors.utils import (
    MonitoredJobSensorMixin,
    PartitionResolver,
)


# TODO: add timeout for jobs in which partitions don't resolve in time, add tests
class MultiToSinglePartitionJobTriggerSensorFactory(DagsterObjectFactory, MonitoredJobSensorMixin):
    def __init__(
        self,
        name: str,
        monitored_asset: str,
        monitored_job: JobDefinition,
        downstream_job: JobDefinition,
        partitions_def_monitored_asset: MultiPartitionsDefinition,
        partitions_def_downstream_asset: PartitionsDefinition,
        run_status: typing.List[DagsterRunStatus] = [DagsterRunStatus.SUCCESS],
        minimum_interval_seconds: typing.Optional[int] = None,
        default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED,
        time_window_seconds: int = 120,
        skip_when_unfinished_count: int = 15,
        description: typing.Optional[str] = None,
    ):
        """A sensor that monitors a partitioned asset in a job and triggers another job when the assets are materialized.

        This works for when the upstream partition has more partitions than the downstream asset. This sensor
        always triggers downstream jobs even when some upstream partitions have failed. This sensor does not trigger
        on reported asset materializations (e.g. registered from existing data).

        Args:
            name (str): name of the sensor
            monitored_asset (str): name of the asset in the job that is monitored
            monitored_job (JobDefinition): name of the job that is monitored
            downstream_job (JobDefinition): name of the downstream job that should be monitored
            partitions_def_monitored_asset (MultiPartitionsDefinition): Partitions definition of the monitored asset. Should be multipartitioned.
            partitions_def_downstream_asset (PartitionsDefinition): Partitions definition of the downstream asset. Should be single partitioned.
            run_status (DagsterRunStatus): the condition that triggers the sensor
            minimum_interval_seconds (typing.Optional[int], optional): The minimum number of seconds that will elapse between sensor evaluations. Defaults to None.
            default_status (DefaultSensorStatus, optional): Default status of the sensor. Defaults to DefaultSensorStatus.STOPPED.
            time_window_seconds (int, optional): The window (in seconds) that is used to monitor events. Defaults to 120.
            skip_when_unfinished_count (int, optional): The number of times we skip evaluating a run record if we know that only x/n upstream partitions have been processed. Defaults to 15.
            description (typing.Optional[str], optional): Description of this sensor. Defaults to None.
        """
        super().__init__(name, description)
        self.monitored_asset = monitored_asset
        self.monitored_job = monitored_job
        self.downstream_job = downstream_job
        self.partitions_def_monitored_asset = partitions_def_monitored_asset
        self.partitions_def_downstream_asset = partitions_def_downstream_asset
        self.run_status = run_status
        self.time_window_seconds = time_window_seconds
        self.skip_when_unfinished_count = skip_when_unfinished_count
        self.minimum_interval_seconds = minimum_interval_seconds
        self.default_status = default_status
        self.partition_mapper = PartitionResolver(
            upstream_partition=partitions_def_monitored_asset,
            downstream_partition=partitions_def_downstream_asset,
        )
        self._logger: logging.Logger = logging.getLogger(
            "dagster_utils.factories.sensors.MultiToSinglePartitionJobTriggerSensorFactory"
        )
        self.run_key_requests_this_sensor: typing.List[str] = []
        self.unfinished_downstream_partitions: typing.Dict[str, int] = {}

    def _get_mapped_downstream_partition_key_from_upstream_partition_key(
        self, upstream_partition_key: str
    ) -> str:
        return self.partition_mapper.map_upstream_to_downstream_partitions(
            [upstream_partition_key]
        )[upstream_partition_key]

    def _get_mapped_upstream_partition_key_from_downstream_partition_key(
        self, downstream_partition_key: str
    ) -> typing.List[str]:
        return self.partition_mapper.map_downstream_to_upstream_partitions(
            [downstream_partition_key]
        )[downstream_partition_key]

    def __call__(self) -> SensorDefinition:
        @sensor(
            name=self.name,
            description=self.description,
            job=self.downstream_job,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=self.default_status,
        )
        def _sensor(context: SensorEvaluationContext):
            run_requests = 0
            job_name = self.monitored_job.name
            context.log.debug(f"Job name: {job_name}")
            run_records = self._get_run_records_for_job(context.instance)
            cursor = float(context.cursor) if context.cursor else float(0)
            ts = cursor - 2  # margin
            for run_record in run_records:
                if self._run_record_end_before_cursor_ts(run_record.end_time, ts):
                    context.log.debug(f"Skipping run for record {run_record.dagster_run.run_id}")
                    continue
                # From the partition key, get the upstream partition key
                downstream_partition_key = (
                    self._get_mapped_downstream_partition_key_from_upstream_partition_key(
                        run_record.dagster_run.tags.get("dagster/partition")
                    )
                )
                context.log.debug(f"Downstream partition key: {downstream_partition_key}")
                # Now, map the upstream key back to all downstream partitions
                all_upstream_partitions = (
                    self._get_mapped_upstream_partition_key_from_downstream_partition_key(
                        downstream_partition_key
                    )
                )
                backfill_name = self._get_backfill_name(run_record.dagster_run.tags)
                scheduled_run_name = self._get_backfill_name(run_record.dagster_run.tags)
                if (scheduled_run_name is None and backfill_name is None) and (
                    scheduled_run_name is not None and backfill_name is not None
                ):
                    raise ValueError(
                        "Exactly one of 'dagster/backfill' or 'dagster/schedule_name' tags must be present in the run record."
                    )
                if backfill_name is not None:
                    all_upstream_partitions = self._get_backfill_partitions(
                        context.instance, backfill_name, all_upstream_partitions
                    )
                run_key = f"{downstream_partition_key}_{backfill_name if backfill_name is not None else scheduled_run_name}"
                if self._increment_unfinished_downstream_partitions(
                    run_key
                ) or self._sensor_already_triggered_with_run_key(run_key):
                    continue
                # NB: this calls the postgres db, so don't want to call it in above lines since these are just lookups and
                #  as such much faster
                if self._run_key_already_completed(context, run_key, context.instance):
                    continue
                else:
                    (
                        num_total,
                        num_successful,
                        num_failed,
                        num_done,
                        num_unfinished,
                    ) = self._get_job_statistics(
                        instance=context.instance,
                        partitions=all_upstream_partitions,
                        backfill_name=backfill_name,
                        scheduled_run_name=scheduled_run_name,
                    )
                    context.log.debug(
                        f"Total: {num_total}, Successful: {num_successful}, Failed: {num_failed}, Done: {num_done}, Unfinished: {num_unfinished}"
                    )
                if num_unfinished > 0:
                    context.log.debug(
                        f"Only {num_done} out of {num_total} partitions have been materialized for partition {downstream_partition_key}. Skipping . . ."
                    )
                    self.unfinished_downstream_partitions[run_key] = 0
                    continue
                else:
                    if num_failed > 0:
                        context.log.warning(
                            f"{num_failed} partitions failed to materialize for partition {downstream_partition_key}. Will still run downstream task."
                        )
                    yield RunRequest(run_key=run_key, partition_key=downstream_partition_key)
                    self.run_key_requests_this_sensor.append(run_key)
                    run_requests += 1

            if run_requests > 0:
                new_ts = max(pendulum.now(tz="UTC").timestamp(), cursor)
                context.log.debug(f"Setting cursor to {new_ts}")
            else:
                new_ts = cursor

            context.update_cursor(str(new_ts))

            if run_requests == 0:
                yield SkipReason("No runs requested")

        return _sensor
