import typing

from dagster import (
    JobDefinition,
    PartitionsDefinition,
    DefaultSensorStatus,
    RunRequest,
    SkipReason,
    DagsterRunStatus,
    run_status_sensor,
    RunStatusSensorContext,
    RunStatusSensorDefinition
)

from dagster_utils.factories.base import DagsterObjectFactory
from dagster_utils.factories.sensors.utils import _get_materialization_info


class PartitionedJobSensorFactory(DagsterObjectFactory):

    def __init__(
        self,
        name: str,
        monitored_asset: str,
        monitored_job: JobDefinition,
        downstream_job: JobDefinition,
        partitions_def_monitored_asset: PartitionsDefinition,
        run_status: DagsterRunStatus,
        minimum_interval_seconds: typing.Optional[int] = None,
        default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED,
        description: typing.Optional[str] = None,
    ):
        """A sensor that monitors a partitioned asset in a job and triggers another job when the assets are materialized.

        This works for assets that have the same partitioned, or when the downstream asset shares a partition. This sensor
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
        self.run_status = run_status
        self.minimum_interval_seconds = minimum_interval_seconds
        self.default_status = default_status

    def __call__(self) -> RunStatusSensorDefinition:

        @run_status_sensor(
            name=self.name,
            description=self.description,
            run_status=self.run_status,
            monitored_jobs=[self.monitored_job],
            request_job=self.downstream_job,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=self.default_status,
        )
        def _sensor(context: RunStatusSensorContext):
            # Also check for tag of registering known materializations
            is_backfill = False if context.dagster_run.tags.get("dagster/backfill") is None else True
            context.log.debug(f"Is backfill: {is_backfill}")
            if is_backfill:
                backfill = context.instance.get_backfill(context.dagster_run.tags.get("dagster/backfill"))
                context.log.debug(f"Backfill: {backfill.backfill_id}")
                partitions = backfill.partition_names
            else:
                # Ordered by partition name
                partition_key = context.partition_key
                date, _ = partition_key.split("|")
                partitions = [f"{date}|{s}" for s in self.partitions_def_monitored_asset.get_partition_keys()]
            num_total, num_done, num_failed, successful_partitions = _get_materialization_info(
                context.instance,
                self.monitored_asset,
                partitions,
                self.partitions_def_monitored_asset,
                return_only_successful_partitions=True
            )
            # TODO: should create new run requests for each partition as partitions are materialized
            #  now, we do it all at the end. See: https://github.com/dagster-io/dagster/issues/19224
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
        return _sensor
