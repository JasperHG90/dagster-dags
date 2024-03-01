import logging
import typing

import pendulum
from dagster import (
    AssetKey,
    DagsterEventType,
    DagsterInstance,
    DagsterRun,
    DagsterRunStatus,
    EventLogRecord,
    EventRecordsFilter,
    MultiPartitionsDefinition,
    PartitionsDefinition,
    RunRecord,
    RunsFilter,
)


class MultiToSinglePartitionResolver:
    def __init__(
        self,
        upstream_partition: MultiPartitionsDefinition,
        downstream_partition: PartitionsDefinition,
    ):
        self.upstream_partition = upstream_partition
        self.downstream_partition = downstream_partition
        self.ref_datetime = pendulum.now(tz="UTC") - pendulum.duration(days=7)
        self.mapped_downstream_partition_dimension = None
        self.logger = logging.getLogger(
            "dagster_utils.factories.sensors.utils.MultiToSinglePartitionResolver"
        )

    def _map_upstream_to_downstream_partition(self):
        for partition_dimension_definition in self.upstream_partition.partitions_defs:
            partitions_def = partition_dimension_definition.partitions_def
            partition_keys = partitions_def.get_partition_keys(self.ref_datetime)
            ref_partition_keys = self.downstream_partition.get_partition_keys(self.ref_datetime)
            partition_keys.sort()
            ref_partition_keys.sort()
            if partition_keys == ref_partition_keys:
                self.logger.info(
                    f"Fount matching partition for {partition_dimension_definition.name}"
                )
                self.mapped_downstream_partition_dimension = partition_dimension_definition.name
        if self.mapped_downstream_partition_dimension is None:
            raise ValueError("No matching partition found")

    def _get_dimension_idx(self):
        if self.mapped_downstream_partition_dimension is None:
            self._map_upstream_to_downstream_partition()
        dimensions_ordered = self.upstream_partition._get_primary_and_secondary_dimension()
        idx = 0
        dim_idx = None
        for dimension in dimensions_ordered:
            if self.mapped_downstream_partition_dimension == dimension.name:
                return idx
            idx += 1
        if dim_idx is None:
            raise ValueError("No matching dimension found")

    def map_downstream_to_upstream_partitions(
        self, partition_keys: typing.List[str]
    ) -> typing.Dict[str, typing.List[str]]:
        if self.mapped_downstream_partition_dimension is None:
            self._map_upstream_to_downstream_partition()
        partition_map = {}
        for partition_key in partition_keys:
            self.downstream_partition.validate_partition_key(partition_key)
            partition_map[
                partition_key
            ] = self.upstream_partition.get_multipartition_keys_with_dimension_value(
                self.mapped_downstream_partition_dimension, partition_key
            )
        return partition_map

    def map_upstream_to_downstream_partitions(
        self, partition_keys: typing.List[str]
    ) -> typing.Dict[str, str]:
        if self.mapped_downstream_partition_dimension is None:
            self._map_upstream_to_downstream_partition()
        partition_map = {}
        for partition_key in partition_keys:
            self.upstream_partition.validate_partition_key(partition_key)
            downstream_partition_key = partition_key.split("|")[self._get_dimension_idx()]
            partition_map[partition_key] = downstream_partition_key
        return partition_map


class MonitoredJobSensorMixin:
    def _get_run_records_for_job(self, instance: DagsterInstance) -> typing.Sequence[RunRecord]:
        time_window_now = pendulum.now(tz="UTC")
        time_window_start = time_window_now - pendulum.duration(
            seconds=self.time_window_seconds
        )  # Make configurable
        self._logger.debug(f"Checking for events after {time_window_start}")
        run_records = instance.get_run_records(
            filters=RunsFilter(
                job_name=self.monitored_job.name,
                statuses=self.run_status,
                updated_after=time_window_start,
            ),
            order_by="update_timestamp",
            ascending=False,
        )
        self._logger.debug(f"Number of records: {len(run_records)}")
        return run_records

    def _run_record_end_before_cursor_ts(self, run_record_end_ts: float, cursor_ts: float) -> bool:
        is_late = run_record_end_ts <= cursor_ts
        if is_late:
            self._logger.debug(
                f"Run record timestamp {run_record_end_ts} lies before cursor {cursor_ts}"
            )
        return is_late

    def _get_backfill_name(self, tags: typing.Mapping[str, str]) -> typing.Union[None, str]:
        _backfill_name = None
        is_backfill = False if tags.get("dagster/backfill") is None else True
        if is_backfill:
            _backfill_name = tags.get("dagster/backfill")
            self._logger.debug(f"This is a backfill with name '{_backfill_name}'")
        return _backfill_name

    def _get_scheduled_run_name(self, tags: typing.Mapping[str, str]) -> typing.Union[None, str]:
        _scheduled_run_name = None
        is_scheduled_run = False if tags.get("dagster/schedule_name") is None else True
        if is_scheduled_run:
            _scheduled_run_name = tags.get("dagster/schedule_name")
            self._logger.debug(f"This is a scheduled run with name '{_scheduled_run_name}'")
        return _scheduled_run_name

    def _get_backfill_partitions(
        self,
        instance: DagsterInstance,
        backfill_name: str,
        all_upstream_partitions: typing.List[str],
    ):
        backfill = instance.get_backfill(backfill_name)
        # Backfill partitions for all upstream partitions of this run
        return [
            partition
            for partition in backfill.partition_names
            if partition in all_upstream_partitions
        ]

    def _increment_unfinished_downstream_partitions(self, run_key: str) -> bool:
        # If already know that run_key is unfinished (still require materializations), then continue
        # until the skip number is self.skip_when_unfinished_count, then remove from the list and try again
        # this keeps us from doing expensive computations and timing out the sensor
        if run_key in self.unfinished_downstream_partitions:
            self._logger.debug(
                "Run key has recentely been reported as unfinished. Skipping this partition ..."
            )
            skip_number = self.unfinished_downstream_partitions[run_key]
            if skip_number == self.skip_when_unfinished_count:
                del self.unfinished_downstream_partitions[run_key]
            else:
                self.unfinished_downstream_partitions[run_key] += 1
            return True
        else:
            return False

    def _sensor_already_triggered_with_run_key(self, run_key: str) -> bool:
        # If this run key has already been requested during this evaluation, skip
        if run_key in self.run_key_requests_this_sensor:
            self._logger.debug(f"Run key {run_key} already requested. Skipping . . .")
            return True
        else:
            return False

    def _run_key_already_completed(self, run_key: str, instance: DagsterInstance) -> bool:
        # If this run key has already been completed previously, skip
        run_key_completed = instance.get_runs(filters=RunsFilter(tags={"dagster/run_key": run_key}))
        self._logger.debug(f"Run key: {run_key}")
        self._logger.debug(f"Run key completed: {False if len(run_key_completed) == 0 else True}")
        if len(run_key_completed) > 0:
            self._logger.debug(f"Run with run_key='{run_key}' already exists. Skipping . . .")
            return True
        else:
            return False

    def _get_successful_materializations_for_monitored_asset_partitions(
        self,
        instance: DagsterInstance,
        partitions: typing.List[str],
        backfill_name: typing.Optional[str] = None,
        schedule_name: typing.Optional[str] = None,
    ) -> typing.Sequence[EventLogRecord]:
        # Retrieve materialization & failed runs information for partitions
        self._logger.debug(
            f"Retrieving materialization events for asset {self.monitored_asset} . . ."
        )
        tag_key = "dagster/backfill" if backfill_name is not None else "dagster/schedule_name"
        tag_value = backfill_name if backfill_name is not None else schedule_name
        filter_materialized_assets = EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=AssetKey(self.monitored_asset),
            asset_partitions=partitions,
            tags={tag_key: tag_value},
            after_timestamp=pendulum.now(tz="UTC").subtract(minutes=60).timestamp(),
        )
        events_successful_materializations = instance.get_event_records(filter_materialized_assets)
        self._logger.debug(
            f"Number of successful materializations: {len(events_successful_materializations)}"
        )
        return events_successful_materializations

    def _get_failed_pipeline_events_for_monitored_asset_partitions(
        self,
        instance: DagsterInstance,
        backfill_name: typing.Optional[str] = None,
        schedule_name: typing.Optional[str] = None,
    ) -> typing.Sequence[DagsterRun]:
        # NB: cannot filter for asset on these events :/
        if (backfill_name is None and schedule_name is None) or (
            backfill_name is not None and schedule_name is not None
        ):
            raise ValueError("Exactly one of backfill_name or schedule_name must be provided")
        self._logger.debug("Retrieving failed pipeline events for time window T-60 minutes . . .")
        tag_key = "dagster/backfill" if backfill_name is not None else "dagster/schedule_name"
        tag_value = backfill_name if backfill_name is not None else schedule_name
        filter_failed_runs = RunsFilter(
            job_name=self.monitored_job.name,
            statuses=[DagsterRunStatus.FAILURE],
            tags={tag_key: tag_value},
        )
        runs_failed = instance.get_run_records(filter_failed_runs)
        self._logger.debug(
            f"Number of failed runs for tag '{tag_key}' with value '{tag_value}': {len(runs_failed)}"
        )
        return runs_failed

    def _get_job_statistics(
        self,
        instance: DagsterInstance,
        partitions: typing.List[str],
        backfill_name: typing.Optional[str] = None,
        schedule_name: typing.Optional[str] = None,
    ) -> typing.Tuple[int, int, int, int, int]:
        events_successful_materializations = (
            self._get_successful_materializations_for_monitored_asset_partitions(
                instance=instance,
                partitions=partitions,
                backfill_name=backfill_name,
                schedule_name=schedule_name,
            )
        )
        runs_failed = self._get_failed_pipeline_events_for_monitored_asset_partitions(
            instance=instance, backfill_name=backfill_name, schedule_name=schedule_name
        )
        num_total = len(partitions)
        num_successful = len(events_successful_materializations)
        num_failed = len(runs_failed)
        num_done = num_successful + num_failed
        num_unfinished = num_total - num_done
        return (
            num_total,
            num_successful,
            num_failed,
            num_done,
            num_unfinished,
        )
