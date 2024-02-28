import logging
import typing

import pendulum
from dagster import DagsterInstance, PartitionsDefinition, RunRecord, RunsFilter


class PartitionResolver:
    def __init__(
        self, upstream_partition: PartitionsDefinition, downstream_partition: PartitionsDefinition
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
        self.logger.debug(f"Checking for events after {time_window_start}")
        run_records = instance.get_run_records(
            filters=RunsFilter(
                job_name=self.monitored_job.name,
                statuses=self.run_status,
                updated_after=time_window_start,
            ),
            order_by="update_timestamp",
            ascending=False,
        )
        self.logger.debug(f"Number of records: {len(run_records)}")
        return run_records

    def _run_record_end_before_cursor_ts(self, run_record_end_ts: float, cursor_ts: float) -> bool:
        is_late = run_record_end_ts <= cursor_ts
        if is_late:
            self.logger.debug(
                f"Run record timestamp {run_record_end_ts} lies before cursor {cursor_ts}"
            )
        return is_late

    def _get_backfill_name(self, tags: typing.Mapping[str, str]) -> typing.Union[None, str]:
        _backfill_name = None
        is_backfill = False if tags.get("dagster/backfill") is None else True
        if is_backfill:
            _backfill_name = tags.get("dagster/backfill")
            self.logger.debug(f"This is a backfill with name '{_backfill_name}'")
        return _backfill_name

    def _get_scheduled_run_name(self, tags: typing.Mapping[str, str]) -> typing.Union[None, str]:
        _scheduled_run_name = None
        is_scheduled_run = False if tags.get("dagster/schedule_name") is None else True
        if is_scheduled_run:
            _scheduled_run_name = tags.get("dagster/schedule_name")
            self.logger.debug(f"This is a scheduled run with name '{_scheduled_run_name}'")
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
