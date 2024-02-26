import logging
import typing

import pendulum
from dagster import (
    AssetKey,
    AssetPartitionStatus,
    DagsterInstance,
    PartitionsDefinition,
)


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

    def map_downstream_to_upstream_partitions(self, partition_keys: typing.List[str]):
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

    def map_upstream_to_downstream_partitions(self, partition_keys: typing.List[str]):
        if self.mapped_downstream_partition_dimension is None:
            self._map_upstream_to_downstream_partition()
        partition_map = {}
        for partition_key in partition_keys:
            self.upstream_partition.validate_partition_key(partition_key)
            downstream_partition_key = partition_key.split("|")[self._get_dimension_idx()]
            partition_map[partition_key] = downstream_partition_key
        return partition_map


def _get_materialization_info(
    instance: DagsterInstance,
    monitored_asset: str,
    partition_keys: typing.Sequence[str],
    partitions_def_monitored_asset: PartitionsDefinition,
    return_only_successful_partitions: bool = False,
    filter_partitions: typing.Optional[typing.List[str]] = None,
) -> typing.Tuple[int, int, int, typing.Mapping[str, AssetPartitionStatus]]:
    # If not yet materialized, then None, else status.FAILED
    status_by_partition = instance.get_status_by_partition(
        AssetKey(monitored_asset),
        partition_keys=partition_keys,
        partitions_def=partitions_def_monitored_asset,
    )
    if filter_partitions:
        status_by_partition = {
            k: v for k, v in status_by_partition.items() if k in filter_partitions
        }
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
    successful_partitions = {
        key: status
        for key, status in status_by_partition_filtered.items()
        if status.value == "MATERIALIZED"
    }
    if return_only_successful_partitions:
        return num_total, num_done, num_failed, successful_partitions
    else:
        return num_total, num_done, num_failed, status_by_partition
