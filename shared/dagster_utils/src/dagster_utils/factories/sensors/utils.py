import logging
import typing

import pendulum
from dagster import PartitionsDefinition


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
