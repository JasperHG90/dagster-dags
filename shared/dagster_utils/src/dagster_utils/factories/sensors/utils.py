import typing

from dagster import (
    AssetKey,
    PartitionsDefinition,
    DagsterInstance,
    AssetPartitionStatus
)


def _get_materialization_info(
        instance: DagsterInstance,
        monitored_asset: str,
        partition_keys: typing.Sequence[str],
        partitions_def_monitored_asset: PartitionsDefinition,
        return_only_successful_partitions: bool = False,
        filter_partitions: typing.Optional[typing.List[str]] = None
    ) -> typing.Tuple[int, int, int, typing.Mapping[str, AssetPartitionStatus]]:
    # If not yet materialized, then None, else status.FAILED
    status_by_partition = instance.get_status_by_partition(
        AssetKey(monitored_asset),
        partition_keys=partition_keys,
        partitions_def=partitions_def_monitored_asset
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
