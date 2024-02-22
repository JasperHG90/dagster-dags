from dagster_scripts.configs import backfill
from dagster_scripts.configs import partitions


def test_partition_config(date_partition_config):
    partitions.DatePartitionConfig(**date_partition_config)


def test_backfill_config(backfill_config):
    backfill.BackfillConfig(**backfill_config)
