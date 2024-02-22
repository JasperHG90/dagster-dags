
import pytest
from pydantic import ValidationError

from dagster_scripts.configs import backfill, partitions, base


def test_partition_config(date_partition_config):
    partitions.DatePartitionConfig(**date_partition_config)


def test_backfill_config(backfill_config):
    backfill.BackfillConfig(**backfill_config)


def test_backfill_config_policy_with_unknown_policy():
    with pytest.raises(ValidationError):
        backfill.BackfillConfig(
            job_name="test",
            repository_name="test",
            policy="failed", # Not part of the BackfillPolicyEnum
            tags=base.TagsConfig(
                name="test",
                partitions=[
                    partitions.DatePartitionConfig(
                        name="test",
                        values=partitions.DateRangeConfig(start_date="2021-01-01", end_date="2021-01-02"),
                    )
                ]
            )
        )


def test_backfill_config_with_start_date_before_end_date():
    with pytest.raises(ValueError, match="Start date must be before end date"):
        partitions.DatePartitionConfig(
            name="test",
            values=partitions.DateRangeConfig(start_date="2021-01-02", end_date="2021-01-01"),
        )


def test_date_partition_config_from_dict(tags_config):
    conf = partitions.DatePartitionConfig(**tags_config["partitions"][0])
    assert isinstance(conf.values, partitions.DateRangeConfig)
