import pathlib as plb

from dagster_scripts.commands import utils
from dagster import DagsterInstance, asset, materialize, DailyPartitionsDefinition, AssetExecutionContext

# Two partitions!
daily_partition = DailyPartitionsDefinition(start_date="2021-01-01", end_date="2021-01-03")

@asset(
    description="Barebones asset to check if the asset materialization works and we can get run information",
)
def my_asset() -> dict:
    return {"data": "test"}


@asset(
    partitions_def=daily_partition,
    description="Barebones partitioned asset to check if the asset materialization works and we can get run information",
)
def my_partitioned_asset(context: AssetExecutionContext) -> dict:
    return {"data": "test", "partition": context.partition_key}


def test_dagster_instance_from_config(dagster_home: plb.Path):
    @utils.dagster_instance_from_config(config_dir=str(dagster_home))
    def test_function(dagster_instance):
        return dagster_instance
    dagster_instance = test_function()
    assert isinstance(dagster_instance, DagsterInstance)


class TestGetMaterializedPartitions:

    def test_get_materialized_partitions(self):
        with DagsterInstance.ephemeral() as dagster_instance:
            materialize([my_asset], instance=dagster_instance)
            assert len(utils._get_materialized_partitions(
                "my_asset",
                dagster_instance=dagster_instance)
            ) == 1

    def test_get_subset_materialized_partitions(self):
        with DagsterInstance.ephemeral() as dagster_instance:
            for partition_key in daily_partition.get_partition_keys():
                materialize(
                    [my_partitioned_asset],
                    instance=dagster_instance,
                    partition_key=partition_key
                )

            assert len(utils._get_materialized_partitions(
                "my_partitioned_asset",
                asset_partitions=[daily_partition.get_first_partition_key()],
                dagster_instance=dagster_instance)
            ) == 1


class TestReportAssetStatus:

    def test_report_subset_asset_status(self):
        with DagsterInstance.ephemeral() as dagster_instance:
            utils._report_asset_status(
                "my_partitioned_asset",
                asset_partitions=daily_partition.get_partition_keys(),
                dagster_instance=dagster_instance
            )
            assert len(utils._get_materialized_partitions(
                "my_partitioned_asset",
                asset_partitions=daily_partition.get_partition_keys(),
                dagster_instance=dagster_instance)
            ) == 2

        dagster_instance.all_asset_keys()
