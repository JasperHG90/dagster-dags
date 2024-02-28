from dagster import JobDefinition, asset, define_asset_job, AssetsDefinition, Definitions, MultiPartitionsDefinition, DailyPartitionsDefinition, StaticPartitionsDefinition
import pytest


@pytest.fixture()
def my_daily_partition() -> DailyPartitionsDefinition:
    return DailyPartitionsDefinition(
        start_date="2021-01-01",
        end_date="2021-01-05"
    )


@pytest.fixture()
def my_static_partition() -> StaticPartitionsDefinition:
    return StaticPartitionsDefinition(
        ["NL01345", "NL05432"]
    )


@pytest.fixture()
def my_multi_partition(my_daily_partition: DailyPartitionsDefinition, my_static_partition: StaticPartitionsDefinition) -> MultiPartitionsDefinition:
    return MultiPartitionsDefinition(
        {"daily": my_daily_partition, "stations": my_static_partition}
    )


@pytest.fixture()
def my_unpartitioned_asset() -> AssetsDefinition:
    @asset
    def my_asset() -> str:
        return "my_asset"
    return my_asset


@pytest.fixture()
def my_multipartitioned_asset(my_multi_partition: MultiPartitionsDefinition) -> AssetsDefinition:
    @asset(partitions_def=my_multi_partition)
    def my_asset() -> str:
        return "my_asset"
    return my_asset


@pytest.fixture()
def my_single_partitioned_asset(my_daily_partition: DailyPartitionsDefinition) -> AssetsDefinition:
    @asset(partitions_def=my_daily_partition)
    def my_asset() -> str:
        return "my_asset"
    return my_asset


@pytest.fixture()
def my_unpartitioned_job(my_unpartitioned_asset: AssetsDefinition) -> JobDefinition:
    return define_asset_job("my_unpartitioned_job", [my_unpartitioned_asset])


@pytest.fixture()
def my_multipartitioned_job(my_multipartitioned_asset: AssetsDefinition, my_multi_partition: MultiPartitionsDefinition) -> JobDefinition:
    return define_asset_job("my_multipartitioned_job", [my_multipartitioned_asset], partitions_def=my_multi_partition)


@pytest.fixture()
def my_single_partitioned_job(my_single_partitioned_asset: AssetsDefinition, my_daily_partition: DailyPartitionsDefinition) -> JobDefinition:
    return define_asset_job("my_single_partitioned_job", [my_single_partitioned_asset], partitions_def=my_daily_partition)


@pytest.fixture()
def my_unpartitioned_definition(my_unpartitioned_asset: AssetsDefinition, my_unpartitioned_job: JobDefinition) -> Definitions:
    return Definitions(
        assets=[my_unpartitioned_asset],
        jobs=[my_unpartitioned_job]
    )

@pytest.fixture()
def my_definition_single_multipartitioned_job(my_multipartitioned_asset: AssetsDefinition, my_multipartitioned_job: JobDefinition) -> Definitions:
    return Definitions(
        assets=[my_multipartitioned_asset],
        jobs=[my_multipartitioned_job]
    )

@pytest.fixture()
def my_definition_multiple_partitioned_jobs(my_single_partitioned_asset: AssetsDefinition, my_multipartitioned_asset: AssetsDefinition, my_single_partitioned_job: JobDefinition, my_daily_partition: DailyPartitionsDefinition) -> Definitions:
    return Definitions(
        assets=[my_single_partitioned_asset, my_multipartitioned_asset],
        jobs=[my_single_partitioned_job, my_multipartitioned_job]
    )
