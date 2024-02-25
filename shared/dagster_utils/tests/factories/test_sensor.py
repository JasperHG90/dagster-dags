import typing
from datetime import datetime as dt

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    DagsterInstance,
    DailyPartitionsDefinition,
    Definitions,
    MultiPartitionsDefinition,
    RunRequest,
    SkipReason,
    StaticPartitionsDefinition,
    asset,
    build_multi_asset_sensor_context,
    define_asset_job,
    materialize,
)
from dagster_utils.factories.sensor import PartitionedAssetSensorFactory

daily_partition = DailyPartitionsDefinition(start_date="2021-01-01", end_date="2021-01-03")

static_partition = StaticPartitionsDefinition(["NL01387", "NL01388"])

multi_partition = MultiPartitionsDefinition(
    {"daily": daily_partition, "stations": static_partition}
)


def my_asset_factory(
    fail_materializations: typing.Optional[typing.List[str]] = None,
) -> AssetsDefinition:
    @asset(partitions_def=multi_partition, name="my_multi_partition_asset")
    def _asset(context: AssetExecutionContext) -> typing.List[str]:
        if fail_materializations is not None:
            if context.partition_key in fail_materializations:
                raise ValueError("This partition failed")
        return context.partition_keys

    return _asset


@asset(partitions_def=daily_partition)
def my_daily_partition_asset(context: AssetExecutionContext) -> typing.List[str]:
    return context.partition_keys


daily_asset_job = define_asset_job(
    "daily_asset_job", [my_daily_partition_asset], partitions_def=daily_partition
)

# See also: https://github.com/dagster-io/hooli-data-eng-pipelines/blob/master/hooli_data_eng_tests/test_assets.py
trigger_weekly_asset_from_daily_asset = PartitionedAssetSensorFactory(
    name="trigger_weekly_asset_from_daily_asset",
    monitored_asset="my_multi_partition_asset",
    downstream_asset="my_daily_partition_asset",
    job=daily_asset_job,
    partitions_def_monitored_asset=multi_partition,
    require_all_partitions_monitored_asset=False,
)()

# TODO
# ignore asset materialization reports
# Also: check for asset runs and job runs
# Check: works with multiple partitions? Out of scope

# Expect two downstream materialization run requests because all partitions have succeeded
def test_multi_asset_sensor_backfill_all_materialized():
    my_multi_partition_asset: AssetsDefinition = my_asset_factory(fail_materializations=None)
    with DagsterInstance.ephemeral() as instance:
        definition = Definitions(
            assets=[my_multi_partition_asset, my_daily_partition_asset], jobs=[daily_asset_job]
        )
        multi_asset_context = build_multi_asset_sensor_context(
            instance=instance,
            monitored_assets=[AssetKey("my_multi_partition_asset")],
            definitions=definition,
        )
        for partition_key in multi_partition.get_partition_keys():
            materialize(
                [my_multi_partition_asset],
                partition_key=partition_key,
                instance=instance,
                tags={"dagster/backfill": "my_backfill"},
            )
        run_requests = [*trigger_weekly_asset_from_daily_asset(multi_asset_context)]
    assert len(run_requests) == 2
    for request in run_requests:
        assert request.run_key in ["2021-01-01_my_backfill", "2021-01-02_my_backfill"]
        assert request.tags["dagster/backfill"] == "my_backfill"


# Expect two downstream materialization run requests because still at least one partition
# on 2021-01-01 has succeeded
def test_multi_asset_sensor_backfill_with_failed_asset_materialization():
    my_multi_partition_asset: AssetsDefinition = my_asset_factory(
        fail_materializations=["2021-01-01|NL01387"]
    )
    with DagsterInstance.ephemeral() as instance:
        definition = Definitions(
            assets=[my_multi_partition_asset, my_daily_partition_asset], jobs=[daily_asset_job]
        )
        multi_asset_context = build_multi_asset_sensor_context(
            instance=instance,
            monitored_assets=[AssetKey("my_multi_partition_asset")],
            definitions=definition,
        )
        for partition_key in multi_partition.get_partition_keys():
            try:
                materialize(
                    [my_multi_partition_asset],
                    partition_key=partition_key,
                    instance=instance,
                    tags={"dagster/backfill": "my_backfill"},
                )
            # This is expected
            except ValueError:
                continue
        run_requests = [*trigger_weekly_asset_from_daily_asset(multi_asset_context)]
    assert len(run_requests) == 2
    for request in run_requests:
        assert request.run_key in ["2021-01-01_my_backfill", "2021-01-02_my_backfill"]


# Expect only a single downstream materialization run request because all partitions on
# 2021-01-01 have failed
def test_multi_asset_sensor_backfill_with_all_failed_asset_materializations_on_day():
    my_multi_partition_asset: AssetsDefinition = my_asset_factory(
        fail_materializations=["2021-01-01|NL01387", "2021-01-01|NL01388"]
    )
    with DagsterInstance.ephemeral() as instance:
        definition = Definitions(
            assets=[my_multi_partition_asset, my_daily_partition_asset], jobs=[daily_asset_job]
        )
        multi_asset_context = build_multi_asset_sensor_context(
            instance=instance,
            monitored_assets=[AssetKey("my_multi_partition_asset")],
            definitions=definition,
        )
        for partition_key in multi_partition.get_partition_keys():
            try:
                materialize(
                    [my_multi_partition_asset],
                    partition_key=partition_key,
                    instance=instance,
                    tags={"dagster/backfill": "my_backfill"},
                )
            # This is expected
            except ValueError:
                continue
        run_requests = [*trigger_weekly_asset_from_daily_asset(multi_asset_context)]
    assert len(run_requests) == 1
    for request in run_requests:
        assert request.run_key == "2021-01-02_my_backfill"


# Materialization should be skipped because the backfill is not yet done
def test_multi_asset_sensor_backfill_with_asset_not_yet_materialized():
    my_multi_partition_asset: AssetsDefinition = my_asset_factory(fail_materializations=None)
    with DagsterInstance.ephemeral() as instance:
        definition = Definitions(
            assets=[my_multi_partition_asset, my_daily_partition_asset], jobs=[daily_asset_job]
        )
        multi_asset_context = build_multi_asset_sensor_context(
            instance=instance,
            monitored_assets=[AssetKey("my_multi_partition_asset")],
            definitions=definition,
        )
        for partition_key in multi_partition.get_partition_keys():
            # Situation: not all keys backfilled
            if partition_key == "2021-01-01|NL01387":
                continue
            materialize(
                [my_multi_partition_asset],
                partition_key=partition_key,
                instance=instance,
                tags={"dagster/backfill": "my_backfill"},
            )
        run_requests = [*trigger_weekly_asset_from_daily_asset(multi_asset_context)]
        assert len(run_requests) == 2
        assert isinstance(run_requests[0], SkipReason)
        assert (
            run_requests[0].skip_message
            == "Only 1 out of 2 partitions have been materialized. Waiting until all partitions have been materialized."
        )
        assert isinstance(run_requests[1], RunRequest)
        assert run_requests[1].partition_key == "2021-01-02"
        assert run_requests[1].run_key == "2021-01-02_my_backfill"


# Materialization of downstream asset should happen because none has failed
def test_multi_asset_sensor_daily_run_all_materialized():
    my_multi_partition_asset: AssetsDefinition = my_asset_factory(fail_materializations=None)
    with DagsterInstance.ephemeral() as instance:
        definition = Definitions(
            assets=[my_multi_partition_asset, my_daily_partition_asset], jobs=[daily_asset_job]
        )
        multi_asset_context = build_multi_asset_sensor_context(
            instance=instance,
            monitored_assets=[AssetKey("my_multi_partition_asset")],
            definitions=definition,
        )
        for partition_key in multi_partition.get_partition_keys(
            dt(2021, 1, 2)
        ):  # dt offset is one (2021-01-01)
            materialize(
                [my_multi_partition_asset],
                partition_key=partition_key,
                instance=instance,
            )
        run_requests = [*trigger_weekly_asset_from_daily_asset(multi_asset_context)]
    assert len(run_requests) == 1
    for request in run_requests:
        assert request.run_key == "2021-01-01"
        assert request.tags.get("dagster/backfill") is None


# Materialization of downstream asset should happen because only one partition has failed
def test_multi_asset_sensor_daily_run_one_asset_failed():
    my_multi_partition_asset: AssetsDefinition = my_asset_factory(
        fail_materializations=["2021-01-01|NL01387"]
    )
    with DagsterInstance.ephemeral() as instance:
        definition = Definitions(
            assets=[my_multi_partition_asset, my_daily_partition_asset], jobs=[daily_asset_job]
        )
        multi_asset_context = build_multi_asset_sensor_context(
            instance=instance,
            monitored_assets=[AssetKey("my_multi_partition_asset")],
            definitions=definition,
        )
        for partition_key in multi_partition.get_partition_keys(
            dt(2021, 1, 2)
        ):  # dt offset is one (2021-01-01)
            try:
                materialize(
                    [my_multi_partition_asset],
                    partition_key=partition_key,
                    instance=instance,
                )
            # This is expected
            except ValueError:
                continue
        run_requests = [*trigger_weekly_asset_from_daily_asset(multi_asset_context)]
    assert len(run_requests) == 1
    for request in run_requests:
        assert request.run_key == "2021-01-01"
        assert request.tags.get("dagster/backfill") is None


# Materialization of downstream asset should not happen because still waiting on partition to materialize
def test_multi_asset_sensor_daily_run_with_asset_not_yet_materialized():
    my_multi_partition_asset: AssetsDefinition = my_asset_factory(fail_materializations=None)
    with DagsterInstance.ephemeral() as instance:
        definition = Definitions(
            assets=[my_multi_partition_asset, my_daily_partition_asset], jobs=[daily_asset_job]
        )
        multi_asset_context = build_multi_asset_sensor_context(
            instance=instance,
            monitored_assets=[AssetKey("my_multi_partition_asset")],
            definitions=definition,
        )
        for partition_key in multi_partition.get_partition_keys(
            dt(2021, 1, 2)
        ):  # dt offset is one (2021-01-01)
            if partition_key == "2021-01-01|NL01387":
                continue
            materialize(
                [my_multi_partition_asset],
                partition_key=partition_key,
                instance=instance,
            )
        run_requests = [*trigger_weekly_asset_from_daily_asset(multi_asset_context)]
    assert len(run_requests) == 1
    assert isinstance(run_requests[0], SkipReason)
    assert (
        run_requests[0].skip_message
        == "Only 1 out of 2 partitions have been materialized. Waiting until all partitions have been materialized."
    )
