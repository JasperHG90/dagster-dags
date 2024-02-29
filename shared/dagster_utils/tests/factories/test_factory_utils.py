import logging
import typing

import pytest
from dagster import (
    DagsterInstance,
    DagsterRunStatus,
    DailyPartitionsDefinition,
    Definitions,
    JobDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)
from dagster_utils.factories.sensors.utils import (
    MonitoredJobSensorMixin,
    MultiToSinglePartitionResolver,
)


class TestClassForMonitoredJobSensorMixin(MonitoredJobSensorMixin):
    def __init__(
        self,
        name: str,
        monitored_asset: str,
        monitored_job: JobDefinition,
        run_status: typing.List[DagsterRunStatus] = [DagsterRunStatus.SUCCESS],
        time_window_seconds: int = 120,
    ):
        self.name = name
        self.monitored_asset = monitored_asset
        self.monitored_job = monitored_job
        self.run_status = run_status
        self.time_window_seconds = time_window_seconds
        self.logger = logging.getLogger("test")


class BackfillMock:
    def __init__(self, partitions) -> None:
        self.partition_names = partitions


@pytest.fixture(scope="function")
def test_class_single_unpartitioned_job(
    my_unpartitioned_job: JobDefinition, request: pytest.FixtureRequest
):
    request.instance._test_cls = TestClassForMonitoredJobSensorMixin(
        name="test",
        monitored_asset="my_unpartitioned_asset",
        monitored_job=my_unpartitioned_job,
    )


@pytest.fixture(scope="function")
def test_class_single_multipartitioned_job(
    my_multipartitioned_job: JobDefinition, request: pytest.FixtureRequest
):
    request.instance._test_cls = TestClassForMonitoredJobSensorMixin(
        name="test",
        monitored_asset="my_multipartitioned_asset",
        monitored_job=my_multipartitioned_job,
    )


@pytest.fixture()
def backfill_mock(my_multi_partition: MultiPartitionsDefinition) -> BackfillMock:
    return BackfillMock(my_multi_partition.get_partition_keys()[:-1])


class TestMonitoredJobSensorMixin:
    @pytest.mark.usefixtures("test_class_single_unpartitioned_job")
    def test_get_run_records_for_job(self, my_unpartitioned_definition: Definitions):
        with DagsterInstance.ephemeral() as instance:
            my_unpartitioned_definition.get_job_def("my_unpartitioned_job").execute_in_process(
                instance=instance,
            )
            run_records = self._test_cls._get_run_records_for_job(instance)
            assert len(run_records) == 1

    @pytest.mark.usefixtures("test_class_single_unpartitioned_job")
    def test_get_backfill_name(self):
        backfill_name = self._test_cls._get_backfill_name({"dagster/backfill": "test"})
        assert backfill_name == "test"

    @pytest.mark.usefixtures("test_class_single_unpartitioned_job")
    def test_get_scheduled_run_name(self):
        schedule_name = self._test_cls._get_scheduled_run_name({"dagster/schedule_name": "test"})
        assert schedule_name == "test"

    @pytest.mark.usefixtures("test_class_single_multipartitioned_job")
    def test_get_backfill_partitions(
        self,
        my_definition_single_multipartitioned_job: Definitions,
        my_multi_partition: MultiPartitionsDefinition,
        backfill_mock: BackfillMock,
    ):
        with DagsterInstance.ephemeral() as instance:
            instance.get_backfill = lambda _: backfill_mock
            all_partition_keys = my_multi_partition.get_partition_keys()
            for partition_key in all_partition_keys[:-1]:
                my_definition_single_multipartitioned_job.get_job_def(
                    "my_multipartitioned_job"
                ).execute_in_process(
                    instance=instance,
                    partition_key=partition_key,
                    tags={"dagster/backfill": "test"},
                )
            backfill_partitions = self._test_cls._get_backfill_partitions(
                instance=instance, backfill_name="test", all_upstream_partitions=all_partition_keys
            )
        assert backfill_partitions == all_partition_keys[:-1]


@pytest.fixture(scope="function")
def partition_resolver(
    my_multi_partition: MultiPartitionsDefinition,
    my_daily_partition: DailyPartitionsDefinition,
    request: pytest.FixtureRequest,
):
    request.instance._test_cls = MultiToSinglePartitionResolver(
        upstream_partition=my_multi_partition,
        downstream_partition=my_daily_partition,
    )


class TestPartitionResolver:
    @pytest.mark.usefixtures("partition_resolver")
    def test_map_upstream_to_downstream_partition(self):
        self._test_cls._map_upstream_to_downstream_partition()
        assert self._test_cls.mapped_downstream_partition_dimension == "daily"

    @pytest.mark.usefixtures("partition_resolver")
    def test_get_dimension_idx_pre(self):
        idx = self._test_cls._get_dimension_idx()
        assert idx == 0

    def test_get_dimension_idx_post(
        self,
        my_static_partition: StaticPartitionsDefinition,
        my_daily_partition: DailyPartitionsDefinition,
    ):
        # Primary dimension is always date, not alphabetical. But partition keys are alphabetical.
        multi_partition = MultiPartitionsDefinition(
            {"astations": my_static_partition, "daily": my_daily_partition}
        )
        resolver = MultiToSinglePartitionResolver(
            upstream_partition=multi_partition,
            downstream_partition=my_daily_partition,
        )
        idx = resolver._get_dimension_idx()
        assert idx == 0

    @pytest.mark.usefixtures("partition_resolver")
    def test_map_downstream_to_upstream_partitions(
        self, my_daily_partition: DailyPartitionsDefinition
    ):
        upstream_partitions = self._test_cls.map_downstream_to_upstream_partitions(
            partition_keys=my_daily_partition.get_partition_keys()
        )
        assert len(upstream_partitions) == 4
        assert upstream_partitions["2021-01-01"] == ["2021-01-01|NL01345", "2021-01-01|NL05432"]

    @pytest.mark.usefixtures("partition_resolver")
    def test_map_upstream_to_downstream_partitions(
        self, my_multi_partition: MultiPartitionsDefinition
    ):
        downstream_partitions = self._test_cls.map_upstream_to_downstream_partitions(
            partition_keys=my_multi_partition.get_partition_keys()
        )
        assert len(downstream_partitions) == 8
        assert downstream_partitions["2021-01-01|NL01345"] == "2021-01-01"
