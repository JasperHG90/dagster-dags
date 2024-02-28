import logging
import typing

import pytest
from dagster import (
    DagsterInstance,
    DagsterRunStatus,
    Definitions,
    JobDefinition,
    MultiPartitionsDefinition,
)
from dagster_utils.factories.sensors.utils import MonitoredJobSensorMixin


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
