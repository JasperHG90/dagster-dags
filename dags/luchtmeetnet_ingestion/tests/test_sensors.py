from unittest import mock

import pytest
import pandas as pd
from dagster import build_run_status_sensor_context, DagsterInstance

from luchtmeetnet_ingestion.sensors import slack_message_on_failure, slack_message_on_success
from luchtmeetnet_ingestion.jobs import ingestion_job
from luchtmeetnet_ingestion import definition


class FakeSlackResource:
    def get_client(self):
        return ...


@pytest.fixture(scope="function")
def luchtmeetnet_data() -> pd.DataFrame:
    return pd.DataFrame(
        {"date": ["2021-01-01"], "station_id": ["NL00001"], "value": [1.0], "parameter": ["NO2"]}
    )


@mock.patch("luchtmeetnet_ingestion.sensors.my_message_fn")
@mock.patch("luchtmeetnet_ingestion.IO.resources.get_results_luchtmeetnet_endpoint")
def test_slack_message_on_failure(mock_air_quality_data_endpoint_call, mock_message_fn, luchtmeetnet_data):
    instance = DagsterInstance.ephemeral()
    mock_air_quality_data_endpoint_call.return_value = luchtmeetnet_data
    result = definition.get_job_def("ingestion_job").execute_in_process(instance=instance, partition_key="2024-01-01")

    dagster_run = result.dagster_run
    dagster_event = result.get_job_success_event() #.get_job_failure_event()

    context = build_run_status_sensor_context(
        sensor_name="slack_message_on_success",
        dagster_instance=instance,
        dagster_run=dagster_run,
        dagster_event=dagster_event,
    )

    slack_resource = FakeSlackResource()

    slack_message_on_success(context, slack=slack_resource)

    mock_message_fn.assert_called_with(
        slack_resource,
        "Job ingestion_job succeeded!"
    )
    mock_air_quality_data_endpoint_call.assert_called_once()
