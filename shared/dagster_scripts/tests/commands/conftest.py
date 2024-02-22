import typing

import pytest


@pytest.fixture(scope="function")
def partition_configs() -> typing.List[typing.Dict[str,str]]:
    return [
        {"dagster/partition/daily": "2020-01-01", "dagster/partition/stations": "NL01497"},
        {"dagster/partition/daily": "2020-01-02", "dagster/partition/stations": "NL01497"},
        {"dagster/partition/daily": "2020-01-03", "dagster/partition/stations": "NL01497"},
    ]


@pytest.fixture(scope="function")
def materialized_partitions() -> typing.List[str]:
    return [
        "2020-01-01|NL01497",
    ]
