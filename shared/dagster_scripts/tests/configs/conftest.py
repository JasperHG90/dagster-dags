import typing

import pytest


@pytest.fixture(scope="function")
def date_partition_config() -> typing.Dict[str, typing.Any]:
    return {
        "name": "daily",
        "values": {
            "start_date": "2024-01-31",
            "end_date": "2024-02-01"
        }
    }


@pytest.fixture(scope="function")
def tags_config() -> typing.Dict[str, typing.Any]:
    return {
        "name": "34543",
        "partitions": [
            {
                "name": "daily",
                "values": {
                    "start_date": "2024-01-31",
                    "end_date": "2024-02-02"
                }
            },
            {
                "name": "stations",
                "values": [
                    "NL01497",
                    "NL01912"
                ]
            }
        ]
    }


@pytest.fixture(scope="function")
def backfill_config() -> typing.Dict[str, typing.Any]:
    return {
        "job_name": "ingestion_job",
        "repository_name": "luchtmeetnet_ingestion",
        "policy": "missing",
        "tags": {
            "name": "34543",
            "partitions": [
            {
                "name": "daily",
                "values": {
                    "start_date": "2024-01-31",
                    "end_date": "2024-02-01"
                }
            },
            {
                "name": "stations",
                "values": [
                    "NL01497",
                    "NL01912"
                ]
            }
            ]
        }
    }
