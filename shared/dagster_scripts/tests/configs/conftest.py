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
def report_asset_storage_config() -> typing.Dict[str, typing.Any]:
    return {
        "path": "gs://my-bucket/my-path"
    }


@pytest.fixture(scope="function")
def report_asset_storage_config_unknown_scheme() -> typing.Dict[str, typing.Any]:
    return {
        "path": "minio://my-bucket/my-path"
    }


@pytest.fixture(scope="function")
def backfill_policy_config() -> typing.Dict[str, typing.Any]:
    return {
        "policy": "missing",
        "asset_key": "my_asset_key"
    }


@pytest.fixture(scope="function")
def backfill_config() -> typing.Dict[str, typing.Any]:
    return {
        "job_name": "ingestion_job",
        "repository_name": "luchtmeetnet_ingestion",
        "backfill_policy": {
            "policy": "all"
        },
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


@pytest.fixture(scope="function")
def report_asset_config() -> typing.Dict[str, typing.Any]:
    return {
        "repository_name": "luchtmeetnet_ingestion",
        "assets": [
            {
                "key": "my_asset_key",
                "storage_location": {
                    "path": "gs://my-bucket/my-path"
                },
                "report_asset_policy": "missing",
                "type": "parquet",
                "skip_checks": False
            }
        ]
    }
