import os
from unittest import mock

import pandas as pd
import pytest
from dagster import (
    InitResourceContext,
    build_init_resource_context,
    build_output_context,
)

from dagster_utils.IO.duckdb_io_manager import duckdb_parquet_io_manager

AWS_ENDPOINT_URL = "storage.googleapis.com"


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "age": [20, 21, 22],
        }
    )


@pytest.fixture(scope="module")
def init_context() -> InitResourceContext:
    return build_init_resource_context(
        {"path": "s3://this/path/to/folder", "aws_endpoint": AWS_ENDPOINT_URL}
    )


@mock.patch("dagster_utils.IO.duckdb_io_manager.plb")
@pytest.mark.parametrize(
    "use_local_path,use_partition_key",
    [
        (True, True),
        (True, False),
        (False, True),
        (False, False),
    ],
)
def test_duckdb_get_table_name(mock_pathlib, use_local_path, use_partition_key):
    if not use_local_path:
        config = {"path": "s3://this/path/to/folder", "aws_endpoint": AWS_ENDPOINT_URL}
    else:
        config = {"path": "/this/path/to/folder"}
    init_context = build_init_resource_context(config)
    manager = duckdb_parquet_io_manager(init_context)
    if not use_local_path:
        assert not manager.path_is_local
    else:
        assert manager.path_is_local
    if not use_partition_key:
        assert manager._get_table_name("test_asset") == os.path.join(
            config["path"], "test_asset.parquet"
        )
    else:
        assert manager._get_table_name("test_asset", "2021-01-01|NL56564") == os.path.join(
            config["path"], "test_asset", "2021-01-01.parquet"
        )


@mock.patch("dagster_utils.IO.duckdb_io_manager.connect_to_duckdb")
def test_duckdb_parquet_io_manager(
    mock_connect_to_duckdb, df: pd.DataFrame, init_context: InitResourceContext
):
    manager = duckdb_parquet_io_manager(init_context)
    output_context = build_output_context(
        asset_key="test_asset"
    )
    manager.handle_output(output_context, df)
    mock_connect_to_duckdb.assert_called()
    mock_connect_to_duckdb.return_value.__enter__.return_value.execute.assert_called_with(
        "COPY obj TO 's3://this/path/to/folder/test_asset.parquet' (FORMAT 'parquet', row_group_size 100000);"
    )
