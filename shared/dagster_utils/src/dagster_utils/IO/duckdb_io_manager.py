import os
import pathlib as plb
import typing

import duckdb
import pandas as pd
from dagster import (
    BoolSource,
    Field,
    InitResourceContext,
    InputContext,
    IOManager,
    OutputContext,
    StringSource,
    io_manager,
)

from .utils import connect_to_duckdb


class DuckdbParquetIOManager(IOManager):
    def __init__(
        self,
        path: str,
        aws_access_key: typing.Optional[str] = None,
        aws_secret_key: typing.Optional[str] = None,
        aws_endpoint: typing.Optional[str] = None,
        aws_region: typing.Optional[str] = None,
        ignore_missing_partitions_on_load: bool = False,
    ) -> None:
        if path.startswith(("s3://", "s3a://", "gs://")):
            self.path_is_local = False
        else:
            self.path_is_local = True
        self.path = path
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_endpoint = aws_endpoint
        self.aws_region = aws_region
        self.ignore_missing_partitions_on_load = ignore_missing_partitions_on_load

    def _get_table_name(self, asset_key: str, partition_key: typing.Optional[str] = None) -> str:
        if partition_key is not None:
            path = os.path.join(self.path, asset_key, f"{partition_key}.parquet")
            if self.path_is_local:
                _path = plb.Path(path)
                if not _path.parent.exists():
                    _path.parent.mkdir(parents=True, exist_ok=True)
        else:
            path = os.path.join(self.path, f"{asset_key}.parquet")
        return path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """Write a pandas DataFrame to disk using DuckDB"""
        context.log.debug(f"Asset key: {context.asset_key}")
        context.log.debug(f"Partition key: {context.partition_key}")
        path = self._get_table_name(
            partition_key=context.partition_key if context.has_partition_key else None,
            asset_key=context.asset_key.to_python_identifier(),
        )
        context.log.debug(obj.head())
        context.log.debug(path)
        context.log.debug(self.aws_endpoint)
        context.log.debug(f"Access key is None: {self.aws_access_key is None}")
        context.log.debug(f"Secret key is None: {self.aws_secret_key is None}")
        sql = f"COPY obj TO '{path}' (FORMAT 'parquet', row_group_size 100000);"
        context.log.debug(sql)
        con: duckdb.DuckDBPyConnection
        with connect_to_duckdb(
            ":memory:",
            self.aws_access_key,
            self.aws_secret_key,
            self.aws_region,
            self.aws_endpoint,
        ) as con:
            con.execute(sql)  # type: ignore
        context.add_output_metadata({"file_name": path})

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a pandas DataFrame using DuckDB"""
        context.log.debug(f"Asset key: {context.asset_key}")
        context.log.debug(f"Partition key: {context.partition_key}")
        context.log.debug(f"Upstream asset key: {context.upstream_output.asset_key}")
        if self.ignore_missing_partitions_on_load:
            status_by_partition = {
                k: v
                for k, v in context.instance.get_status_by_partition(
                    context.asset_key,
                    partition_keys=context.asset_partition_keys,
                    partitions_def=context.asset_partitions_def,
                ).items()
                if v is not None
            }
            partitions = [k for k, v in status_by_partition.items() if v.value == "MATERIALIZED"]
            if len(context.asset_partition_keys) > len(partitions):
                context.log.warning(
                    f"Not all partitions are materialized. Missing partitions: {list(set(context.asset_partition_keys) - set(partitions))}"
                )
        else:
            partitions = context.asset_partition_keys
        context.log.debug(f"Partitions: {partitions}")
        if context.has_partition_key:
            path = [
                self._get_table_name(
                    partition_key=partition_key,
                    asset_key=context.upstream_output.asset_key.to_python_identifier(),
                )
                for partition_key in partitions  # context.asset_partition_keys
            ]

        else:
            path = [
                self._get_table_name(
                    asset_key=context.upstream_output.asset_key.to_python_identifier(),
                )
            ]
        path = [f"'{p}'" for p in path]
        path_join = f'[{",".join(path)}]'
        sql = f"SELECT * FROM read_parquet({path_join})"
        context.log.debug(sql)
        con: duckdb.DuckDBPyConnection
        with connect_to_duckdb(
            ":memory:",
            self.aws_access_key,
            self.aws_secret_key,
            self.aws_region,
            self.aws_endpoint,
        ) as con:
            return con.sql(sql).df()  # type: ignore


@io_manager(
    config_schema={
        "path": StringSource,
        "aws_access_key": Field(StringSource, is_required=False),
        "aws_secret_key": Field(StringSource, is_required=False),
        "aws_endpoint": Field(StringSource, is_required=False),
        "aws_region": Field(StringSource, is_required=False),
        "ignore_missing_partitions_on_load": Field(
            BoolSource, is_required=False, default_value=False
        ),
    },
    version="v1",
)
def duckdb_parquet_io_manager(
    init_context: InitResourceContext,
) -> DuckdbParquetIOManager:
    return DuckdbParquetIOManager(
        path=init_context.resource_config["path"],
        aws_access_key=init_context.resource_config.get("aws_access_key"),
        aws_secret_key=init_context.resource_config.get("aws_secret_key"),
        aws_endpoint=init_context.resource_config.get("aws_endpoint"),
        aws_region=init_context.resource_config.get("aws_region"),
        ignore_missing_partitions_on_load=init_context.resource_config.get(
            "ignore_missing_partitions_on_load"
        ),
    )
