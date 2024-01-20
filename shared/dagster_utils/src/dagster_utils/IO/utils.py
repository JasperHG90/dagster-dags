import typing
from contextlib import contextmanager

import duckdb


@contextmanager  # type: ignore
def connect_to_duckdb(  # type: ignore
    path: str,
    aws_access_key: typing.Optional[str] = None,
    aws_secret_key: typing.Optional[str] = None,
    aws_region: typing.Optional[str] = None,
    aws_endpoint: typing.Optional[str] = None,
) -> duckdb.DuckDBPyConnection:
    """Open a connection to DuckDB and close it when done.

    Parameters
    ----------
    path : str
        path to DuckDB database
    aws_access_key : typing.Optional[str], optional
        AWS access key, by default None
    aws_secret_key : typing.Optional[str], optional
        AWS secret key, by default None
    aws_region : typing.Optional[str], optional
        AWS region, by default None
    aws_endpoint : typing.Optional[str], optional
        AWS endpoint, by default None

    Yields
    ------
    duckdb.DuckDBPyConnection
        DuckDB connection
    """
    con = duckdb.connect(database=path, read_only=False)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    if aws_access_key is not None:
        con.execute(f"SET s3_access_key_id='{aws_access_key}';")
    if aws_secret_key is not None:
        con.execute(f"SET s3_secret_access_key='{aws_secret_key}';")
    if aws_endpoint is not None:
        con.execute(f"SET s3_endpoint='{aws_endpoint}';")
        con.execute("SET s3_url_style='path';")
    if aws_region is not None:
        con.execute(f"SET s3_region='{aws_region}';")
    try:
        yield con
    finally:
        con.close()
