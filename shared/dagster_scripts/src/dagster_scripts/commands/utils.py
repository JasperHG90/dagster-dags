import typing
import itertools
import collections
import functools
import logging
import pathlib as plb
import os
import asyncio

import yaml
from dagster_graphql.client import DagsterGraphQLClient, DagsterGraphQLClientError
from pydantic import BaseModel

from dagster_scripts.configs.partitions import PartitionConfig
from dagster_scripts.configs.backfill import BackfillConfig

logger = logging.getLogger("dagster_scripts.commands.utils")


def load_config(path: typing.Union[str, plb.Path], config_cls: BaseModel) -> BaseModel:
    """Load a YAML configuration file from disk"""
    path = plb.Path(path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Config at '{path}' not found")
    with path.resolve().open("r") as inFile:
        cnf_raw = yaml.safe_load(inFile)
        cnf_out = config_cls(**cnf_raw)
        return cnf_out


def graphql_client(host: str = "localhost", port: int = 3000):
    """Decorator to provide the Dagster GraphQL client to a function"""
    _host = os.getenv("DAGSTER_HOST", host)
    _port = os.getenv("DAGSTER_PORT", port)

    def decorator(f):
        functools.wraps(f)

        def wrapper(*args, **kwargs):
            client = DagsterGraphQLClient(_host, port_number=_port)
            return f(*args, client=client, **kwargs)

        return wrapper

    return decorator


def request_job_run(
    tags: typing.Dict[str, str],
    job_name: str,
    repository_name: str,
    client: typing.Optional[DagsterGraphQLClient] = None,
):
    """Request a new job run from the Dagster GraphQL API"""
    if client is None:
        raise ValueError("No client provided")
    _client: DagsterGraphQLClient = client
    try:
        new_run_id: str = _client.submit_job_execution(
            job_name=job_name, repository_name=repository_name, run_config={}, tags=tags
        )
        return new_run_id
    except DagsterGraphQLClientError as e:
        logger.error(f"Error submitting job execution: {e}")


@graphql_client()
def submit_backfill_jobs(conf: BackfillConfig, client: typing.Optional[DagsterGraphQLClient] = None) -> typing.Iterator[str]:
    if client is None:
        raise ValueError("No client provided")
    _client: DagsterGraphQLClient = client
    partition_configs = _generate_partition_configs(conf.tags.partitions)
    for partition_config in partition_configs:
        run_id = request_job_run(
            job_name=conf.job_name,
            repository_name=conf.repository_name,
            client=_client,
            tags=partition_config
        )
        yield run_id


async def _poll_job_termination(client: DagsterGraphQLClient, run_id: str):
    """Poll the status of a run until it is complete"""
    while True:
        status = client.get_run_status(run_id).value
        if status == "SUCCESS":
            return {"run_id": run_id, "status": status}
        elif status == "FAILURE":
            return {"run_id": run_id, "status": status}
        await asyncio.sleep(0.1)


async def _stop_for_backfill_status(client: DagsterGraphQLClient, run_ids: list):
    """Await the completion of a set of runs"""
    return await asyncio.gather(*[_poll_job_termination(client, run_id) for run_id in run_ids])


@graphql_client()
def await_backfill_status(run_ids: typing.List[str], client: typing.Optional[DagsterGraphQLClient] = None):
    """Await the completion of a set of runs"""
    if client is None:
        raise ValueError("No client provided")
    _client: DagsterGraphQLClient = client
    return asyncio.run(_stop_for_backfill_status(_client, run_ids))


def _generate_partition_configs(conf: PartitionConfig) -> typing.List[typing.Dict[str, str]]:
    """Generate a list of partition configurations from a combination of partitions"""
    return [
        dict(collections.ChainMap(*cnf_parsed)) for cnf_parsed in
        itertools.product(
            *[
                partition.config_dict()
                for partition in conf
            ]
        )
    ]
