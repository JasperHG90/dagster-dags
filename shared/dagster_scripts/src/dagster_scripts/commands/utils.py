import asyncio
import collections
import functools
import itertools
import logging
import os
import pathlib as plb
import typing

import yaml
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterEventType,
    DagsterInstance,
    EventRecordsFilter,
)
from dagster_graphql.client import DagsterGraphQLClient, DagsterGraphQLClientError
from dagster_scripts.configs.backfill import BackfillConfig
from dagster_scripts.configs.partitions import PartitionConfig
from pydantic import BaseModel

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
    _port = int(os.getenv("DAGSTER_PORT", port))

    def decorator(f):
        functools.wraps(f)

        def wrapper(*args, **kwargs):
            client = DagsterGraphQLClient(_host, port_number=_port)
            return f(*args, client=client, **kwargs)

        return wrapper

    return decorator


def dagster_instance_from_config(
    config_dir: str = "/opt/dagster_home/dagster", config_filename: str = "dagster.yaml"
):
    """Decorator to provide a Dagster instance to a function"""
    _config_dir = os.getenv("DAGSTER_CONFIG_DIR", config_dir)
    _config_filename = os.getenv("DAGSTER_CONFIG_FILENAME", config_filename)

    def decorator(f):
        functools.wraps(f)

        def wrapper(*args, **kwargs):
            with DagsterInstance.from_config(
                config_dir=_config_dir, config_filename=_config_filename
            ) as instance:
                return f(*args, dagster_instance=instance, **kwargs)

        return wrapper

    return decorator


def check_asset_exists(f: typing.Callable):
    functools.wraps(f)

    def wrapper(*args, **kwargs):
        dagster_instance: typing.Union[None, DagsterInstance] = kwargs.get("dagster_instance")
        if dagster_instance is None:
            raise ValueError("Dagster instance not provided.")
        _dagster_instance: DagsterInstance = dagster_instance
        asset_key = args[0]
        asset_keys = [
            asset_key.to_user_string() for asset_key in _dagster_instance.all_asset_keys()
        ]
        if asset_key not in asset_keys:
            raise ValueError(f"Asset '{asset_key}' not found")
        return f(*args, **kwargs)

    return wrapper


@dagster_instance_from_config()
@check_asset_exists
def get_materialized_partitions(
    asset_key: str,
    asset_partitions: typing.Optional[typing.Sequence[str]] = None,
    dagster_instance: typing.Optional[DagsterInstance] = None,
) -> typing.List[str]:
    """Get all asset materializations for a specific asset and partition values

    Args:
        asset_key (str): name of the asset
        asset_partitions (typing.Sequence[str], optional): Sequence of partition keys for which to subset. Defaults to None.
        dagster_instance (typing.Optional[DagsterInstance], optional): DagsterInstance. Defaults to None.

    Returns:
        typing.List[str]: List of partition keys that have been materialized
    """
    return _get_materialized_partitions(
        asset_key, asset_partitions=asset_partitions, dagster_instance=dagster_instance
    )


def _get_materialized_partitions(
    asset_key: str,
    asset_partitions: typing.Optional[typing.Sequence[str]] = None,
    dagster_instance: typing.Optional[DagsterInstance] = None,
) -> typing.List[str]:
    """See docstring for get_materialized_partitions"""
    filter = EventRecordsFilter(
        event_type=DagsterEventType.ASSET_MATERIALIZATION,
        asset_key=AssetKey(asset_key),
        asset_partitions=asset_partitions,
    )
    events = dagster_instance.get_event_records(filter)
    return [event.partition_key for event in events]


@dagster_instance_from_config()
@check_asset_exists
def report_asset_status(
    asset_key: str,
    asset_partitions: typing.Sequence[str] = None,
    dagster_instance: typing.Optional[DagsterInstance] = None,
) -> typing.List[str]:
    """Set the status as materialized for a set of partitions

    Args:
        asset_key (str): name of the asset
        asset_partitions (typing.Sequence[str], optional): Sequence of partition keys for which to subset. Defaults to None.
        dagster_instance (typing.Optional[DagsterInstance], optional): DagsterInstance. Defaults to None.

    Returns:
        typing.List[str]: List of partition keys that have been materialized
    """
    return _report_asset_status(
        asset_key, asset_partitions=asset_partitions, dagster_instance=dagster_instance
    )


def _report_asset_status(
    asset_key: str,
    asset_partitions: typing.Sequence[str],
    dagster_instance: typing.Optional[DagsterInstance] = None,
) -> typing.List[str]:
    """See docstring for report_asset_status"""
    asset_materializations = [
        AssetMaterialization(
            AssetKey(asset_key),
            partition=partition_key,
        )
        for partition_key in asset_partitions
    ]
    logger.debug(
        f"Reporting asset status for Asset key='{asset_key}' ({len(asset_materializations)} partitions)"
    )
    for asset_materialization in asset_materializations:
        dagster_instance.report_runless_asset_event(asset_materialization)


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
def submit_backfill_jobs(
    conf: BackfillConfig, client: typing.Optional[DagsterGraphQLClient] = None
) -> typing.Iterator[str]:
    if client is None:
        raise ValueError("No client provided")
    _client: DagsterGraphQLClient = client
    partition_configs = _generate_partition_configs(conf.tags.partitions)
    for partition_config in partition_configs:
        _partition_config = partition_config | {
            "dagster/backfill": conf.tags.name,
            "dagster/partition": _create_partition_key(partition_config),
            "manual/job_name": os.getenv("JOB_NAME", "NA"),
            "manual/github_actions_run_id": os.getenv("GITHUB_ACTIONS_RUN_ID", "NA"),
            "manual/github_actions_url": os.getenv("GITHUB_ACTIONS_URL", "NA"),
            "manual/triggered_by": "cicd",
        }
        logger.debug(f"Submitting job run with tags: {_partition_config}")
        run_id = request_job_run(
            job_name=conf.job_name,
            repository_name=conf.repository_name,
            client=_client,
            tags=_partition_config,
        )
        yield run_id


@graphql_client()
def await_backfill_status(
    run_ids: typing.List[str], client: typing.Optional[DagsterGraphQLClient] = None
):
    """Await the completion of a set of runs"""
    if client is None:
        raise ValueError("No client provided")
    _client: DagsterGraphQLClient = client
    return asyncio.run(_stop_for_backfill_status(_client, run_ids))


async def _poll_job_termination(client: DagsterGraphQLClient, run_id: str):
    """Poll the status of a run until it is complete"""
    # TODO: timeout
    while True:
        status = client.get_run_status(run_id).value
        if status == "SUCCESS":
            logger.debug(f"Run {run_id} status: \x1b[32;1m{status}\x1b[0m")
            return {"run_id": run_id, "status": status}
        elif status == "FAILURE":
            logger.debug(f"Run {run_id} status: \x1b[31;1m{status}\x1b[0m")
            return {"run_id": run_id, "status": status}
        logger.debug(f"Run {run_id} status: \x1b[38;20m{status}\x1b[0m")
        await asyncio.sleep(1)


async def _stop_for_backfill_status(client: DagsterGraphQLClient, run_ids: list):
    """Await the completion of a set of runs"""
    return await asyncio.gather(*[_poll_job_termination(client, run_id) for run_id in run_ids])


def _generate_partition_configs(conf: PartitionConfig) -> typing.List[typing.Dict[str, str]]:
    """Generate a list of partition configurations from a combination of partitions"""
    return [
        dict(collections.ChainMap(*cnf_parsed))
        for cnf_parsed in itertools.product(*[partition.config_dict() for partition in conf])
    ]


def _create_partition_key(partition_config: dict) -> str:
    """
    Create a partition key from a partition configuration

    Logic should be the same as the "dagster.MultiPartitionKey.keys_by_dimension()" method
    Dimensions are ordered by name and joined by "|"
    """
    return "|".join([i[-1] for i in sorted(partition_config.items())])
