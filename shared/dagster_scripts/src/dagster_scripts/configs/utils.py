import collections
import itertools
import pathlib as plb
import typing

import yaml
from dagster_scripts.configs.partitions import PartitionConfig
from pydantic import BaseModel


def load_config(path: typing.Union[str, plb.Path], config_cls: BaseModel) -> BaseModel:
    """Load a YAML configuration file from disk"""
    path = plb.Path(path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Config at '{path}' not found")
    with path.resolve().open("r") as inFile:
        cnf_raw = yaml.safe_load(inFile)
        cnf_out = config_cls(**cnf_raw)
        return cnf_out


def generate_partition_configs(
    conf: typing.List[PartitionConfig],
) -> typing.List[typing.Dict[str, str]]:
    """Generate a list of partition configurations from a combination of partitions"""
    return [
        dict(collections.ChainMap(*cnf_parsed))
        for cnf_parsed in itertools.product(*[partition.config_dict() for partition in conf])
    ]


def create_partition_key(partition_config: dict) -> str:
    """
    Create a partition key from a partition configuration

    Logic should be the same as the "dagster.MultiPartitionKey.keys_by_dimension()" method
    Dimensions are ordered by name and joined by "|"
    """
    return "|".join([i[-1] for i in sorted(partition_config.items())])
