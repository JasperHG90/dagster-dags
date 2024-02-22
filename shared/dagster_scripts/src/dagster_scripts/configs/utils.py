import collections
import itertools
import typing

from dagster_scripts.configs.partitions import PartitionConfig


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
