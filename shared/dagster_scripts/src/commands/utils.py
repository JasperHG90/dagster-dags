import typing
import itertools
import collections

from dagster_scripts.configs.partitions import PartitionConfig


def generate_partition_configs(conf: PartitionConfig) -> typing.List[typing.Dict[str, str]]:
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
