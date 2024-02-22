import typing
from enum import Enum

from pydantic import BaseModel

from .partitions import DatePartitionConfig, StaticPartitionConfig


class TagsConfig(BaseModel):
    name: str
    partitions: typing.List[typing.Union[StaticPartitionConfig, DatePartitionConfig]]


class PolicyEnum(str, Enum):
    missing = "missing"
    all = "all"
