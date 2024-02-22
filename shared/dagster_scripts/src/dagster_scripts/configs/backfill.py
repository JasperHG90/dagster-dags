import typing
from enum import Enum

from pydantic import BaseModel

from .partitions import DatePartitionConfig, StaticPartitionConfig


class BackfillTagsConfig(BaseModel):
    name: str
    partitions: typing.List[typing.Union[StaticPartitionConfig, DatePartitionConfig]]


class BackfillPolicy(str, Enum):
    missing = "missing"
    all = "all"


class BackfillConfig(BaseModel):
    job_name: str
    repository_name: str
    policy: BackfillPolicy
    tags: BackfillTagsConfig
