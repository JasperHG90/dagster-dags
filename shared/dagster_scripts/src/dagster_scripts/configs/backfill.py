import typing

from pydantic import BaseModel

from .partitions import DatePartitionConfig, StaticPartitionConfig


class BackfillTagsConfig(BaseModel):
	name: str
	partitions: typing.List[typing.Union[StaticPartitionConfig, DatePartitionConfig]]


class BackfillConfig(BaseModel):
	job_name: str
	repository_name: str
	tags: BackfillTagsConfig
