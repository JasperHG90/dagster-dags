from pydantic import BaseModel

from .base import PolicyEnum, TagsConfig


class BackfillConfig(BaseModel):
    job_name: str
    repository_name: str
    policy: PolicyEnum
    tags: TagsConfig
