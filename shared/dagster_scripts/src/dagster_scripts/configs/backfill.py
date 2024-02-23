import typing

from dagster_scripts.configs.base import PolicyEnum, TagsConfig
from pydantic import BaseModel, model_validator


class BackfillPolicy(BaseModel):
    policy: PolicyEnum
    asset_key: typing.Optional[str] = None

    @model_validator(mode="after")
    def validate_assets(self):
        if self.policy == PolicyEnum.missing and self.asset_key is None:
            raise ValueError("You must specify an asset key when policy is 'missing'")
        return self


class BackfillConfig(BaseModel):
    job_name: str
    repository_name: str
    backfill_policy: BackfillPolicy
    tags: TagsConfig
    skip_checks: bool = False
