import pathlib as plb
import typing
from urllib.parse import urlparse

from dagster_scripts.configs.base import FileTypeEnum, PolicyEnum, StorageTypeEnum
from pydantic import BaseModel, computed_field, model_validator


class StorageConfig(BaseModel):
    path: str

    @computed_field(repr=True)
    @property
    def type(self) -> str:
        path_scheme = urlparse(self.path).scheme
        if path_scheme == "gs":
            return StorageTypeEnum.gcs
        elif path_scheme == "":
            return StorageTypeEnum.local
        else:
            raise ValueError(f"Unsupported storage type {path_scheme}")

    @model_validator(mode="after")
    def check_model(self):
        if self.type == StorageTypeEnum.local:
            if not plb.Path(self.path).resolve().exists():
                raise ValueError(f"Path {self.path} does not exist")


class AssetConfig(BaseModel):
    key: str
    storage_location: StorageConfig
    report_asset_policy: PolicyEnum
    type: FileTypeEnum


class BackfillConfig(BaseModel):
    repository_name: str
    assets: typing.List[AssetConfig]
