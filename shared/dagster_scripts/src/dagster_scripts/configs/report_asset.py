import logging
import pathlib as plb
import typing
from urllib.parse import urlparse

from dagster_scripts.configs.base import FileTypeEnum, PolicyEnum, StorageTypeEnum
from google.cloud import storage
from pydantic import BaseModel, computed_field, model_validator

logger = logging.getLogger("dagster_scripts.configs.report_asset")


class AssetConfig(BaseModel):
    key: str
    storage_location: str
    report_asset_policy: PolicyEnum
    type: FileTypeEnum
    skip_checks: bool = False

    @computed_field(repr=True)
    @property
    def storage_type(self) -> StorageTypeEnum:
        path_scheme = self.scheme
        if path_scheme == "gs":
            return StorageTypeEnum.gcs
        elif path_scheme == "":
            return StorageTypeEnum.local
        else:
            raise ValueError(f"Unsupported storage type {path_scheme}")

    @computed_field(repr=True)
    @property
    def scheme(self) -> str:
        return urlparse(self.storage_location).scheme

    @computed_field(repr=True)
    @property
    def bucket(self) -> str:
        if self.storage_type == StorageTypeEnum.gcs:
            return urlparse(self.storage_location).netloc
        else:
            return None

    @computed_field(repr=True)
    @property
    def prefix(self) -> str:
        if self.storage_type == StorageTypeEnum.gcs:
            return urlparse(self.storage_location).path.lstrip("/").rstrip("/")
        else:
            return None

    @model_validator(mode="after")
    def check_model(self):
        if self.storage_type == StorageTypeEnum.local:
            if not plb.Path(self.storage_location).resolve().exists():
                raise ValueError(f"Path {self.storage_location} does not exist")
        return self

    def list_files(self) -> typing.List[str]:
        """List all files in the storage location.

        Raises:
            ValueError: If the storage type is not supported

        Returns:
            typing.List[str]: list of asset partitions found in the storage location
        """
        if self.storage_type == StorageTypeEnum.local:
            logger.debug("Using local storage")
            return self._list_files_local()
        elif self.storage_type == StorageTypeEnum.gcs:
            logger.debug("Using GCS")
            return self._list_files_gcs()
        else:
            raise ValueError(f"Unsupported storage type {self.type}")

    def _list_files_local(self):
        """
        List files in a local directory.
        Internal use only
        """
        _path = plb.Path(self.storage_location)
        globs = _path.glob("*.parquet")
        return [f.with_suffix("").name for f in globs]

    def _list_files_gcs(self) -> typing.List[str]:
        """
        List all files in a GCS bucket.
        Internal use only
        """
        storage_client = storage.Client()
        logger.debug(f"Listing files in {self.scheme}://{self.bucket}/{self.prefix}")
        blobs: typing.Iterator[storage.Blob] = storage_client.list_blobs(
            prefix=self.prefix, bucket_or_name=self.bucket, match_glob=f"**.{self.type}"
        )
        return [blob.name for blob in blobs]


class ReportAssetConfig(BaseModel):
    repository_name: str
    assets: typing.List[AssetConfig]
