from importlib import metadata

try:
    __version__ = metadata.version("dagster_utils")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"
