from importlib import metadata

try:
    __version__ = metadata.version("dagster_scripts")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"
