import typing
import pathlib as plb

import pytest


@pytest.fixture(scope="session")
def dagster_home(tmp_path_factory: plb.Path) -> plb.Path:
    return tmp_path_factory.mktemp("dagster")  # type: ignore


@pytest.fixture(scope="session")
def dagster_config_on_disk(dagster_home: plb.Path) -> str:
    config_path = dagster_home / "dagster.yaml"  # type: ignore
    config_path.touch()
    return str(config_path)
