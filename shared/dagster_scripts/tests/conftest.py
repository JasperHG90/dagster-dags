import pathlib as plb

import pytest


@pytest.fixture(scope="session")
def dagster_home(tmp_path_factory: plb.Path) -> plb.Path:
    dagster_home = tmp_path_factory.mktemp("dagster")  # type: ignore
    config_path = dagster_home / "dagster.yaml"  # type: ignore
    config_path.touch()
    return dagster_home


@pytest.fixture(scope="session")
def configs_home(tmp_path_factory: plb.Path) -> plb.Path:
    return tmp_path_factory.mktemp("configs")  # type: ignore
