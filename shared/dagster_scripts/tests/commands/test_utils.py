import pathlib as plb

from dagster_scripts.commands import utils
from dagster import DagsterInstance


def test_dagster_instance_from_config(dagster_home: plb.Path):
    @utils.dagster_instance_from_config(config_dir=str(dagster_home))
    def test_function(dagster_instance):
        return dagster_instance
    dagster_instance = test_function()
    assert isinstance(dagster_instance, DagsterInstance)
