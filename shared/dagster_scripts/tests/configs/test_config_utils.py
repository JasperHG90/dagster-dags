import typing
import pytest

from dagster_scripts.configs import utils, base
import pandas as pd


@pytest.fixture
def tags_config_cls(tags_config: typing.Dict[str, typing.Dict[str, str]]):
    return base.TagsConfig(**tags_config)


def test_generate_partition_configs(tags_config_cls: base.TagsConfig):
    partition_configs = (
        pd.DataFrame(
            utils._generate_partition_configs(
                tags_config_cls.partitions
            )
        )
        .sort_values(['dagster/partition/stations', 'dagster/partition/daily'])
        .reset_index(drop=True)
    )
    ref_configs = (
        pd.DataFrame([
            {
                "dagster/partition/stations": "NL01497",
                "dagster/partition/daily": "2024-01-31"
            },
            {
                "dagster/partition/stations": "NL01497",
                "dagster/partition/daily": "2024-02-01"
            },
            {
                "dagster/partition/stations": "NL01497",
                "dagster/partition/daily": "2024-02-02"
            },
            {
                "dagster/partition/stations": "NL01912",
                "dagster/partition/daily": "2024-01-31"
            },
            {
                "dagster/partition/stations": "NL01912",
                "dagster/partition/daily": "2024-02-01"
            },
            {
                "dagster/partition/stations": "NL01912",
                "dagster/partition/daily": "2024-02-02"
            },
        ])
        .sort_values(['dagster/partition/stations', 'dagster/partition/daily'])
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(partition_configs, ref_configs)
