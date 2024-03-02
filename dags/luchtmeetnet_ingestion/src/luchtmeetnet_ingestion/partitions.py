import datetime
import json
import warnings
from importlib import resources as impresources

from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

# Annoying, but pants cannot resolve this (normal poetry can)
#  so we wrap this in a try-except block for now so that tests
#  don't fail when we run pants test ::
try:
    from luchtmeetnet_ingestion import static

    with (impresources.files(static) / "stations.json").open("r") as f:
        stations = [sd["number"] for sd in json.load(f)]
except ImportError:
    warnings.warn("Could not load stations.json, using empty list")
    stations = []

daily_partition = DailyPartitionsDefinition(
    start_date=datetime.datetime(2024, 1, 24),
    end_offset=0,
    timezone="Europe/Amsterdam",
    fmt="%Y-%m-%d",
)

stations_partition = StaticPartitionsDefinition(stations)

daily_station_partition = MultiPartitionsDefinition(
    {
        "daily": daily_partition,
        "stations": stations_partition,
    }
)
