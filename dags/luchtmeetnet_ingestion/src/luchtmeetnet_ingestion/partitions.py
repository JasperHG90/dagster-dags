import datetime
import json
from importlib import resources as impresources

from dagster import (  # DynamicPartitionsDefinition,
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)
from luchtmeetnet_ingestion import static

# stations_partition = DynamicPartitionsDefinition(name="stations")

with (impresources.files(static) / "stations.json").open("r") as f:
    stations = [sd["number"] for sd in json.load(f)]

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
