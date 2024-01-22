from dagster import (
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
)

daily_partition = DailyPartitionsDefinition(
    start_date="2023-12-20", end_offset=0, timezone="Europe/Amsterdam", fmt="%Y-%m-%d"
)

stations_partition = DynamicPartitionsDefinition(name="stations")

daily_station_partition = MultiPartitionsDefinition(
    {
        "daily": daily_partition,
        "stations": stations_partition,
    }
)
