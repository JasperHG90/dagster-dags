from dagster import (
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
)

daily_partition = DailyPartitionsDefinition(
    start_date="2024-01-26", end_offset=0, timezone="Europe/Amsterdam", fmt="%Y-%m-%d"
)

stations_partition = DynamicPartitionsDefinition(name="stations")

daily_station_partition = MultiPartitionsDefinition(
    {
        "daily": daily_partition,
        "stations": stations_partition,
    }
)
