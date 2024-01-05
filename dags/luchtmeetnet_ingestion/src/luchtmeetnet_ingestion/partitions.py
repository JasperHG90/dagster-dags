from dagster import (
    DailyPartitionsDefinition
)

daily_partition = DailyPartitionsDefinition(
    start_date="2023-12-20", end_offset=0, timezone="Europe/Amsterdam", fmt="%Y-%m-%d"
)
