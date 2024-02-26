from dagster import build_schedule_from_partitioned_job
from luchtmeetnet_ingestion.jobs import ingestion_job

daily_schedule = build_schedule_from_partitioned_job(
    job=ingestion_job, hour_of_day=13, minute_of_hour=42
)
