from dagster import define_asset_job
from luchtmeetnet_ingestion.assets import air_quality_data
from luchtmeetnet_ingestion.partitions import daily_partition

ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=[air_quality_data],
    description="Ingestion job for air quality data",
    partitions_def=daily_partition,
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1,
                },
                "k8s_job_executor": {"max_concurrent": 3},
            }
        }
    },
)
