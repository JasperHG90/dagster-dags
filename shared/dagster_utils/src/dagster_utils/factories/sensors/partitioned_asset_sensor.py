import typing

from dagster import (
    JobDefinition,
    AssetKey,
    PartitionsDefinition,
    multi_asset_sensor,
    MultiAssetSensorDefinition,
    DefaultSensorStatus,
    MultiAssetSensorEvaluationContext,
    RunRequest,
    SkipReason,
    EventLogRecord
)

from dagster_utils.factories.base import DagsterObjectFactory
from dagster_utils.factories.sensors.utils import _get_materialization_info


class PartitionedAssetSensorFactory(DagsterObjectFactory):

    def __init__(
        self,
        name: str,
        monitored_asset: str,
        downstream_asset: str,
        job: JobDefinition,
        partitions_def_monitored_asset: PartitionsDefinition,
        require_all_partitions_monitored_asset: bool = False,
        minimum_interval_seconds: typing.Optional[int] = None,
        default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED,
        description: typing.Optional[str] = None,
    ):
        """A sensor that monitors some partitioned asset and triggers a job when the asset is materialized.

        This works for assets that have the same partitioned, or when the downstream asset shares a partition. This
        sensor also works if the monitored asset has some partitions that have failed by setting the
        `require_all_partitions_monitored_asset` parameter to False.

        Args:
            name (str): name of the sensor
            monitored_asset (str): name of asset that is monitored to determine if the job should be triggered
            downstream_asset (str): name of asset that is materialized by the job
            job (JobDefinition): job that should be triggered when the monitored asset is materialized
            partitions_def_monitored_asset (PartitionsDefinition): Partitions definition of the monitored asset.
            require_all_partitions_monitored_asset (bool, optional): If True, then downstream asset will be materialized even if upstream asset has failed partitions. Defaults to False.
            minimum_interval_seconds (typing.Optional[int], optional): The minimum number of seconds that will elapse between sensor evaluations. Defaults to None.
            default_status (DefaultSensorStatus, optional): Default status of the sensor. Defaults to DefaultSensorStatus.STOPPED.
            description (typing.Optional[str], optional): Description of this sensor. Defaults to None.
        """
        super().__init__(name, description)
        self.monitored_asset = monitored_asset
        self.downstream_asset = downstream_asset
        self.job = job
        self.partitions_def_monitored_asset = partitions_def_monitored_asset
        self.require_all_partitions_monitored_asset = require_all_partitions_monitored_asset
        self.minimum_interval_seconds = minimum_interval_seconds
        self.default_status = default_status

    def __call__(self) -> MultiAssetSensorDefinition:

        @multi_asset_sensor(
            monitored_assets=[AssetKey(self.monitored_asset)],
            job=self.job,
            name=self.name,
            description=self.description,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=self.default_status,
        )
        def _sensor(context: MultiAssetSensorEvaluationContext) -> typing.Generator[typing.Union[RunRequest, SkipReason], None, None]:
            run_requests_by_partition = {}
            materializations_by_partition = context.latest_materialization_records_by_partition(
                AssetKey(self.monitored_asset)
            )
            context.log.debug(f"Materializations: {materializations_by_partition}")

            # Get all corresponding weekly partitions for any materialized daily partitions
            for partition, materialization in materializations_by_partition.items():
                materialization: EventLogRecord
                context.log.info(f"Partition: {partition}")
                weekly_partitions = context.get_downstream_partition_keys(
                    partition,
                    from_asset_key=AssetKey(self.monitored_asset),
                    to_asset_key=AssetKey(self.downstream_asset),
                )
                context.log.info(f"Weekly partitions: {weekly_partitions}")
                run_tags = materialization.event_log_entry.tags
                context.log.debug(run_tags)
                if isinstance(run_tags, dict):
                    is_backfill = False if run_tags.get("dagster/backfill") is None else True
                    backfill_name = run_tags.get("dagster/backfill")
                    context.log.debug(f"Is backfill: {is_backfill}")

                if weekly_partitions:  # Check that a downstream weekly partition exists
                    # Upstream daily partition can only map to at most one downstream weekly partition
                    daily_partitions_in_week = context.get_downstream_partition_keys(
                        weekly_partitions[0],
                        from_asset_key=AssetKey(self.downstream_asset),
                        to_asset_key=AssetKey(self.monitored_asset),
                    )
                    context.log.debug(f"Daily partitions in week: {daily_partitions_in_week}")
                    # Filter for daily partitions in the backfill (if this is a backfill). Need failed
                    #  partitionss from get materialization info
                    if is_backfill:
                        backfill = context.instance.get_backfill(backfill_name)
                        partitions = backfill.partition_names
                    num_total, num_done, num_failed, _ = _get_materialization_info(
                        context.instance,
                        self.monitored_asset,
                        daily_partitions_in_week,
                        self.partitions_def_monitored_asset,
                        filter_partitions=partitions if is_backfill else None,
                    )
                    context.log.info(f"Total: {num_total}, Done: {num_done}, Failed: {num_failed}")
                    if num_done < num_total:
                        context.log.debug(
                            f"Only {num_done} out of {num_total} partitions have been materialized. Skipping . . ."
                        )
                        context.log.debug(
                            f"Only {num_done} out of {num_total} partitions have been materialized. Waiting until all partitions have been materialized."
                        )
                        context.log.info(f"Total: {num_total}, Done: {num_done}, Failed: {num_failed}")
                    else:
                        if num_failed > 0:
                            if self.require_all_partitions_monitored_asset:
                                context.advance_cursor({AssetKey(self.monitored_asset): materialization})
                                yield context.log.debug(
                                    f"'require_all_partitions_monitored_asset' is set to True. Encountered {num_failed} failed partitions. Skipping materialization of downstream asset with partition '{weekly_partitions[0]}'."
                                )
                                continue
                            else:
                                context.log.debug(
                                    f"'require_all_partitions_monitored_asset' is set to False. {num_failed} partitions failed to materialize. Will still run downstream task."
                                )
                        if weekly_partitions[0] in run_requests_by_partition:
                            continue
                        run_requests_by_partition[weekly_partitions[0]] = RunRequest(
                            partition_key=weekly_partitions[0],
                            run_key=weekly_partitions[0] if not is_backfill else f"{weekly_partitions[0]}_{backfill_name}",
                            tags={} if not is_backfill else {"dagster/backfill": backfill_name},
                            job_name=self.job.name,
                        )
                        # Advance the cursor so we only check event log records past the cursor
                        context.advance_cursor({AssetKey(self.monitored_asset): materialization})
            if len(materializations_by_partition) == 0:
                yield SkipReason("No materializations found. Skipping . . .")
            elif len(run_requests_by_partition) == 0:
                yield SkipReason("Materializations found for monitored asset but no downstream materialization requested. See logs for more details. Skipping ...")
            for request in list(run_requests_by_partition.values()):
                yield request
        return _sensor
