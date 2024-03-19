import typing

from dagster import HookContext, failure_hook, success_hook
from dagster_utils.factories.base import DagsterObjectFactory


def gcp_metric_job_success_hook_factory(
    name: str, description: str, on_success: bool, gcp_resource_name: typing.Optional[str] = None
) -> typing.Callable:
    return GcpMetricJobSuccessHookFactory(
        name=name,
        description=description,
        on_success=on_success,
        gcp_resource_name=gcp_resource_name,
    )()


class GcpMetricJobSuccessHookFactory(DagsterObjectFactory):
    def __init__(
        self,
        name: str,
        description: str,
        on_success: bool,
        gcp_resource_name: typing.Optional[str] = None,
    ):
        super().__init__(name, description)
        self.on_success = on_success
        self.gcp_resource_name = gcp_resource_name

    def __call__(self) -> typing.Callable:
        def post_metric(context: HookContext, value: int):
            labels = {
                "job_name": context.job_name,
                "run_id": context.run_id,
            }
            for k, v in context.op.tags.items():
                labels[k] = v
            context.resources.gcp_metrics.post_time_series(
                series_type="custom.googleapis.com/dagster/job_success",
                value={"bool_value": value},
                metric_labels=labels,
            )

        if self.on_success:

            @success_hook(
                name=self.name,
                required_resource_keys={self.gcp_resource_name},
            )
            def _function(context: HookContext):
                post_metric(context, 1)

        else:

            @failure_hook(
                name="job_failure_gcp_metric",
                required_resource_keys={self.gcp_resource_name},
            )
            def _function(context: HookContext):
                post_metric(context, 0)

        return _function
