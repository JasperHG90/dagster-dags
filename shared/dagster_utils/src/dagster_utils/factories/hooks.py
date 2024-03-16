import typing

from dagster import HookContext, failure_hook, success_hook
from dagster_utils.factories.base import DagsterObjectFactory


def gcp_metric_hook_factory(
    name: str, description: str, on_success: bool, gcp_resource_name: typing.Optional[str] = None
) -> typing.Callable:
    return GcpMetricHookFactory(
        name=name,
        description=description,
        on_success=on_success,
        gcp_resource_name=gcp_resource_name,
    )()


class GcpMetricHookFactory(DagsterObjectFactory):
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
        if self.on_success:

            @success_hook(
                name=self.name,
                description=self.description,
                required_resource_keys={self.gcp_resource_name},
            )
            def _function(context: HookContext):
                context.resources.__getattr__(self.gcp_resource_name).gcp_metrics.post_time_series(
                    series_type="custom.googleapis.com/dagster/job_success",
                    value={"bool_value": 1},
                    metric_labels={
                        "job_name": context.job_name,
                        "run_id": context.run_id,
                    },
                )

        else:

            @failure_hook(
                name="job_failure_gcp_metric",
                description=self.description,
                required_resource_keys={self.gcp_resource_name},
            )
            def _function(context: HookContext):
                context.resources.__getattr__(self.gcp_resource_name).gcp_metrics.post_time_series(
                    series_type="custom.googleapis.com/dagster/job_success",
                    value={"bool_value": 0},
                    metric_labels={
                        "job_name": context.job_name,
                        "run_id": context.run_id,
                    },
                )

        return _function
