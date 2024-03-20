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


class GcpMetricLabels:
    def __init__(self):
        self.__labels = {}

    def add(self, key: str, value: str):
        self.__labels[key] = value
        return self

    @property
    def labels(self):
        return self.__labels


def parse_tags(tags: typing.Mapping[str, str]) -> typing.Iterator[typing.Dict[str, str]]:
    for k, v in tags.items():
        if k == "dagster/partition":
            yield {"partition": v}
        elif k in ["dagster/schedule_name", "dagster/sensor_name", "dagster/backfill"]:
            yield {"trigger_type": k.split("/")[-1], "trigger_name": v}
        else:
            continue


def generate_gcp_metric_labels(context: HookContext, labels: GcpMetricLabels) -> GcpMetricLabels:
    run = context.instance.get_run_by_id(context.run_id)
    labels.add("location", run.external_job_origin.location_name)
    for tag_dict in parse_tags(run.tags):
        for k, v in tag_dict.items():
            labels.add(k, v)
    return labels


def post_metric(context: HookContext, value: int, labels: typing.Dict[str, str]):
    _labels = generate_gcp_metric_labels(context, GcpMetricLabels())
    for k, v in labels.items():
        _labels.add(k, v)
    context.log.debug(_labels.labels)
    context.resources.gcp_metrics.post_time_series(
        series_type="custom.googleapis.com/dagster/job_success",
        value={"bool_value": value},
        metric_labels=_labels.labels,
    )


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
        if self.on_success:

            @success_hook(
                name=self.name,
                required_resource_keys={self.gcp_resource_name},
            )
            def _function(context: HookContext):
                post_metric(
                    context,
                    1,
                    {
                        "job_name": context.job_name,
                        "run_id": context.run_id,
                    },
                )

        else:

            @failure_hook(
                name="job_failure_gcp_metric",
                required_resource_keys={self.gcp_resource_name},
            )
            def _function(context: HookContext):
                post_metric(
                    context,
                    0,
                    {
                        "job_name": context.job_name,
                        "run_id": context.run_id,
                    },
                )

        return _function
