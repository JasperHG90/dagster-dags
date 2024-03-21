import typing

from dagster import HookContext, failure_hook, success_hook
from dagster_utils.factories.base import DagsterObjectFactory


class CallableHookFn(typing.Protocol):
    def __call__(
        self, context: HookContext, value: typing.Dict[str, bool], labels: typing.Dict[str, str]
    ) -> None:
        ...


def metric_job_success_hook_factory(
    name: str,
    description: str,
    on_success: bool,
    callable_fn: CallableHookFn,
    required_resource_keys: typing.Optional[typing.Set[str]] = None,
) -> typing.Callable:
    return MetricJobSuccessHookFactory(
        name=name,
        description=description,
        on_success=on_success,
        callable_fn=callable_fn,
        required_resource_keys=required_resource_keys,
    )()


def parse_tags(tags: typing.Mapping[str, str]) -> typing.Iterator[typing.Dict[str, str]]:
    for k, v in tags.items():
        if k == "dagster/partition":
            yield {"partition": v}
        elif k in ["dagster/schedule_name", "dagster/sensor_name", "dagster/backfill"]:
            yield {"trigger_type": k.split("/")[-1], "trigger_name": v}
        else:
            continue


def generate_metric_labels(context: HookContext) -> typing.Dict[str, str]:
    labels = {}
    run = context.instance.get_run_by_id(context.run_id)
    labels["location"] = run.external_job_origin.location_name
    for tag_dict in parse_tags(run.tags):
        for k, v in tag_dict.items():
            labels[k] = v
    return labels


class MetricJobSuccessHookFactory(DagsterObjectFactory):
    def __init__(
        self,
        name: str,
        description: str,
        on_success: bool,
        callable_fn: CallableHookFn,
        required_resource_keys: typing.Optional[typing.Set[str]] = None,
    ):
        super().__init__(name, description)
        self.on_success = on_success
        self.required_resource_keys = required_resource_keys
        self.callable_fn = callable_fn

    def _generate_labels(self, context: HookContext) -> typing.Dict[str, str]:
        labels = generate_metric_labels(context)
        labels["job_name"] = context.job_name
        labels["run_id"] = context.run_id
        return labels

    def __call__(self) -> typing.Callable:
        if self.on_success:

            @success_hook(
                name=self.name,
                required_resource_keys=self.required_resource_keys,
            )
            def _function(context: HookContext):
                self.callable_fn(context, 1, self._generate_labels(context))

        else:

            @failure_hook(
                name=self.name,
                required_resource_keys=self.required_resource_keys,
            )
            def _function(context: HookContext):
                self.callable_fn(context, 0, self._generate_labels(context))

        return _function
