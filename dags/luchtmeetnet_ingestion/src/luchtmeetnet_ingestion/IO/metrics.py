import logging
import time
import typing

from dagster import ConfigurableResource
from google.cloud import monitoring_v3

logger = logging.getLogger("luchtmeetnet_ingestion.IO.metrics")


class RetryException(Exception):
    ...


def retry(attempts: int, seconds: int):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            retries = 0
            if retries < attempts:
                try:
                    result = func(self, *args, **kwargs)
                    return result
                except Exception as e:
                    logger.exception(e)
                    logger.info(f"Retrying {func.__name__} after {seconds} seconds.")
                    retries += 1
                    time.sleep(seconds)
            else:
                raise RetryException(f"Max retries of function {func} exceeded")

        return wrapper

    return decorator


class GcpMetricsResource(ConfigurableResource):
    environment: str
    project_id: str

    @retry(attempts=3, seconds=5)
    def post_time_series(
        self,
        series_type: str,
        value: typing.Union[int, float, bool],
        metric_labels: typing.Dict[str, str],
    ):
        client = monitoring_v3.MetricServiceClient()
        project_name = f"projects/{self.project_id}"
        series = monitoring_v3.TimeSeries()
        series.metric.type = series_type
        series.resource.type = "global"
        for k, v in metric_labels.items():
            series.metric.labels[k] = v
        series.metric.labels["environment"] = self.environment
        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10**9)
        interval = monitoring_v3.TimeInterval({"end_time": {"seconds": seconds, "nanos": nanos}})
        point = monitoring_v3.Point({"interval": interval, "value": value})
        series.points = [point]
        client.create_time_series(name=project_name, time_series=[series])
