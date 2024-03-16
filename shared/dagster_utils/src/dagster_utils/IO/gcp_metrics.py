import logging
import time
import typing

from dagster import ConfigurableResource
from dagster_utils.IO.utils import retry
from google.cloud import monitoring_v3

logger = logging.getLogger("luchtmeetnet_ingestion.IO.metrics")


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
