import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from dagster import AssetExecutionContext, ConfigurableResource
from luchtmeetnet_ingestion.luchtmeetnet.api import get_results_luchtmeetnet_endpoint
from pydantic import PrivateAttr
from pyrate_limiter import BucketFullException, Duration, Limiter, Rate


@contextmanager
def rate_limiter(rate_calls: int, rate_minutes: int):
    rate = Rate(rate_calls, Duration.MINUTE * rate_minutes)
    limiter = Limiter(rate)
    try:
        yield limiter
    finally:
        ...


# Todo: change resource type so can add description/docs
#  https://docs.dagster.io/concepts/resources
class LuchtMeetNetResource(ConfigurableResource):
    rate_calls: int
    rate_minutes: int

    _limiter = PrivateAttr()

    @contextmanager
    def yield_for_execution(self, context):
        with rate_limiter(self.rate_calls, self.rate_minutes) as limiter:
            self._limiter = limiter
            yield self

    def request(
        self,
        endpoint: str,
        context: AssetExecutionContext,
        request_params: Optional[Dict[str, Any]] = None,
        retries_before_failing: int = 10,
        delay_in_seconds: int = 30,
    ) -> List[Dict[str, Any]]:
        partition_key = context.partition_key
        for retry in range(retries_before_failing):
            try:
                self._limiter.try_acquire(partition_key)
            except BucketFullException as e:
                context.log.warning(f"Rate limit exceeded for {partition_key} on try={retry}.")
                if retry == retries_before_failing - 1:
                    context.log.error((f"Rate limit exceeded for {partition_key} on try={retry}."))
                    raise BucketFullException() from e
                else:
                    time.sleep(delay_in_seconds)
            context.log.debug(
                f"Successfully retrieved connection for {partition_key} after {retry} tries."
            )
            context.log.debug(f"Requesting data from {endpoint} with params: {request_params}")
            return get_results_luchtmeetnet_endpoint(
                endpoint=endpoint, request_params=request_params
            )
