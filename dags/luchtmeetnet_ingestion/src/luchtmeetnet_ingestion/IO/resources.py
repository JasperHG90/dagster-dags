import time
from typing import Any, Dict, List, Optional

from dagster import ConfigurableResource
from luchtmeetnet_ingestion.luchtmeetnet.api import get_results_luchtmeetnet_endpoint
from pyrate_limiter import BucketFullException, Duration, Limiter, Rate


# Todo: change resource type so can add description/docs
#  https://docs.dagster.io/concepts/resources
class LuchtMeetNetResource(ConfigurableResource):
    rate: Dict[str, int]

    def request(
        self,
        endpoint: str,
        partition_key: str,
        request_params: Optional[Dict[str, Any]] = None,
        retries_before_failing: int = 10,
        delay_in_seconds: int = 30,
    ) -> List[Dict[str, Any]]:
        context = self.get_resource_context()
        rate = Rate(self.rate["calls"], Duration.MINUTE * self.rate["minutes"])
        limiter = Limiter(rate)
        for retry in range(retries_before_failing):
            try:
                limiter.try_acquire(partition_key)
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
