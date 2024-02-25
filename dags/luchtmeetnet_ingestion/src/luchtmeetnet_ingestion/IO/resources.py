import time
from typing import Any, Dict, List, Optional

from dagster import AssetExecutionContext, Field, InitResourceContext, resource
from luchtmeetnet_ingestion.luchtmeetnet.api import get_results_luchtmeetnet_endpoint
from pyrate_limiter import BucketFullException, Duration, Limiter, Rate


# Todo: change resource type so can add description/docs
#  https://docs.dagster.io/concepts/resources
class LuchtMeetNetResource:
    def __init__(self, rate_calls: int, rate_minutes: int):
        self.rate_calls = rate_calls
        self.rate_minutes = rate_minutes

    def request(
        self,
        endpoint: str,
        context: AssetExecutionContext,
        request_params: Optional[Dict[str, Any]] = None,
        retries_before_failing: int = 10,
        delay_in_seconds: int = 30,
    ) -> List[Dict[str, Any]]:
        # context = self.get_resource_context()
        partition_key = context.partition_key
        rate = Rate(self.rate_calls, Duration.MINUTE * self.rate_minutes)
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


@resource(
    config_schema={
        "rate_calls": Field(int, is_required=True, description="Number of calls allowed per rate"),
        "rate_minutes": Field(int, is_required=True, description="Number of minutes for the rate"),
    },
    version="v1",
)
def luchtmeetnet_resource(
    init_context: InitResourceContext,
) -> LuchtMeetNetResource:
    return LuchtMeetNetResource(
        rate_calls=init_context.resource_config["rate_calls"],
        rate_minutes=init_context.resource_config["rate_minutes"],
    )
