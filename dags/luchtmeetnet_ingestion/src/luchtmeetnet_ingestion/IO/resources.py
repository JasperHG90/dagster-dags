import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from coolname import generate_slug
from dagster import (
    AssetExecutionContext,
    ConfigurableResource,
    InitResourceContext,
    ResourceDependency,
)
from luchtmeetnet_ingestion.luchtmeetnet.api import get_results_luchtmeetnet_endpoint
from pydantic import PrivateAttr
from pyrate_limiter import BucketFullException, Duration, Limiter, Rate, RedisBucket
from redis import Redis


class RedisResource(ConfigurableResource):
    host: str
    port: Optional[int] = None
    password: Optional[str] = None
    username: Optional[str] = None

    @contextmanager
    def connection(self, context: InitResourceContext):
        context.log.debug("Connecting to Redis backend")
        try:
            conn = Redis(
                host=self.host,
                port=self.port if self.port is not None else 6379,
                password=self.password,
                username=self.username,
            )
            yield conn
        finally:
            conn.close()


@contextmanager
def rate_limiter(rate_calls: int, rate_minutes: int, bucket_key: str, redis_conn: Redis):
    rate = Rate(rate_calls, Duration.MINUTE * rate_minutes)
    bucket = RedisBucket.init(rates=[rate], redis=redis_conn, bucket_key=bucket_key)
    limiter = Limiter(bucket)
    try:
        yield limiter
    finally:
        ...


# See: https://docs.dagster.io/concepts/resources for referencing resources in resources
class RateLimiterResource(ConfigurableResource):
    rate_calls: int
    rate_minutes: int
    bucket_key: str
    redis: ResourceDependency[RedisResource]

    _limiter = PrivateAttr()

    # See: https://docs.dagster.io/concepts/resources#lifecycle-hooks for information about this method
    @contextmanager
    def yield_for_execution(self, context: InitResourceContext):
        with self.redis.connection(context=context) as redis_conn:
            with rate_limiter(
                self.rate_calls, self.rate_minutes, self.bucket_key, redis_conn=redis_conn
            ) as limiter:
                self._limiter = limiter
                yield self

    def try_acquire(
        self,
        context: AssetExecutionContext,
        partition_key: str,
        retries_before_failing: int,
        delay_in_seconds: int,
    ) -> None:
        for retry in range(retries_before_failing):
            try:
                self._limiter.try_acquire(partition_key)
                context.log.debug(
                    f"Successfully retrieved connection for {partition_key} after {retry} tries."
                )
                return
            except BucketFullException as e:
                context.log.warning(f"Rate limit exceeded for {partition_key} on try={retry}.")
                if retry == retries_before_failing - 1:
                    context.log.error((f"Rate limit exceeded for {partition_key} on try={retry}."))
                    raise BucketFullException() from e
                else:
                    time.sleep(delay_in_seconds)
                    continue


# Todo: change resource type so can add description/docs
#  https://docs.dagster.io/concepts/resources
class LuchtMeetNetResource(ConfigurableResource):
    rate_limiter: ResourceDependency[RateLimiterResource]

    def request(
        self,
        endpoint: str,
        context: AssetExecutionContext,
        request_params: Optional[Dict[str, Any]] = None,
        retries_before_failing: int = 10,
        delay_in_seconds: int = 30,
        paginate: bool = True,
    ) -> List[Dict[str, Any]]:
        if not context.has_partition_key:
            partition_key = generate_slug(2)
        else:
            partition_key = context.partition_key
        context.log.debug("Acquiring rate limiter")
        self.rate_limiter.try_acquire(
            context=context,
            partition_key=partition_key,
            retries_before_failing=retries_before_failing,
            delay_in_seconds=delay_in_seconds,
        )
        context.log.debug(f"Requesting data from {endpoint} with params: {request_params}")
        return get_results_luchtmeetnet_endpoint(
            endpoint=endpoint, request_params=request_params, paginate=paginate
        )
