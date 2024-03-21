# Add the root directory to the Python path
import os
import sys

import redis

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)

from tasks import _remove_container_on_exit  # noqa


def test_remove_container_on_exit():
    with _remove_container_on_exit("alpine:latest") as container:
        assert container.status == "created"


def test_redis_container_with_args():
    with _remove_container_on_exit(
        "redis/redis-stack-server:latest",
        environment=["REDIS_ARGS=--requirepass dagster --port 16564"],
        ports={"16564/tcp": 16564},
    ):
        conn = redis.Redis(host="localhost", port=16564, password="dagster", username="default")
        assert conn.ping()
