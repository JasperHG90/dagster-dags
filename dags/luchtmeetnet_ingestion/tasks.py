import logging
import pathlib as plb
import time
from contextlib import contextmanager

import docker
from invoke import task

logger = logging.getLogger("cli")
handler = logging.StreamHandler()
format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(format)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

root = plb.Path(__file__).parent.resolve()


@contextmanager
def _remove_container_on_exit(image: str, **container_kwargs):
    """Context manager managing docker container lifecycle"""
    client = docker.from_env()
    client.images.pull(image)
    try:
        container = client.containers.run(image, detach=True, **container_kwargs)
        time.sleep(.2)
        yield container
    finally:
        container.stop()
        container.remove()


@task
def dagster_dev(c):
    """Run Dagster in development mode"""
    with _remove_container_on_exit(
        "redis/redis-stack-server:latest",
        environment=['REDIS_ARGS=--requirepass dagster --user dagster --port 16564'],
        ports={"16564/tcp": 16564}
    ):
        c.run("mkdir -p .dagster")
        c.run("cp dagster.yaml .dagster/dagster.yaml")
        c.run(f"DAGSTER_HOME='{root}/.dagster' poetry run dagster dev")
