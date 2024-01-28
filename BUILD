python_requirements(
    name="root",
)

docker_environment(
    name="py39-slim",
    platform="linux_x86_64",
    image="python:3.9-slim",
    python_bootstrap_search_path=["<PATH>"],
    fallback_environment="py39_slim_arm64"
)

docker_environment(
    name="py39-slim-arm64",
    platform="linux_arm64",
    image="python:3.9-slim",
    python_bootstrap_search_path=["<PATH>"]
)
