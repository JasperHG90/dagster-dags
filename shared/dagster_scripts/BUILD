poetry_requirements(
    name="poetry",
)

resources(name="pyproject", sources=["pyproject.toml", "README.md"])

pex_binary(
    name="binary-deps",
    entry_point="dagster_scripts.cli:entrypoint",
    execution_mode="venv",
    layout="packed",
    include_sources=False,
    include_tools=True,
    dependencies=[":pyproject", ":poetry"],
    environment="py39_slim"
)

pex_binary(
    name="binary-srcs",
    entry_point="dagster_scripts.cli:entrypoint",
    execution_mode="venv",
    layout="packed",
    include_requirements=False,
    include_tools=True,
    dependencies=[
        ":pyproject",
        "shared/dagster_scripts/src/dagster_scripts:dagster_scripts"
    ],
    environment="py39_slim",
)

docker_image(
    name="img-deps",
    repository="dagster_scripts",
    image_tags=["deps"],
    skip_push=True,
    instructions=[
        "FROM --platform=linux/amd64 python:3.9-slim as deps",
        "COPY shared.dagster_scripts/binary-deps.pex /binary_deps.pex",
        "RUN PEX_TOOLS=1 /usr/local/bin/python3.9 /binary_deps.pex venv --scope=deps --compile /bin/app",
    ],
)

docker_image(
    name="img-srcs",
    repository="dagster_scripts",
    image_tags=["srcs"],
    skip_push=True,
    instructions=[
        "FROM --platform=linux/amd64 python:3.9-slim as srcs",
        "COPY shared.dagster_scripts/binary-srcs.pex /binary_srcs.pex",
        "RUN PEX_TOOLS=1 /usr/local/bin/python3.9 /binary_srcs.pex venv --scope=srcs --compile /bin/app",
    ],
)

docker_image(
    name="img",
    dependencies=[":img-srcs", ":img-deps"],
    repository="dagster_scripts",
    registries=["europe-west4-docker.pkg.dev/jasper-ginn-dagster/utilities-areg-euw4-jgdag-prd"],
    instructions=[
        "FROM --platform=linux/amd64 python:3.9-slim",
        "WORKDIR /app",
        "COPY --from=dagster_scripts:deps /bin/app /bin/app",
        "COPY --from=dagster_scripts:srcs /bin/app /bin/app",
        "RUN ln -s /bin/app/pex /usr/bin/dagster_scripts",
        "ENTRYPOINT [\"dagster_scripts\"]"
    ],
    image_tags=["latest", "{build_args.VERSION}"],
)
