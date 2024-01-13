poetry_requirements(
    name="poetry",
)

resources(name="pyproject", sources=["pyproject.toml", "README.md"])

pex_binary(
    name="binary-deps",
    entry_point="dagster",
    execution_mode="venv",
    layout="packed",
    include_sources=False,
    include_tools=True,
    dependencies=[":pyproject", ":poetry"],
    environment="py39_slim",
)

pex_binary(
    name="binary-srcs",
    entry_point="dagster",
    execution_mode="venv",
    layout="packed",
    include_requirements=False,
    include_tools=True,
    dependencies=[
        ":pyproject",
        "dags/luchtmeetnet_ingestion/src/luchtmeetnet_ingestion:luchtmeetnet_ingestion",
    ],
    environment="py39_slim",
)

docker_image(
    name="img-deps",
    repository="luchtmeetnet_ingestion",
    # registry=["companyname"],
    image_tags=["deps"],
    skip_push=True,
    instructions=[
        "FROM python:3.9-slim as deps",
        "COPY dags.luchtmeetnet_ingestion/binary-deps.pex /binary_deps.pex",
        "RUN PEX_TOOLS=1 /usr/local/bin/python3.9 /binary_deps.pex venv --scope=deps --compile /bin/app",
    ],
)

docker_image(
    name="img-srcs",
    repository="luchtmeetnet_ingestion",
    # registry=["companyname"],
    image_tags=["srcs"],
    skip_push=True,
    instructions=[
        "FROM python:3.9-slim as srcs",
        "COPY dags.luchtmeetnet_ingestion/binary-srcs.pex /binary_srcs.pex",
        "RUN PEX_TOOLS=1 /usr/local/bin/python3.9 /binary_srcs.pex venv --scope=srcs --compile /bin/app",
    ],
)

docker_image(
    name="img",
    dependencies=[":img-srcs", ":img-deps"],
    repository="luchtmeetnet_ingestion",
    registries=["europe-west4-docker.pkg.dev/jasper-ginn-dagster/dags-areg-euw4-jgdag-prd"],
    instructions=[
        "FROM python:3.9-slim",
        "COPY --from=luchtmeetnet_ingestion:deps /bin/app /bin/app",
        "COPY --from=luchtmeetnet_ingestion:srcs /bin/app /bin/app",
        # Since dagster expects that we have a command-line argument called 'dagster'
        #  available, but that command is not available since we are using a pex binary,
        #  we use an entrypoint script to alias the dagster command to the pex binary.
        "RUN ln -s /bin/app/pex /usr/bin/dagster"
    ],
    image_tags=["latest", "{build_args.VERSION}"],
)
