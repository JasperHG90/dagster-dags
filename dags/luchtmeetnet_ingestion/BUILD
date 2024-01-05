# docker_image(
#     name="docker",
# )

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
    registries=["europe-west4-docker.pkg.dev/jasperg-dagster/regi-areg-euw4-dagi-prd"],
    instructions=[
        "FROM python:3.9-slim",
        "COPY --from=luchtmeetnet_ingestion:deps /bin/app /bin/app",
        "COPY --from=luchtmeetnet_ingestion:srcs /bin/app /bin/app",
        # So that dagster knows which dag we want to run
        # because that info is given in '[tool.dagster]' in pyproject
        "COPY dags.luchtmeetnet_ingestion/binary-srcs.pex/pyproject.toml /pyproject.toml",
        'ENTRYPOINT ["/bin/app/pex"]',
    ],
    image_tags=["latest"],
)
