install:
    poetry install && poetry self add poetry-git-version-plugin


dev:
    mkdir -p .dagster && \
        cp dagster.yaml .dagster/dagster.yaml && \
        DAGSTER_HOME="$(pwd)/.dagster" poetry run dagster dev
