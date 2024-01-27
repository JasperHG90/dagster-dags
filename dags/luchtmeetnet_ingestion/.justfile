set dotenv-load

alias i := install
alias d := dev

# Install poetry dependencies
install:
    poetry install && poetry self add poetry-git-version-plugin

# Run local dagster service
dev:
    mkdir -p .dagster && \
        cp dagster.yaml .dagster/dagster.yaml && \
        DAGSTER_HOME="$(pwd)/.dagster" poetry run dagster dev
