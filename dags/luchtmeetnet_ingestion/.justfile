set dotenv-load

alias i := install
alias d := dev

INSTALL_DEVCONTAINER_GROUP := "0"

# Install poetry dependencies
install devcontainer=INSTALL_DEVCONTAINER_GROUP:
    #!/usr/bin/env bash
    set -eo pipefail
    if [ "{{devcontainer}}" = "1" ]; then
        echo "Installing with devcontainer dependencies"
        poetry install --with devcontainer
    else
        echo "Installing without devcontainer dependencies"
        poetry install
    fi
    poetry self add poetry-git-version-plugin

# Run local dagster service
dev:
    mkdir -p .dagster && \
        cp dagster.yaml .dagster/dagster.yaml && \
        DAGSTER_HOME="$(pwd)/.dagster" poetry run dagster dev
