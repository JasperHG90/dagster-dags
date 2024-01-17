set dotenv-load

PROJECT := "jasper-ginn-dagster"
ARTIFACT_STORE := "dags-areg-euw4-jgdag-prd"
DOCKER_BASE := "europe-west4-docker.pkg.dev/" + PROJECT + "/" + ARTIFACT_STORE
IMAGE_NAME := "luchtmeetnet_ingestion"
IMAGE_URI := DOCKER_BASE + "/" + IMAGE_NAME

alias i := install
alias d := dev
alias b := build
alias p := push
alias bp := build_and_push

# Install poetry dependencies
install:
    poetry install && poetry self add poetry-git-version-plugin

# Run local dagster service
dev:
    mkdir -p .dagster && \
        cp dagster.yaml .dagster/dagster.yaml && \
        DAGSTER_HOME="$(pwd)/.dagster" poetry run dagster dev

# Build docker image with dag installed
build:
    #!/usr/bin/env bash
    set -eo pipefail
    GIT_SHA=$(git rev-parse --short HEAD)
    TAG_LATEST="{{IMAGE_URI}}:latest"
    TAG_GIT_SHA="{{IMAGE_URI}}:$GIT_SHA"
    TAG_LOCAL="dagster/{{IMAGE_NAME}}:local"
    docker build . -t "$TAG_LATEST" -t "$TAG_GIT_SHA" -t "$TAG_LOCAL"

# Push docker image to GCP Artifact Registry
push:
    #!/usr/bin/env bash
    set -eo pipefail
    GIT_SHA=$(git rev-parse --short HEAD)
    TAG_LATEST="{{IMAGE_URI}}:latest"
    TAG_GIT_SHA="{{IMAGE_URI}}:$GIT_SHA"
    docker push "$TAG_LATEST" && docker push "$TAG_GIT_SHA"

# Build and push docker image to GCP Artifact Registry
build_and_push: build push
