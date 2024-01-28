FROM mcr.microsoft.com/devcontainers/base:ubuntu-22.04

ENV GIT_PYTHON_REFRESH=quiet
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHON_VERSION=3.9
ENV HOME="/home/vscode"
ENV PYENV_ROOT="$HOME/.pyenv"
ENV PIPX_HOME="/home/vscode/.pipx"
ENV PIPX_DEFAULT_PYTHON="/home/vscode/.pyenv/shims/python"

# Prerequisites
RUN apt-get update \
    && apt-get -y install libsqlite3-dev libffi-dev pipx gcc musl-dev python3-dev \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://pyenv.run | bash
ENV PATH="$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH"
RUN echo 'eval "$(pyenv init -)"' >> /home/vscode/.bashrc
RUN pyenv install ${PYTHON_VERSION}
RUN pyenv global ${PYTHON_VERSION}
RUN pyenv rehash
RUN pip install wheel --no-cache-dir --upgrade pip

# DuckDB dist
# See: https://github.com/duckdb/duckdb/issues/8035
RUN mkdir /home/vscode/.dist
RUN apt-get update \
    && apt-get install -y g++ git cmake ninja-build libssl-dev \
    && rm -rf /var/lib/apt/lists/* \
    && git clone --depth 1 --branch v0.9.2 https://github.com/duckdb/duckdb /home/vscode/duckdb \
    && BUILD_PYTHON=1 BUILD_HTTPFS=1 GEN=ninja cd /home/vscode/duckdb/tools/pythonpkg && python setup.py bdist_wheel --dist-dir /home/vscode/.dist \
    && rm -R /home/vscode/duckdb

# Just
RUN wget -qO - 'https://proget.makedeb.org/debian-feeds/prebuilt-mpr.pub' | gpg --dearmor | sudo tee /usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg 1> /dev/null \
    && echo "deb [arch=all,$(dpkg --print-architecture) signed-by=/usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg] https://proget.makedeb.org prebuilt-mpr $(lsb_release -cs)" | sudo tee /etc/apt/sources.list.d/prebuilt-mpr.list \
    && apt-get update \
    && apt-get -y install just \
    && rm -rf /var/lib/apt/lists/*

# Poetry
RUN pipx install poetry

# Pants
RUN curl --proto '=https' --tlsv1.2 -fsSL https://static.pantsbuild.org/setup/get-pants.sh | bash

# pipx distros
ENV PATH="/home/vscode/.local/bin:$PATH"

# Versioning with poetry
RUN poetry self add poetry-git-version-plugin

# Gcloud
RUN apt-get update -y \
    && apt-get install -y apt-transport-https \
        ca-certificates \
        gnupg \
        curl \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && apt-get update -y \
    && apt-get install google-cloud-sdk -y \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /home/vscode/.ssh
RUN chmod -R 777 /home/vscode
RUN usermod -aG sudo vscode
