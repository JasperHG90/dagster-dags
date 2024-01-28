# Luchtmeetnet ingestion

## ☄️ Setting up for local development

Boot up the devcontainer. This takes a while because we are compiling DuckDB from source (required since the extensions are not compiled for ARM-based architecture).

Then:

- If you are on an ARM-based architecture and want to develop your DAG locally, install the DAG/package using:

```shell
just install 1
```

- Else, install using:

```shell
just install
```

On ARM-based infrastructures, we use the optional dependency group 'devcontainer' that will install duckdb from the wheel stored in '/home/vscode/.dist'. For more information about why this is necessary you can check out [this link](https://github.com/duckdb/duckdb/issues/8035).

## ⛏️ Running dagster locally

To run Dagster locally, execute

```shell
just dev
```

This will:

1. Create a '.dagster' folder
2. Copy the 'dagster.yaml' configuration to the '.dagster' folder
3. Set the DAGSTER_HOME environment variable to the '.dagster' folder
4. Host the dagster web UI on http://localhost:3000

### ⁉️ FAQ

#### DuckDB is not being installed and complains about a hash, what should I do?

Execute:

```shell
poetry lock
```

Then try to install.

## 🚀 Deployment

Deployment is configured using CI/CD.

How to package this DAG is defined in the 'BUILD' file in the root of this repository.

Packaging the repository is managed using [pants](https://www.pantsbuild.org/).

To package the repository, execute:

```shell
pants package dags/luchtmeetnet_ingestion
```

## Required environment variables

This deployment requires:

- `DAGSTER_SECRET_SLACK_BOT_OAUTH_TOKEN`: OAuth2 token to authenticate with slack. For dev, this isn't used so you can fill out anything you like.
