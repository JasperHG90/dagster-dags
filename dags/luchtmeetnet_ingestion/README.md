# 🌬️ Luchtmeetnet ingestion

## Developing locally

### Installing

Execute `just install`/`just i` to install the required dependencies.

### Environment variables

You need to create a .env file in the root directory (dags/luchtmeetnet_ingestion) with the following keys/values:

```
DAGSTER_SECRET_SLACK_BOT_OAUTH_TOKEN="xoxb-xxxxxx-xxxxxxxx-xxxxxxx-xxxxxxxxxxxxxxxxx"
DAGSTER_SECRET_REDIS_HOST=localhost
DAGSTER_SECRET_REDIS_USERNAME=default
DAGSTER_SECRET_REDIS_PASSWORD=dagster
```

NB: the `DAGSTER_SECRET_SLACK_BOT_OAUTH_TOKEN` is **not** used when developing. It's value could be anything.

### Starting the dagster server

You should boot dagster using the `just dev`/`just d` command.

**You need to start docker before running `just dev`**

Before starting `dagster dev`, this command:

1. Creates a '.dagster' folder
2. Copies the `dagster.yaml` file to this folder
3. Sets the `DAGSTER_HOME` environment variable to the '.dagster' folder
4. Boots up a redis server using Docker as long as the Dagster server is running. See below for more info.

The dagster web UI is hosted on http://localhost:3000

To remove the '.dagster' folder, you can execute `just clean`/`just c`

### Use of Redis server

This Dagster project requires a Redis server so that we can limit requests to the Luchtmeetnet API. We use [pyrate-limiter](https://pypi.org/project/pyrate-limiter/) to configure rate limit requests with Redis as a backend, since Dagster spawns resources for each separate run and we need to track state.

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
- `DAGSTER_SECRET_REDIS_HOST`: Host of a redis database, e.g. hosted on Redis cloud.
- `DAGSTER_SECRET_REDIS_USERNAME`: Username of the Redis database.
- `DAGSTER_SECRET_REDIS_PASSWORD`: Password of the user that is used to connect to the Redis database.

You can set these variables in 'values.yaml.j2'. They need to be provisioned using the 'add_secrets' workflow in the [dagster-infra](https://github.com/JasperHG90/dagster-infra) repository.

To do this, navigate to the [dagster-infra repository settings](https://github.com/JasperHG90/dagster-infra/settings/secrets/actions) and add the secrets there using the prefix 'DAGSTER_SECRET_'.

Then, run the [Add secrets](https://github.com/JasperHG90/dagster-infra/actions/workflows/add_secrets.yml) workflow.
