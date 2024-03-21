# 🌬️ Luchtmeetnet ingestion

## What this DAG does

This DAG ingests data from the [Luchtmeetnet](https://www.luchtmeetnet.nl/) open [API](https://api-docs.luchtmeetnet.nl/).

The development workflow looks as follows:

![](./architecture/luchtmeetnet_ingestion_dev.png)

The production workflow looks as follows:

![](./architecture/luchtmeetnet_ingestion_prod.png)

Where:

![](./architecture/legend.png)

## Stations

[Station](https://api-docs.luchtmeetnet.nl/#7a6126f0-95c9-45f6-9552-6d97c9a418bf) data is ingested on the first day of each month on a CRON schedule. The asset that ingests the station data is partitioned by station number. A second asset copies the data to the data lake.

## Station measurements

[Station measurements](https://api-docs.luchtmeetnet.nl/#6ac38ef4-5d43-4d9b-b0b9-69b2d00e6f4c) are ingested daily via a scheduled asset job. The data is partitioned by station number and date. A second asset job copies the data to the data lake.

> **Why not a single job?** Assets that grouped in the same asset job must share the same partition set.

## Peculiarities

### Custom sensor to trigger downstream job

- AQD is partitioned by station and date
- We still want downstream jobs to run, even if some stations fail
- This is not supported out of the box
- Custom sensor checks if a backfill or a scheduled run is complete (failures and successes) and triggers downstream job

### Rate limiter

- LMN has rate limit
- Because dagster runs each job in their own process, we cannot use an in-memory rate limiter
- Rate limiter uses Redis backend to keep track of rate limits

### I/O manager

- Duckdb I/O manager reads partitioned data even if upstream is not entirely completed (e.g. failed partitions)
- Support both partitioned and non-partitioned assets

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
