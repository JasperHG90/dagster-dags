# Dagster Scripts

## Running backfills

- Ensure that you are running a dagster server somewhere
- Add .env (copy from .env.example)
- Run `just install`
- Run `just backfill <CONFIG>` e.g. `just backfill ../../jobs/conf/backfills/backfill-22022024-5.yml`
