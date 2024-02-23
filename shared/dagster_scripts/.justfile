set dotenv-load

alias i := install
alias bf := backfill
alias ras := report_asset_status

# Install poetry dependencies
install:
    poetry install

backfill config_location:
  poetry run dagster_scripts --trace backfill {{config_location}}

report_asset_status config_location:
  poetry run dagster_scripts --trace report-asset-status {{config_location}}
