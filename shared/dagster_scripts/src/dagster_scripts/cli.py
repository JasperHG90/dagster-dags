import logging

import typer
from dagster_scripts import __version__
from dagster_scripts.commands.backfill import backfill, load_backfill_config

logger = logging.getLogger("dagster_scripts")
handler = logging.StreamHandler()
format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(format)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


app = typer.Typer(
    help="🧰 Command-line tools for running one-time dagster production jobs.",
    no_args_is_help=True,
)


@app.callback()
def main(trace: bool = typer.Option(False, help="Enable debug logging.")):
    if trace:
        logger.setLevel(logging.DEBUG)


@app.command(short_help="📌 Displays the current version number of the dagster_scripts library")
def version():
    print(__version__)


@app.command(
    name="backfill",
    help="🚀 Backfill a set of runs using a configuration file.",
    no_args_is_help=True,
)
def _backfill(
    config_path: str = typer.Argument(..., help="Path to the backfill configuration file.")
):
    cnf = load_backfill_config(config_path)
    backfill(cnf)


def entrypoint():
    app()
