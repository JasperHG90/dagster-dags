import logging

import typer

from dagster_scripts import __version__

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
    name="validate-config",
    help="✅ Validate a configuration file.",
    short_help="✅ Validate a configuration file.",
    no_args_is_help=True,
)
def _validate_config(path_to_config: str = typer.Argument(None, help="Path to a config file.")):
    ...
    #_ = commands.load_and_validate_config(path_to_config, _)


def entrypoint():
    app()
