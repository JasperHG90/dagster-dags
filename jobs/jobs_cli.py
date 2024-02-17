import logging

import typer

import utils

logger = logging.getLogger("jobs_cli")
handler = logging.StreamHandler()
format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(format)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


app = typer.Typer(
    help="🧰 Command-line tools for parsing manual production jobs",
    no_args_is_help=True,
)


@app.callback()
def main(trace: bool = typer.Option(False, help="Enable debug logging.")):
    if trace:
        logger.setLevel(logging.DEBUG)


@app.command(
    name="parse",
    help="Parse a manual production job into a kubernetes job spec.",
    no_args_is_help=True
)
def parse_job(
    job_config: str = typer.Argument(
        ..., help="Path to the job configuration file."
    ),
    output_path: str = typer.Argument(
        ..., help="Path to the output kubernetes job spec."
    ),
    image: str = typer.Argument(
        ..., help="The docker image to use for the job."
    ),
    image_tag: str = typer.Argument(
        ..., help="The docker image tag to use for the job."
    ),
    command: str = typer.Argument(
        ..., help="The command to run in the job."
    ),
):
    logger.info("Parsing job...")
    utils.parse_and_write_template(
        job_config, output_path, image, image_tag, command
    )


def entrypoint():
    app()
