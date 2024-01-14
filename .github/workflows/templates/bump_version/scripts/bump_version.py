import logging
import pathlib as plb

import typer
import sh

logger = logging.getLogger("bump_version")
handler = logging.StreamHandler()
format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(format)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

app = typer.Typer()


def get_version(path):
    # NB: requires https://pypi.org/project/poetry-git-version-plugin/
    v = (
        sh.poetry(
            '--directory',
            path,
            'git-version',
            '--no-ansi'
        )
        .strip()
    )
    logger.debug(f"Found version {v} for {path}")
    return v


def match_exclude(path, exclude):
    _path = set(str(path).split("/"))
    return True if len(_path.intersection(exclude)) > 0 else False


@app.command()
def bump_version(
    version: str = typer.Option(
        None,
        help="Version to bump to",
    ),
    root: str = typer.Option(
        ".",
        help="Path to root of project",
    ),
    exclude: str = typer.Option(
        ".venv,dist",
        help="Comma-separated list of paths to exclude from version bumping",
    )
):
    _exclude = set(exclude.split(","))
    logger.debug(f"Excluding paths {_exclude}")
    p = plb.Path(root).resolve()
    logger.info(f"Base directory {str(p)}")
    pyprojs = [py.resolve() for py in p.glob("*/**/pyproject.toml")]
    pyprojs = [py for py in pyprojs if not match_exclude(py, _exclude)]
    logger.info(f"Found {len(pyprojs)} pyproject.toml files")
    for pyproj in pyprojs:
        logger.debug(f"Processing {str(pyproj)}")
        if version is None:
            version = get_version(pyproj.parent)
        logger.info(f"Bumping version of {str(pyproj)} to {version}")
        sh.poetry('--directory', pyproj.parent, 'version', version)


if __name__=="__main__":
    app()
