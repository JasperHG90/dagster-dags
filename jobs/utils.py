import typing
import pathlib as plb

import yaml
import jinja2
import coolname

_path = plb.Path(__file__).resolve().parent

env = jinja2.Environment(
    loader=jinja2.FileSystemLoader("templates"), # Use PackageLoader("package-name", "templates-folder-name") for package
    autoescape=jinja2.select_autoescape(),
)
# NB: control whitespaces. See <https://ttl255.com/jinja2-tutorial-part-3-whitespace-control/>
env.trim_blocks = True
env.lstrip_blocks = True
env.keep_trailing_newline = True


def load_config(path: typing.Union[str, plb.Path]) -> dict:
    """Load a YAML configuration file from disk"""
    path = plb.Path(path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Config at '{path}' not found")
    with path.resolve().open("r") as inFile:
        return yaml.safe_load(inFile)


def parse_and_write_template(
    config_path: typing.Union[str, plb.Path],
    output_path: typing.Union[str, plb.Path],
    image: str,
    image_tag: str,
    command: str,
    github_actions_run_id: str,
    github_actions_url: str,
    debug: bool = False,
):
    cnf = load_config(config_path)
    dagster_config = load_config(_path / "static" / "dagster.yaml")
    template = env.get_template("job.yml.j2")
    template_rendered = template.render(
        job_name_suffix=coolname.generate_slug(2),
        image=image if not debug else "ubuntu",
        image_tag=image_tag if not debug else "latest",
        command=command,
        args=", ".join(['"/etc/configs/config.yml"']) if not debug else ", ".join(['"while true; do sleep 30; done;"']),
        github_actions_run_id=github_actions_run_id,
        github_actions_url=github_actions_url,
        config_version="v1",
        config=yaml.safe_dump(cnf),
        dagster_config=yaml.safe_dump(dagster_config)
    )
    with plb.Path(output_path).open("w") as f:
        f.write(template_rendered)
