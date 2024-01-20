poetry_requirements(
    name="poetry",
)

resources(name="pyproject", sources=["pyproject.toml", "README.md"])

python_distribution(
    name="dist",
    dependencies=[":pyproject", ":poetry", "shared/delt/src/delt:delt"],
    generate_setup=False,
    wheel=True,
    sdist=True,
    provides=python_artifact(name="delt"),
)
