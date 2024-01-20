poetry_requirements(
    name="poetry",
)

resources(name="pyproject", sources=["pyproject.toml", "README.md"])

python_distribution(
    name="dist",
    dependencies=[":pyproject", ":poetry", "shared/dagster_utils/src/dagster_utils:dagster_utils"],
    generate_setup=False,
    wheel=True,
    sdist=True,
    provides=python_artifact(name="dagster_utils"),
)
