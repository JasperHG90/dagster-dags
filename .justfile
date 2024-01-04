venv:
  python -m venv .venv

venv_activate:
  . .venv/bin/activate

pip:
  pip install -r requirements.txt --upgrade pip

pre_commit_setup:
  pre-commit install

setup: venv pip pre_commit_setup

pre_commit: venv_activate
  pre-commit run -a
