venv:
  python -m venv .venv
  . .venv/bin/activate

pip:
  pip install -r requirements.txt --upgrade pip

pre_commit_setup:
  pre-commit install

setup: venv pip pre_commit_setup
