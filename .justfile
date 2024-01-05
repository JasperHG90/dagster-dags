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

webserver:
  #!/usr/bin/env bash
  set -eo pipefail
  DAGSTER_WEBSERVER_POD_NAME=$(kubectl get pods --namespace dagster-prd -l "app.kubernetes.io/name=dagster,app.kubernetes.io/instance=dagster,component=dagster-webserver" | cut -d' ' -f 1 | sed -n '2p')
  kubectl --namespace dagster-prd port-forward $DAGSTER_WEBSERVER_POD_NAME 8080:80
