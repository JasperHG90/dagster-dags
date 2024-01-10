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

fmt:
  pants fmt ::

lint:
  pants lint :: # check

test:
  pants test ::

package:
  pants package ::

webserver:
  #!/usr/bin/env bash
  set -eo pipefail
  DAGSTER_WEBSERVER_POD_NAME=$(kubectl get pods --namespace dagster-prd -l "app.kubernetes.io/name=dagster,app.kubernetes.io/instance=dagster,component=dagster-webserver" | cut -d' ' -f 1 | sed -n '2p')
  kubectl --namespace dagster-prd port-forward $DAGSTER_WEBSERVER_POD_NAME 8080:80

docker_login:
  #!/usr/bin/env bash
  set -e
  ARTIFACT_SA=$(gcloud secrets versions access latest \
    --secret=CONTAINERADMIN_PRD_SA_JSON_KEY_B64 \
    | python -m base64 -d)
  docker login -u _json_key -p "$ARTIFACT_SA" europe-west4-docker.pkg.dev
