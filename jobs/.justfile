alias p := parse
alias dep := deploy
alias e := exec
alias j := jobs
alias l := logs
alias del := delete
alias po := pods

parse command job_config:
    python jobs_cli.py parse \
        {{job_config}} \
        ./job_spec.yml \
        europe-west4-docker.pkg.dev/jasper-ginn-dagster/utilities-areg-euw4-jgdag-prd/dagster_scripts \
        latest \
        {{command}} \
        7941458770 \
        https://github.com/JasperHG90/dagster-dags/actions/runs/7941458770

deploy job_spec:
    kubectl apply -f ./job_spec.yml -n dagster-prd

exec pod_name:
    kubectl exec \
    --stdin \
    --tty \
    {{pod_name}} \
    -n dagster-prd -- /bin/bash

jobs:
    kubectl get jobs -n dagster-prd

pods:
    kubectl get pods -n dagster-prd

delete type name:
    kubectl delete {{type}} {{name}} -n dagster-prd

logs name:
    kubectl logs {{name}} -n dagster-prd --stream

attach name:
    kubectl attach {{name}} -n dagster-prd
