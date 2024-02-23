alias p := parse
alias d := deploy

parse job_config:
    python jobs_cli.py parse \
        {{job_config}} \
        ./job_spec.yml \
        europe-west4-docker.pkg.dev/jasper-ginn-dagster/utilities-areg-euw4-jgdag-prd/dagster_scripts \
        latest \
        backfill \
        7941458770 \
        https://github.com/JasperHG90/dagster-dags/actions/runs/7941458770

deploy job_spec:
    kubectl apply -f ./job_spec.yml -n dagster-prd
