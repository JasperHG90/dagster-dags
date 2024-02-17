Job configurations for one-time scripts go here

```shell
python jobs_cli.py parse \
    /Users/jasperginn/Documents/code_projects/dagster/dagster-dags/jobs/conf/backfill-170224.yml \
    ./job_spec.yml \
    europe-west4-docker.pkg.dev/jasper-ginn-dagster/dags-areg-euw4-jgdag-prd/dagster_scripts \
    latest \
    backfill \
    7941458770 \
    https://github.com/JasperHG90/dagster-dags/actions/runs/7941458770
```

Then, you can execute a job using kubectl:

```shell
kubectl apply -f job_spec.yml -n dagster-prd
```
