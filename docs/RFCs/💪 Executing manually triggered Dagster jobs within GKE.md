---
creation date: 2024-02-17 11:02
tags:
  - RFC
  - template
  - kubernetes
  - GKE
  - dagster
  - orchestration
  - one-time-jobs
template: "[[🏷 Templates/RFC template]]"
status: ✅ Accepted
---
- [[#🤓 TL;DR;|🤓 TL;DR;]]
- [[#🔭 Context and Scope|🔭 Context and Scope]]
- [[#🎯 Goals (and Non-Goals)|🎯 Goals (and Non-Goals)]]
- [[#🦉 The Actual Design|🦉 The Actual Design]]
	- [[#🦉 The Actual Design#🐍 Python implementation|🐍 Python implementation]]
- [[#🤝 Final decision|🤝 Final decision]]
- [[#☝️ Follow-ups|☝️ Follow-ups]]
- [[#🔗 Related|🔗 Related]]

## 🤓 TL;DR;
---
This RFC will look at how we can execute the library CLI commands on the GKE server. In short: we trigger a Kubernetes job that executes the CLI command in the dagster namespace on GKE.

## 🔭 Context and Scope
---
RFC [[🔘 Manually triggering jobs on the Dagster GKE production server using a configuration file]] described how we will structure the python library that calls the Dagster GraphQL endpoint to execute jobs. Now, we want to know how we can actually use this on GKE.

Locally, we can spin up a server using `dagster dev`, and then visit 'https://localhost:3000/grapql' or use the Python client to interact with the GraphQL endpoint. On Kubernetes, this is not possible because ingress is not allowed.

In scope:
- Design method so that we can execute CLI commands using the Dagster graphql endpoint on GKE

Out of scope:
- Implementing the approach
- Defining where the templates & configurations will live in the repository. References made to these objects are examples.
- Figuring out the location of the GraphQL dagster server on the GKE cluster.

## 🎯 Goals (and Non-Goals)
---
Goals:
- Enable users to run one-time dagster jobs such as backfills from a configuration file instead of the UI
- Ensure flexible approach that can take as input any command

## 🦉 The Actual Design
---
Because we need to access the Dagster server on GKE, we could simply port-forward the server address using `kubectl`. Not sure how stable this would be and also doesn't leave an artifact for us to associate with the CI/CD pipeline.

A better approach is to use a Kubernetes job in the same namespace as the Dagster server. We can define the entrypoint in the job spec and add the configuration as a mountable volume. The job spec can be saved to disk and uploaded as an artifact in CI/CD. We can use Jinja templating to fill in values that are only known at runtime (e.g. configuration version, command, image etc.).
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: manual-job-{{ job_name_suffix }}
  namespace: dagster-prod
spec:
  ttlSecondsAfterFinished: 86400 # 1 day
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: kubernetes
      containers:
      - name: manual-job
        # This refers to "dagster_scripts" docker image
        image: {{ image }} # Specify version in tag
        command: ["dagster_scripts", "{{ command }}"]
        args: ["/etc/configs/config.yml"]
        env:
        # These can be used for logging purposes
        - name: JOB_NAME
          value: "manual-job-{{ job_name_suffix }}"
        - name: CONFIG_VERSION
          value: "{{ config_version }}"
        resources:
          requests:
            # Only calling API so very small resource
            #  requirements
            cpu: "100m"
            memory: "64Mi"
        volumeMounts:
        - name: configs-volume
          mountPath: /etc/configs
      volumes:
        - name: configs-volume
          configMap:
            name: manual-job-config
      # Only try jobs once
      restartPolicy: "Never"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: configs-volume
  namespace: dagster-prod
data:
  config.yml: |
    {{ config | indent(4) }}
```

Some values in the template above need to be specified by the user, such as:
- Command that they want to run (e.g. "dagster_scripts backfill")
- Location of the configuration file (e.g. "\<REPO\>/scripts/conf/backfill-\<ISSUE-NUMBER\>.yml")
- Version of the "dagster_scripts" library (e.g. "0.0.1"). Might default to latest if not specified.
### 🐍 Python implementation
The following script shows how we can use python and jinja templating to create a job spec that can be executed on the GKE server.

Dagster job configuration (for CLI command)
```yaml
# ./job_config.yml
job_name: ingestion_job
repository_name: luchtmeetnet_ingestion
tags:
  name: "34543"
  partitions:
  - name: daily
    start_date: '2024-01-31'
    end_date: '2024-02-01'
  - name: stations
    values:
    - NL01497
    - NL01912
```
Jinja template for kubernetes job spec
```yaml
# ./templates/template.yml.j2
apiVersion: batch/v1
kind: Job
metadata:
  name: manual-job-{{ job_name_suffix }}
  namespace: dagster-prod
spec:
  ttlSecondsAfterFinished: 86400 # 1 day
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: kubernetes
      containers:
      - name: manual-job
        image: {{ image }}:{{ image_tag }}
        command: ["dagster_scripts", "{{ command }}"]
        args: ["/etc/configs/config.yml"]
        env:
        - name: JOB_NAME
          value: "manual-job-{{ job_name_suffix }}"
        - name: CONFIG_VERSION
          value: "{{ config_version }}"
        resources:
          requests:
            cpu: "100m"
            memory: "64Mi"
        volumeMounts:
        - name: configs-volume
          mountPath: /etc/configs
      volumes:
        - name: configs-volume
          configMap:
            name: manual-job-config
      restartPolicy: "Never"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: configs-volume
  namespace: dagster-prod
data:
  config.yml: |
    {{ config | indent(4) }}
```
Python script for rendering the template
```python
# ./parse_job.py
#%%
import typing
import pathlib as plb
import datetime as dt

import yaml
import pendulum
from pydantic import BaseModel
import jinja2
import coolname

#%% Configuration

class DateBackfillPartitionConfig(BaseModel):
	name: str
	start_date: dt.date
	end_date: dt.date

	def get_range(self):
		return pendulum.period(self.start_date, self.end_date).range('days')

	def config_dict(self):
		return [{f"dagster/partition/{self.name}": d.to_date_string()} for d in self.get_range()]


class StaticBackfillPartitionConfig(BaseModel):
	name: str
	values: typing.List[str]

	def config_dict(self):
		return [{f"dagster/partition/{self.name}": v} for v in self.values]


class BackfillTagsConfig(BaseModel):
	name: str
	partitions: typing.List[typing.Union[StaticBackfillPartitionConfig, DateBackfillPartitionConfig]]


class BackfillConfig(BaseModel):
	job_name: str
	repository_name: str
	tags: BackfillTagsConfig

#%% Utils

def load_config(path: typing.Union[str, plb.Path], config_cls: BaseModel) -> BaseModel:
	"""Load a YAML configuration file from disk"""
	path = plb.Path(path).resolve()
	if not path.exists():
		raise FileNotFoundError(f"Config at '{path}' not found")
	with path.resolve().open("r") as inFile:
		cnf_raw = yaml.safe_load(inFile)
		cnf_out = config_cls(**cnf_raw)
		return cnf_out

cnf = load_config("job_config.yml", BackfillConfig)

#%% Set up jinja environment

env = jinja2.Environment(
    loader=jinja2.FileSystemLoader("templates"), # Use PackageLoader("package-name", "templates-folder-name") for package
    autoescape=jinja2.select_autoescape(),
)

# NB: control whitespaces. See <https://ttl255.com/jinja2-tutorial-part-3-whitespace-control/>
env.trim_blocks = True
env.lstrip_blocks = True
env.keep_trailing_newline = True
# Load job spec
template = env.get_template("template.yml.j2")

#%% Render values in template

template_rendered = template.render(
	job_name_suffix=coolname.generate_slug(2),
	image="europe-west4-docker.pkg.dev/jasper-ginn-dagster/dags-areg-euw4-jgdag-prd/dagster_scripts",
	image_tag="0.1.0",
	command="backfill",
	config_version="v1",
	config=yaml.safe_dump(cnf.model_dump())
)

# %% Write to disk

with plb.Path("template_rendered.yml").open("w") as f:
    f.write(template_rendered)
```
Rendered template looks as follows:
```yaml
# ./template_rendered.yml
apiVersion: batch/v1
kind: Job
metadata:
  name: manual-job-precious-mongoose
  namespace: dagster-prod
spec:
  ttlSecondsAfterFinished: 86400 # 1 day
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: kubernetes
      containers:
      - name: manual-job
        # This refers to "dagster_scripts" docker image
        image: europe-west4-docker.pkg.dev/jasper-ginn-dagster/dags-areg-euw4-jgdag-prd/dagster_scripts:0.1.0 # Specify version in tag
        command: ["dagster_scripts", "backfill"]
        args: ["/etc/configs/config.yml"]
        env:
        # These can be used for logging purposes
        - name: JOB_NAME
          value: "manual-job-precious-mongoose"
        - name: CONFIG_VERSION
          value: "v1"
        resources:
          requests:
            # Only calling API so very small resource
            #  requirements
            cpu: "100m"
            memory: "64Mi"
        volumeMounts:
        - name: configs-volume
          mountPath: /etc/configs
      volumes:
        - name: configs-volume
          configMap:
            name: manual-job-config
      # Only try jobs once
      restartPolicy: "Never"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: configs-volume
  namespace: dagster-prod
data:
  config.yml: |
    job_name: ingestion_job
    repository_name: luchtmeetnet_ingestion
    tags:
      name: '34543'
      partitions:
      - end_date: 2024-02-01
        name: daily
        start_date: 2024-01-31
      - name: stations
        values:
        - NL01497
        - NL01912
```
Note that in the above implementation we've not added *where* the GraphQL endpoint lives.

## 🤝 Final decision
---
We will implement the above approach. The RFC [[🚀 Defining a CICD job to manually trigger Dagster jobs in GKE from a configuration file]] will detail what the manual CI/CD job looks like that uses this templated job spec to trigger a job on the Dagster server.

## ☝️ Follow-ups
---
- Ensure that the "dagster_scripts" library is packaged in a docker image that is available for the Kubernetes job to pull and run.
- Figure out the location of the GraphQL dagster endpoint. Hardcode this in e.g. environment variable in the job spec.

## 🔗 Related
---
- [[🔘 Manually triggering jobs on the Dagster GKE production server using a configuration file]]
- [[🚀 Defining a CICD job to manually trigger Dagster jobs in GKE from a configuration file]]
