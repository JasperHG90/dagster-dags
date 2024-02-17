---
creation date: 2024-02-17 13:02
tags:
  - ADR
  - template
  - dagster
  - GKE
  - configuration
  - orchestration
template: "[[🏷 Templates/ADR template]]"
status: ✅ Accepted
---
- [[#✍️ Context|✍️ Context]]
- [[#🤝 Decision|🤝 Decision]]
- [[#☝️Consequences|☝️Consequences]]
- [[#➡️ Follow-ups|➡️ Follow-ups]]

## ✍️ Context
---
We need to be able to control the resources that are used by a Dagster job running on GKE. Each job uses its own pod, but not all pods require the same resources.

## 🤝 Decision
---
We will leave it up to the engineer designing the DAG which resources are required. This can be done by specifying the following configuration on a Dagster job:

```python
from dagster import define_asset_job, multiprocess_executor


ingestion_job = define_asset_job(
	...
	tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {
	                    "cpu": "100m",
	                    "memory": "64Mi"
	                },
                    "limits": {
	                    "cpu": "100m",
	                    "memory": "64Mi"
	                },
                },
            },
        }
    },
)
```
Other options are described [here](https://docs.dagster.io/deployment/guides/kubernetes/customizing-your-deployment).
## ☝️Consequences
---
Easier
- Users can specify their own needs in terms of compute resources

Harder
- Need to monitor compute resources to see if lots of compute is going idle.

## ➡️ Follow-ups
- RFC: monitoring compute resources (and alerting)
- Write documentation about setting resource constraints, and how you should determine these.
