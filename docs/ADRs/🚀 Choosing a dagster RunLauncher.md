---
creation date: 2024-02-18 10:02
tags:
  - ADR
  - template
  - GKE
  - kubernetes
  - dagster
  - orchestration
  - configuration
template: "[[🏷 Templates/ADR template]]"
status: ✅ Accepted
homepage:
---
## 📜 Table of contents
---
```table-of-contents
```
## ✍️ Context
---
There are a couple of different options to launch Dagster jobs in the Dagster OS deployment on GKE. We need to make a decision on:

1. Which [RunLauncher](https://docs.dagster.io/deployment/run-launcher) we're using to run Dagster jobs
2. Which [Executor](https://docs.dagster.io/deployment/executors#example-executors) we're using to run individual assets within a Dagster job

Broadly speaking, we have two options:

1. Run every Dagster job in their own pod, and run every Dagster asset in their own pod.
2. Run every Dagster job in their own pod, and use a multiprocess executor to run Dagster assets in this pod.
## 🤝 Decision
---
For the time being, we are using the following settings:

- We use the [K8sRunLauncher](https://docs.dagster.io/_apidocs/libraries/dagster-k8s#dagster_k8s.K8sRunLauncher) to launch Dagster job runs. This will spin up a new pod for the K8s run. The RunLauncher is specified in the Dagster helm chart, and set in the [dagster-infra repository](https://github.com/JasperHG90/dagster-infra/blob/main/infra/app.tf#L58-L61).
- We use a [multiprocess_executor](https://docs.dagster.io/_apidocs/execution#dagster.multiprocess_executor) to launch asset runs within a job. This will execute work on the job kubernetes pod instead of spinning up new pods.

## ☝️Consequences
---
- Engineer needs to think about resource usage when defining the executor for their job. Specifically:
	- How much resources are available to the Dagster job pod (see [[📉 Limiting resource usage in Dagster jobs]])
	- Which executor is suitable for the types of assets that I'm running?
	- How does this impact job concurrency (see [[⚙️ Limiting concurrency for Dagster jobs]])
	- Should I set [asset concurrency limits](https://docs.dagster.io/guides/limiting-concurrency-in-data-pipelines#limiting-overall-concurrency-in-a-job) in the job specification?
## 🔗 References
---
- [[📉 Limiting resource usage in Dagster jobs]]
