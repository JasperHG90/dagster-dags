---
creation date: 2024-02-17 13:02
tags:
  - ADR
  - template
  - dagster
  - concurrency
  - GKE
  - partitions
template: "[[🏷 Templates/ADR template]]"
status: ✅ Accepted
---
- [[#✍️ Context|✍️ Context]]
- [[#🤝 Decision|🤝 Decision]]
	- [[#🤝 Decision#🏷️ Tag-based concurrency limits|🏷️ Tag-based concurrency limits]]
- [[#☝️Consequences|☝️Consequences]]
- [[#➡️ Follow-ups|➡️ Follow-ups]]
- [[#🔗 References|🔗 References]]

## ✍️ Context
---
We want to limit concurrency on Dagster jobs because. When we are triggering lots of jobs (e.g. because of a job with a lot of partitions), we don't want to open up too many connections to the PostgreSQL database, or start too many pods at the same time on the GKE cluster.

## 🤝 Decision
---
We did some research in [[⚙️ Dagster concurrency]], and found that the easiest way to limit concurrency is to impose it on the entire cluster on the **job** level (all options described [here](https://docs.dagster.io/guides/limiting-concurrency-in-data-pipelines)). It is possible to scope this to the type of job (e.g. backfills versus other runs, example below).

> [!important]
> You can limit concurrency at different levels. This ADR only describes limiting job concurrency. It does not describe how you can limit asset concurrency (e.g. if you have lots of assets running in a single run). This should be configured in the job specification of a DAG. More information can be found [here](https://docs.dagster.io/guides/limiting-concurrency-in-data-pipelines#configuring-opasset-level-concurrency).

Limiting concurrency using this approach is achieved by setting the following property when deploying the Dagster Helm chart:

```terraform
  set {
    name  = "dagsterDaemon.runCoordinator.config.queuedRunCoordinator.maxConcurrentRuns"
    value = 5
  }
```

This uses the default `QueuedRunCoordinator`. See the [dagster documentation](https://docs.dagster.io/deployment/run-coordinator) for more information.

### 🏷️ Tag-based concurrency limits
Dagster allows you to set concurrency limits [based on tags](https://docs.dagster.io/guides/limiting-concurrency-in-data-pipelines#configuring-opasset-level-concurrency). For now, this is only configured for backfills, which are limited to three concurrent runs.

```terraform
  set {
    name  = "dagsterDaemon.runCoordinator.config.queuedRunCoordinator.tagConcurrencyLimits[0].key"
    value = "dagster/backfill"
  }

  set {
    name  = "dagsterDaemon.runCoordinator.config.queuedRunCoordinator.tagConcurrencyLimits[0].limit"
    value = 3
  }
```

## ☝️Consequences
---
Easier:
- Don't have to worry about concurrency at the DAG level since this has been taken care of by the system.
- Can use tags to change default concurrency limit of five jobs if required.

## ➡️ Follow-ups
---
- Check out advanced limits on orchestration using e.g. [tags defined on jobs](https://docs.dagster.io/guides/limiting-concurrency-in-data-pipelines#limiting-concurrency-using-tags)
- Check out limiting global [job concurrency across runs](https://docs.dagster.io/guides/limiting-concurrency-in-data-pipelines#limiting-opasset-concurrency-across-runs)

## 🔗 References
---
- [[⚙️ Dagster concurrency]]
