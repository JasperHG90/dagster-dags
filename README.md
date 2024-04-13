# 🚧 WARNING

This repository is under construction and contains untested, experimental and unfinished code. This code is not production-ready, and while it could be useful for others, should **not** be copied over without appropriate scrutiny.

Use at your own risk.

# Dagster-dags

Monorepo containing dagster DAGs.

## Overview

## Setting up

The resources in this repository assume that you have set up your infrastructure using the resources in the [dagster-infra]() repository.

### Initial setup

- Install the gcloud client
- Log into your own GCP account `gcloud auth login`
- Set the default project `gcloud config set project <project-name>`
- Also set the default project and cluster base name in the `.justfile`
- Execute `gcloud components install gke-gcloud-auth-plugin`

- Install just
- Install pants
- Install poetry

### Authenticating with your cluster

Execute `just authenticate_kubectl` to authenticate with the kubernetes cluster.

## CI/CD

The CI/CD pipelines defined under '.github'
