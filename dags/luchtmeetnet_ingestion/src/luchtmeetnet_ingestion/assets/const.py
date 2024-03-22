K8S_TAGS = {
    "dagster-k8s/config": {
        "container_config": {
            "resources": {
                "requests": {"cpu": "100m", "memory": "64Mi"},
                "limits": {"cpu": "100m", "memory": "64Mi"},
            },
        },
        "job_spec_config": {"ttl_seconds_after_finished": 7200},
    }
}
