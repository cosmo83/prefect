# prefect-cloudfoundry

Prefect integration for executing flow runs on Cloud Foundry with custom Docker images or ephemeral Tasks.

## Overview

`prefect-cloudfoundry` provides a custom worker (`CloudFoundryWorker`) and work pool type (`cloud-foundry`) that executes Prefect flow runs using two execution modes:

1. **`execution_mode: "app"` (Ephemeral Docker App - Default)**:
   - Spawns a dedicated Cloud Foundry application instance directly from any specified Docker image (`image: "docker.io/my-org/my-image:tag"`).
   - Injects flow run environment variables and credentials.
   - Starts the container instance to execute the flow run.
   - Once the flow run completes (or fails/crashes), **automatically issues `DELETE /v3/apps/{app_guid}` to completely destroy and purge the instance**, releasing all CPU and memory.

2. **`execution_mode: "task"` (Cloud Foundry Task)**:
   - Submits a one-off task against an existing staged CF application droplet via `POST /v3/apps/{app_guid}/tasks`.
   - Diego destroys the task container instance upon completion.

---

## Credentials Configuration

Configure the `CloudFoundryCredentials` block with your Cloud Foundry endpoint, user credentials, and target organization and space:

```python
from pydantic import SecretStr
from prefect_cloudfoundry import CloudFoundryCredentials

cf_creds = CloudFoundryCredentials(
    api_url="https://api.sys.example.com",
    username="cf-admin-user",
    password=SecretStr("supersecretpassword"),
    org_name="my-organization",
    space_name="my-space",
)
cf_creds.save("my-cf-credentials", overwrite=True)
```

---

## Work Pool Configuration

### 1. Create the Work Pool

```bash
prefect work-pool create "cf-docker-pool" --type cloud-foundry
```

### 2. Configure Pool / Deployment Variables

In the UI or `prefect.yaml`:

```json
{
  "credentials": "{{ prefect.blocks.cloud-foundry-credentials.my-cf-credentials }}",
  "execution_mode": "app",
  "image": "docker.io/my-org/my-prefect-flow:latest",
  "memory_in_mb": 2048,
  "disk_in_mb": 1024,
  "auto_remove_app": true
}
```

*For private container registries, you can also specify `registry_username` and `registry_password`.*

### 3. Start the Worker

```bash
prefect worker start --pool "cf-docker-pool"
```
