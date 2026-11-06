"""
Prefect Worker for executing flow runs on Cloud Foundry using ephemeral Docker containers.

Spawns a temporary Cloud Foundry application directly from a specified Docker image,
runs the flow run, and destroys the container instance/app immediately upon completion.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, Optional
from uuid import UUID

import anyio
import httpx
from pydantic import Field, SecretStr
from slugify import slugify

from prefect.exceptions import InfrastructureNotFound
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from prefect_cloudfoundry.credentials import CloudFoundryCredentials

if TYPE_CHECKING:
    import anyio.abc
    from prefect.client.schemas.objects import FlowRun


class CloudFoundryVariables(BaseVariables):
    """
    Variables accepted by the Cloud Foundry worker template.
    """
    image: str = Field(
        ...,
        description="The Docker image URI (e.g., docker.io/my-org/my-image:tag) used for the container instance.",
    )
    registry_username: Optional[str] = Field(
        default=None,
        description="Optional username for authenticating with a private container registry.",
    )
    registry_password: Optional[SecretStr] = Field(
        default=None,
        description="Optional password for authenticating with a private container registry.",
    )
    credentials: Optional[CloudFoundryCredentials] = Field(
        default=None,
        description="The Cloud Foundry credentials block to authenticate with the Cloud Controller API.",
    )
    memory_in_mb: int = Field(
        default=1024,
        description="Memory limit in megabytes allocated to the container instance.",
    )
    disk_in_mb: int = Field(
        default=1024,
        description="Disk limit in megabytes allocated to the container instance.",
    )
    auto_remove_app: bool = Field(
        default=True,
        description="Whether to automatically delete the Cloud Foundry application once execution finishes.",
    )
    poll_interval_seconds: int = Field(
        default=5,
        description="Interval in seconds between polling Cloud Foundry for status updates.",
    )


class CloudFoundryJobConfiguration(BaseJobConfiguration):
    """
    Job configuration for Cloud Foundry execution.
    """
    credentials: Optional[CloudFoundryCredentials] = None
    image: str
    registry_username: Optional[str] = None
    registry_password: Optional[SecretStr] = None
    memory_in_mb: int = 1024
    disk_in_mb: int = 1024
    auto_remove_app: bool = True
    poll_interval_seconds: int = 5

    def prepare_for_flow_run(
        self,
        flow_run: "FlowRun",
        deployment=None,
        flow=None,
        work_pool=None,
        worker_name: str | None = None,
        worker_id: UUID | None = None,
    ) -> None:
        super().prepare_for_flow_run(
            flow_run=flow_run,
            deployment=deployment,
            flow=flow,
            work_pool=work_pool,
            worker_name=worker_name,
            worker_id=worker_id,
        )


class CloudFoundryWorkerResult(BaseWorkerResult):
    """
    Result returned by the Cloud Foundry worker upon execution termination.
    """
    identifier: str
    status_code: int


class CloudFoundryWorker(
    BaseWorker[CloudFoundryJobConfiguration, CloudFoundryVariables, CloudFoundryWorkerResult]
):
    """
    Prefect worker that executes flow runs on Cloud Foundry as ephemeral Docker containers.
    """

    type: str = "cloud-foundry"
    job_configuration = CloudFoundryJobConfiguration
    job_configuration_variables = CloudFoundryVariables
    _description: str = (
        "Execute flow runs on Cloud Foundry using dynamic Docker container instances. "
        "Instances are destroyed immediately upon completion."
    )
    _display_name: str = "Cloud Foundry"
    _logo_url: str = (
        "https://raw.githubusercontent.com/cloudfoundry/logos/master/CF_Icon_4-colour.svg"
    )

    async def run(
        self,
        flow_run: "FlowRun",
        configuration: CloudFoundryJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus[str]] = None,
    ) -> CloudFoundryWorkerResult:
        """
        Creates an ephemeral Cloud Foundry application backed by a Docker image,
        executes the flow run, and destroys the app upon completion.
        """
        logger = self.get_flow_run_logger(flow_run)

        if not configuration.image:
            raise ValueError("An `image` (Docker image URI) must be specified.")
        if not configuration.credentials:
            raise ValueError(
                "CloudFoundryCredentials must be provided in the work pool configuration."
            )

        token = await configuration.credentials.get_auth_token()
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
        }
        space_guid = await configuration.credentials.get_space_guid()

        clean_name = slugify(flow_run.name or "flow")[:25]
        app_name = f"prefect-{clean_name}-{str(flow_run.id)[:8]}"
        app_guid = None

        async with httpx.AsyncClient(
            base_url=configuration.credentials.api_url, timeout=30.0
        ) as client:
            try:
                # 1. Create App with Docker lifecycle
                logger.info(f"Creating ephemeral Cloud Foundry Docker app '{app_name}'...")
                app_payload = {
                    "name": app_name,
                    "relationships": {
                        "space": {"data": {"guid": space_guid}}
                    },
                    "lifecycle": {
                        "type": "docker",
                        "data": {}
                    },
                }
                resp = await client.post("/v3/apps", json=app_payload, headers=headers)
                resp.raise_for_status()
                app_guid = resp.json()["guid"]

                if task_status:
                    task_status.report(app_guid)

                # 2. Inject Environment Variables into App
                env_vars = {
                    k: str(v) for k, v in configuration.env.items() if v is not None
                }
                if env_vars:
                    env_resp = await client.patch(
                        f"/v3/apps/{app_guid}/environment_variables",
                        json={"var": env_vars},
                        headers=headers,
                    )
                    env_resp.raise_for_status()

                # 3. Create Docker Package
                logger.info(f"Setting Docker image '{configuration.image}' for app {app_guid}...")
                pkg_data: dict[str, Any] = {"image": configuration.image}
                if configuration.registry_username and configuration.registry_password:
                    pkg_data["username"] = configuration.registry_username
                    pkg_data["password"] = configuration.registry_password.get_secret_value()

                pkg_payload = {
                    "type": "docker",
                    "relationships": {
                        "app": {"data": {"guid": app_guid}}
                    },
                    "data": pkg_data,
                }
                pkg_resp = await client.post("/v3/packages", json=pkg_payload, headers=headers)
                pkg_resp.raise_for_status()
                pkg_guid = pkg_resp.json()["guid"]

                # 4. Stage Droplet from Package
                droplet_resp = await client.post(
                    f"/v3/packages/{pkg_guid}/droplets",
                    json={},
                    headers=headers,
                )
                droplet_resp.raise_for_status()
                droplet_guid = droplet_resp.json()["guid"]

                # Poll until droplet is STAGED
                for _ in range(60):
                    d_check = await client.get(f"/v3/droplets/{droplet_guid}", headers=headers)
                    d_check.raise_for_status()
                    d_state = d_check.json().get("state")
                    if d_state == "STAGED":
                        break
                    elif d_state == "FAILED":
                        error = d_check.json().get("error", "Failed to stage docker image")
                        raise RuntimeError(f"Droplet staging failed: {error}")
                    await asyncio.sleep(2)

                # Assign current droplet
                cur_drop_resp = await client.patch(
                    f"/v3/apps/{app_guid}/relationships/current_droplet",
                    json={"data": {"guid": droplet_guid}},
                    headers=headers,
                )
                cur_drop_resp.raise_for_status()

                # 5. Configure Process Resources & Healthcheck
                proc_resp = await client.get(f"/v3/apps/{app_guid}/processes", headers=headers)
                proc_resp.raise_for_status()
                processes = proc_resp.json().get("resources", [])
                web_proc = next((p for p in processes if p.get("type") == "web"), None)
                if web_proc:
                    web_proc_guid = web_proc["guid"]
                    cmd = configuration.command or "prefect flow-run execute"
                    patch_proc_payload = {
                        "command": cmd,
                        "memory_in_mb": configuration.memory_in_mb,
                        "disk_in_mb": configuration.disk_in_mb,
                        "health_check": {
                            "type": "process"
                        },
                    }
                    p_resp = await client.patch(
                        f"/v3/processes/{web_proc_guid}",
                        json=patch_proc_payload,
                        headers=headers,
                    )
                    p_resp.raise_for_status()

                # 6. Start the App Instance
                logger.info(f"Starting ephemeral app instance for flow run {flow_run.id}...")
                start_resp = await client.post(f"/v3/apps/{app_guid}/actions/start", headers=headers)
                start_resp.raise_for_status()

                # 7. Monitor Flow Run Execution
                status_code = 0
                while True:
                    await asyncio.sleep(configuration.poll_interval_seconds)

                    # Check Prefect flow run state
                    try:
                        latest_run = await self.client.read_flow_run(flow_run.id)
                        if latest_run.state and latest_run.state.is_final():
                            status_code = 0 if latest_run.state.is_completed() else 1
                            logger.info(
                                f"Flow run {flow_run.id} reached final state: {latest_run.state.name}"
                            )
                            break
                    except Exception:
                        pass

                    # Check CF Process status in case container crashed
                    if web_proc:
                        stats_resp = await client.get(
                            f"/v3/processes/{web_proc['guid']}/stats", headers=headers
                        )
                        if stats_resp.status_code == 200:
                            stats_res = stats_resp.json().get("resources", [])
                            if stats_res:
                                inst_state = stats_res[0].get("state")
                                if inst_state == "CRASHED":
                                    logger.error("Cloud Foundry app instance crashed.")
                                    status_code = 1
                                    break

                return CloudFoundryWorkerResult(identifier=app_guid, status_code=status_code)

            finally:
                # 8. Destroy the App Instance
                if configuration.auto_remove_app and app_guid:
                    logger.info(f"Destroying Cloud Foundry app {app_guid}...")
                    try:
                        del_resp = await client.delete(f"/v3/apps/{app_guid}", headers=headers)
                        if del_resp.status_code in (200, 202, 204):
                            logger.info(f"Cloud Foundry app {app_guid} destroyed.")
                    except Exception as exc:
                        logger.warning(f"Failed to destroy Cloud Foundry app {app_guid}: {exc}")

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: CloudFoundryJobConfiguration,
        grace_seconds: int = 30,
    ) -> None:
        """
        Destroys the running Cloud Foundry app instance.
        """
        if not configuration.credentials:
            raise ValueError(
                "CloudFoundryCredentials must be provided to terminate Cloud Foundry resources."
            )

        token = await configuration.credentials.get_auth_token()
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient(
            base_url=configuration.credentials.api_url, timeout=30.0
        ) as client:
            resp = await client.delete(f"/v3/apps/{infrastructure_pid}", headers=headers)
            if resp.status_code == 404:
                raise InfrastructureNotFound(
                    f"Cloud Foundry app {infrastructure_pid} not found."
                )
            if resp.status_code >= 400 and resp.status_code != 422:
                resp.raise_for_status()
