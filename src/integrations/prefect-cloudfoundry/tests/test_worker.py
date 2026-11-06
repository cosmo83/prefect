from uuid import uuid4
import pytest
import respx
import httpx
from unittest.mock import AsyncMock, MagicMock

from prefect.client.schemas.objects import FlowRun
from prefect.states import Completed
from prefect_cloudfoundry.worker import (
    CloudFoundryJobConfiguration,
    CloudFoundryWorker,
    CloudFoundryWorkerResult,
)


@pytest.fixture
def mock_flow_run():
    return FlowRun(
        id=uuid4(),
        flow_id=uuid4(),
        name="delicate-badger",
    )


def test_job_configuration_env_and_command(mock_flow_run):
    config = CloudFoundryJobConfiguration(
        image="docker.io/my-org/my-image:latest",
        env={"PREFECT__FLOW_RUN_ID": str(mock_flow_run.id), "CUSTOM_VAR": "hello"},
    )
    config.prepare_for_flow_run(mock_flow_run)

    assert config.command == "prefect flow-run execute"
    assert config.env["CUSTOM_VAR"] == "hello"
    assert config.env["PREFECT__FLOW_RUN_ID"] == str(mock_flow_run.id)


def test_base_job_template():
    template = CloudFoundryWorker.get_default_base_job_template()
    assert "job_configuration" in template
    assert "variables" in template
    props = template["variables"]["properties"]
    assert "image" in props
    assert "memory_in_mb" in props
    assert "disk_in_mb" in props
    assert "auto_remove_app" in props
    # Verify task configs have been removed
    assert "cf_app_guid" not in props
    assert "cf_app_name" not in props
    assert "execution_mode" not in props


@respx.mock
async def test_worker_run_docker_app_lifecycle(cf_credentials, mock_flow_run):
    config = CloudFoundryJobConfiguration(
        credentials=cf_credentials,
        image="docker.io/my-org/my-image:latest",
        memory_in_mb=2048,
        disk_in_mb=1024,
        poll_interval_seconds=0,
    )
    config.prepare_for_flow_run(mock_flow_run)

    app_guid = "app-guid-docker-123"
    pkg_guid = "pkg-guid-456"
    drop_guid = "drop-guid-789"
    proc_guid = "proc-guid-web"

    # Mock Space Lookup
    respx.get("https://api.sys.example.com/v3/organizations?names=my-org").mock(
        return_value=httpx.Response(200, json={"resources": [{"guid": "org-1"}]})
    )
    respx.get("https://api.sys.example.com/v3/spaces?names=my-space&organization_guids=org-1").mock(
        return_value=httpx.Response(200, json={"resources": [{"guid": "space-1"}]})
    )

    # Mock App Creation
    respx.post("https://api.sys.example.com/v3/apps").mock(
        return_value=httpx.Response(201, json={"guid": app_guid})
    )

    # Mock Environment Variables patch
    respx.patch(f"https://api.sys.example.com/v3/apps/{app_guid}/environment_variables").mock(
        return_value=httpx.Response(200, json={"var": {}})
    )

    # Mock Package Creation
    respx.post("https://api.sys.example.com/v3/packages").mock(
        return_value=httpx.Response(201, json={"guid": pkg_guid})
    )

    # Mock Droplet Staging
    respx.post(f"https://api.sys.example.com/v3/packages/{pkg_guid}/droplets").mock(
        return_value=httpx.Response(201, json={"guid": drop_guid})
    )
    respx.get(f"https://api.sys.example.com/v3/droplets/{drop_guid}").mock(
        return_value=httpx.Response(200, json={"guid": drop_guid, "state": "STAGED"})
    )

    # Mock Current Droplet patch
    respx.patch(f"https://api.sys.example.com/v3/apps/{app_guid}/relationships/current_droplet").mock(
        return_value=httpx.Response(200, json={})
    )

    # Mock Process get & patch
    respx.get(f"https://api.sys.example.com/v3/apps/{app_guid}/processes").mock(
        return_value=httpx.Response(200, json={"resources": [{"type": "web", "guid": proc_guid}]})
    )
    respx.patch(f"https://api.sys.example.com/v3/processes/{proc_guid}").mock(
        return_value=httpx.Response(200, json={})
    )

    # Mock App Start
    respx.post(f"https://api.sys.example.com/v3/apps/{app_guid}/actions/start").mock(
        return_value=httpx.Response(200, json={})
    )

    # Mock App Deletion
    del_route = respx.delete(f"https://api.sys.example.com/v3/apps/{app_guid}").mock(
        return_value=httpx.Response(202, json={})
    )

    worker = CloudFoundryWorker(work_pool_name="test-pool")
    completed_run = mock_flow_run.model_copy()
    completed_run.state = Completed()
    worker._client = MagicMock()
    worker._client.read_flow_run = AsyncMock(return_value=completed_run)

    result = await worker.run(flow_run=mock_flow_run, configuration=config)

    assert isinstance(result, CloudFoundryWorkerResult)
    assert result.identifier == app_guid
    assert result.status_code == 0
    # Verify app was deleted on completion
    assert del_route.called


@respx.mock
async def test_worker_kill_infrastructure(cf_credentials):
    config = CloudFoundryJobConfiguration(
        credentials=cf_credentials,
        image="docker.io/my-org/my-image:latest",
    )
    app_guid = "app-guid-to-kill"

    del_route = respx.delete(f"https://api.sys.example.com/v3/apps/{app_guid}").mock(
        return_value=httpx.Response(202, json={})
    )

    worker = CloudFoundryWorker(work_pool_name="test-pool")
    await worker.kill_infrastructure(infrastructure_pid=app_guid, configuration=config)
    assert del_route.called
