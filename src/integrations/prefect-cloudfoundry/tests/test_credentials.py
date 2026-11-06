import pytest
import respx
import httpx
from pydantic import SecretStr
from prefect_cloudfoundry.credentials import CloudFoundryCredentials


@respx.mock
async def test_get_auth_token_with_username_and_password():
    creds = CloudFoundryCredentials(
        api_url="https://api.sys.example.com",
        username="cf-user",
        password=SecretStr("cf-pass"),
        org_name="my-org",
        space_name="my-space",
        uaa_url="https://login.sys.example.com",
    )

    respx.post("https://login.sys.example.com/oauth/token").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "generated-token-xyz",
                "token_type": "bearer",
                "expires_in": 3600,
            },
        )
    )

    token = await creds.get_auth_token()
    assert token.lower() == "bearer generated-token-xyz"


@respx.mock
async def test_guid_lookups():
    creds = CloudFoundryCredentials(
        api_url="https://api.sys.example.com",
        username="cf-user",
        password=SecretStr("cf-pass"),
        org_name="finance-org",
        space_name="prod-space",
        api_token=SecretStr("test-token"),
    )

    # Mock org lookup
    respx.get("https://api.sys.example.com/v3/organizations?names=finance-org").mock(
        return_value=httpx.Response(
            200,
            json={"resources": [{"guid": "org-1234"}]},
        )
    )

    # Mock space lookup
    respx.get("https://api.sys.example.com/v3/spaces?names=prod-space&organization_guids=org-1234").mock(
        return_value=httpx.Response(
            200,
            json={"resources": [{"guid": "space-5678"}]},
        )
    )

    # Mock app lookup
    respx.get("https://api.sys.example.com/v3/apps?names=my-app&space_guids=space-5678").mock(
        return_value=httpx.Response(
            200,
            json={"resources": [{"guid": "app-9999"}]},
        )
    )

    org_guid = await creds.get_org_guid()
    assert org_guid == "org-1234"

    space_guid = await creds.get_space_guid()
    assert space_guid == "space-5678"

    app_guid = await creds.get_app_guid("my-app")
    assert app_guid == "app-9999"
