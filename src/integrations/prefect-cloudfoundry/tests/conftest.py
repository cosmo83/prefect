import pytest
from pydantic import SecretStr
from prefect_cloudfoundry.credentials import CloudFoundryCredentials


@pytest.fixture
def cf_credentials():
    return CloudFoundryCredentials(
        api_url="https://api.sys.example.com",
        username="cf-admin",
        password=SecretStr("supersecret"),
        org_name="my-org",
        space_name="my-space",
        api_token="test-token-12345",
    )
