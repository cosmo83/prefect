"""Module containing Cloud Foundry credentials block."""
from __future__ import annotations

import time
from typing import Optional
import httpx
from pydantic import Field, PrivateAttr, SecretStr

from prefect.blocks.core import Block


class CloudFoundryCredentials(Block):
    """
    Block used to manage credentials and target space for interacting with Cloud Foundry.

    Examples:
        Load credentials and obtain an authenticated HTTP client:
        ```python
        from prefect_cloudfoundry import CloudFoundryCredentials

        credentials = CloudFoundryCredentials.load("my-cf-credentials")
        async with credentials.get_client() as client:
            resp = await client.get("/v3/apps")
        ```
    """

    _block_type_name = "Cloud Foundry Credentials"
    _logo_url = "https://raw.githubusercontent.com/cloudfoundry/cli/main/assets/cloudfoundry.png"
    _description = "Store credentials and organization/space for authenticating with Cloud Foundry APIs."

    api_url: str = Field(
        default=...,
        description="The Cloud Foundry Cloud Controller API URL (e.g., https://api.sys.example.com).",
    )
    username: str = Field(
        default=...,
        description="Cloud Foundry username for authentication.",
    )
    password: SecretStr = Field(
        default=...,
        description="Cloud Foundry password for authentication.",
    )
    org_name: str = Field(
        default=...,
        description="Target Cloud Foundry Organization name.",
    )
    space_name: str = Field(
        default=...,
        description="Target Cloud Foundry Space name within the organization.",
    )

    uaa_url: Optional[str] = Field(
        default=None,
        description="Optional Cloud Foundry UAA login URL (e.g., https://login.sys.example.com). Auto-discovered if omitted.",
    )
    client_id: Optional[str] = Field(
        default="cf",
        description="UAA OAuth2 client ID. Defaults to standard 'cf' CLI client.",
    )
    client_secret: Optional[SecretStr] = Field(
        default=None,
        description="UAA OAuth2 client secret if required by the target environment.",
    )
    api_token: Optional[SecretStr] = Field(
        default=None,
        description="Optional pre-existing OAuth2 bearer token. If provided, overrides username/password authentication.",
    )

    _cached_token: Optional[str] = PrivateAttr(default=None)
    _token_expiry: float = PrivateAttr(default=0.0)

    async def get_auth_token(self) -> str:
        """
        Retrieves an active OAuth2 Bearer token, authenticating against Cloud Foundry UAA
        using username and password (with in-memory token caching).
        """
        now = time.time()
        if self._cached_token and now < (self._token_expiry - 60):
            return self._cached_token

        if self.api_token:
            token = self.api_token.get_secret_value()
            if not token.lower().startswith("bearer "):
                return f"Bearer {token}"
            return token

        # Determine UAA endpoint if not explicitly provided
        uaa_endpoint = self.uaa_url
        if not uaa_endpoint:
            async with httpx.AsyncClient(base_url=self.api_url) as client:
                resp = await client.get("/v3-info")
                if resp.status_code == 200:
                    info = resp.json()
                    uaa_endpoint = info.get("links", {}).get("login", {}).get("href")
            if not uaa_endpoint:
                raise ValueError(
                    "Could not discover UAA endpoint from Cloud Foundry API. Please specify `uaa_url`."
                )

        token_url = f"{uaa_endpoint.rstrip('/')}/oauth/token"
        client_id = self.client_id or "cf"
        client_secret = self.client_secret.get_secret_value() if self.client_secret else ""

        data = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password.get_secret_value(),
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(token_url, data=data, auth=(client_id, client_secret))
            resp.raise_for_status()
            token_data = resp.json()

            token_type = token_data.get("token_type", "Bearer")
            access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)

            bearer_token = f"{token_type} {access_token}"
            self._cached_token = bearer_token
            self._token_expiry = now + float(expires_in)
            return bearer_token

    async def get_client(self) -> httpx.AsyncClient:
        """
        Returns an httpx.AsyncClient pre-configured with the base API URL and Authorization header.
        """
        token = await self.get_auth_token()
        return httpx.AsyncClient(
            base_url=self.api_url,
            headers={"Authorization": token, "Content-Type": "application/json"},
            timeout=30.0,
        )

    async def get_org_guid(self) -> str:
        """
        Resolves the GUID of the configured organization name.
        """
        async with await self.get_client() as client:
            resp = await client.get(f"/v3/organizations?names={self.org_name}")
            resp.raise_for_status()
            resources = resp.json().get("resources", [])
            if not resources:
                raise ValueError(f"Organization '{self.org_name}' not found on Cloud Foundry.")
            return resources[0]["guid"]

    async def get_space_guid(self) -> str:
        """
        Resolves the GUID of the configured space name within the organization.
        """
        org_guid = await self.get_org_guid()
        async with await self.get_client() as client:
            resp = await client.get(
                f"/v3/spaces?names={self.space_name}&organization_guids={org_guid}"
            )
            resp.raise_for_status()
            resources = resp.json().get("resources", [])
            if not resources:
                raise ValueError(
                    f"Space '{self.space_name}' not found in organization '{self.org_name}'."
                )
            return resources[0]["guid"]

    async def get_app_guid(self, app_name: str) -> str:
        """
        Resolves the GUID of an application by name within the target space.
        """
        space_guid = await self.get_space_guid()
        async with await self.get_client() as client:
            resp = await client.get(f"/v3/apps?names={app_name}&space_guids={space_guid}")
            resp.raise_for_status()
            resources = resp.json().get("resources", [])
            if not resources:
                raise ValueError(
                    f"Application '{app_name}' not found in space '{self.space_name}'."
                )
            return resources[0]["guid"]
