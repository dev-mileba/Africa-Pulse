import requests
from datetime import datetime, timedelta
import os
from pathlib import Path
from dotenv import load_dotenv

# Resolve from the project root, not the current working directory.
# This file lives at dags/pipeline/functions/, so the root is 3 levels up.
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
CLIENT_ID = os.environ["OPENSKY_CLIENT_ID"]
CLIENT_SECRET = os.environ["OPENSKY_CLIENT_SECRET"]

# How many seconds before expiry to proactively refresh the token.
TOKEN_REFRESH_MARGIN = 30


class TokenManager:
    def __init__(self):
        self.token = None
        self.expires_at = None

    def get_token(self):
        """Return a valid access token, refreshing automatically if needed."""
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
        """Fetch a new access token from the OpenSky authentication server."""
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
        r.raise_for_status()

        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(
            seconds=expires_in - TOKEN_REFRESH_MARGIN
        )
        return self.token

    def headers(self):
        """Return request headers with a valid Bearer token."""
        return {"Authorization": f"Bearer {self.get_token()}"}


# Create a single shared instance for your script.
# tokens = TokenManager()

# Use it for any API call - the token is refreshed automatically.
# response = requests.get(
#     "https://opensky-network.org/api/states/all",
#     headers=tokens.headers(),
# )
# print(response.json())
