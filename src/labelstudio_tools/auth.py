"""Authentication and connectivity helpers shared by library and CLI code."""

from __future__ import annotations

import os
import warnings
from urllib.parse import urljoin

import requests


def auth_header(token: str) -> dict:
    """Return the Label Studio Authorization header for legacy or modern tokens."""
    is_legacy_token = len(token) <= 40
    auth_type = "Token" if is_legacy_token else "Bearer"
    return {"Authorization": f"{auth_type} {token}"}


def json_headers(token: str) -> dict:
    return {"Content-Type": "application/json", **auth_header(token)}


def read_token(token_path: str) -> str:
    """Deprecated token-file helper.

    Tokens should now be supplied directly in the project config or auth file.
    This helper is kept for compatibility with older callers.
    """
    warnings.warn(
        "read_token() is deprecated; put tokens in the auth file or project config.",
        DeprecationWarning,
        stacklevel=2,
    )
    if os.path.isfile(token_path):
        with open(token_path) as f:
            return f.read().strip()
    return token_path


def validate_ls_token(host: str, token: str, timeout: int = 10) -> tuple[bool, str]:
    try:
        r = requests.get(
            urljoin(host, "/api/version"),
            headers=auth_header(token),
            timeout=timeout,
            allow_redirects=True,
        )
        return r.status_code == 200, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)


def validate_storage(s: dict) -> tuple[bool, str]:
    if not s.get("aws_access_key_id") or not s.get("aws_secret_access_key"):
        return False, "credentials not available (deferred)"
    try:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=s.get("endpoint_url"),
            aws_access_key_id=s["aws_access_key_id"],
            aws_secret_access_key=s["aws_secret_access_key"],
        )
        client.head_bucket(Bucket=s["bucket"])
        return True, "head_bucket OK"
    except Exception as e:
        return False, str(e).splitlines()[0]


def validate_ml_backend(url: str, user: str | None = None,
                        password: str | None = None,
                        timeout: int = 10) -> tuple[bool, str]:
    auth = (user, password) if user else None
    try:
        r = requests.get(url, auth=auth, timeout=timeout, allow_redirects=True)
        return r.status_code == 200, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)
