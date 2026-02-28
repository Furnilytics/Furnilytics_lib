from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Union
import os
import json

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd

BASE_URL = "https://furnilytics-api.fly.dev"


class ClientError(Exception): ...
class AuthError(ClientError): ...
class NotFoundError(ClientError): ...
class RateLimitError(ClientError):
    def __init__(self, message: str, reset_at: Optional[int] = None):
        super().__init__(message)
        self.reset_at = reset_at


def _env(k: str) -> Optional[str]:
    return os.getenv(k)


@dataclass
class Client:
    """
    Furnilytics API client (API v2.x)

    Endpoints used:
      - GET /health
      - GET /datasets            -> returns { ..., "data": [rows...] }
      - GET /metadata            -> returns { ..., "data": [items...] }
      - GET /metadata/{id}       -> returns { "id": ..., "meta": ..., "schema": ... }
      - GET /data/{id}           -> returns [data rows...]
    """
    api_key: str | None = None
    base_url: str = BASE_URL
    timeout: int = 20
    user_agent: str = "furnilytics-python/0.2.0"

    _last_meta: Dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self):
        # API key is OPTIONAL (public endpoints work without it)
        if self.api_key is None:
            self.api_key = _env("FURNILYTICS_API_KEY")

        self.session = requests.Session()

        retries = Retry(
            total=4,
            backoff_factor=0.6,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        })

        # IMPORTANT: new API expects header name "X-API-Key"
        if self.api_key:
            self.session.headers["X-API-Key"] = self.api_key

    # -------------------------
    # Basics
    # -------------------------
    def health(self) -> Dict[str, Any]:
        return self._get_json("/health")

    @property
    def last_response_meta(self) -> Dict[str, Any]:
        return dict(self._last_meta)

    # -------------------------
    # Catalog / metadata
    # -------------------------
    def datasets(self) -> pd.DataFrame:
        """
        Returns a DataFrame of the dataset catalog (/datasets).
        API response shape: { ..., "data": [ {id, visibility, topic, subtopic, ...}, ... ] }
        """
        payload = self._get_json("/datasets")
        return pd.DataFrame(payload.get("data", []))

    def metadata(self) -> pd.DataFrame:
        """
        Returns DataFrame of metadata items (/metadata).
        API response shape: { ..., "data": [ {id, visibility, topic, subtopic, title, ...}, ... ] }
        """
        payload = self._get_json("/metadata")
        return pd.DataFrame(payload.get("data", []))

    def metadata_one(self, dataset_id: str) -> Dict[str, Any]:
        """
        Returns the metadata object for one dataset id (/metadata/{id}).
        """
        safe_id = dataset_id.strip("/")
        return self._get_json(f"/metadata/{safe_id}")

    # -------------------------
    # Data
    # -------------------------
    def data(
        self,
        dataset_id: str,
        *,
        frm: Optional[str] = None,
        to: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Returns data-only rows as a DataFrame from /data/{id}.
        Optional server-side filters:
          - frm (YYYY-MM-DD)
          - to  (YYYY-MM-DD)
          - limit (<= 20000)
        """
        safe_id = dataset_id.strip("/")
        params: Dict[str, Any] = {}
        if frm is not None:
            params["frm"] = frm
        if to is not None:
            params["to"] = to
        if limit is not None:
            params["limit"] = int(limit)

        rows = self._get_json(f"/data/{safe_id}", params=params)

        # /data/{id} returns a LIST, not {data: ...}
        if isinstance(rows, list):
            return pd.DataFrame(rows)

        # Defensive fallback if server changes shape
        if isinstance(rows, dict) and "data" in rows:
            return pd.DataFrame(rows["data"])

        raise ClientError("Unexpected response shape from /data/{id}")

    # -------------------------
    # Internal HTTP helper
    # -------------------------
    def _get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Union[Dict[str, Any], List[Any]]:
        url = self.base_url.rstrip("/") + path
        r = self.session.get(url, params=params, timeout=self.timeout)

        self._last_meta = {
            "ETag": r.headers.get("ETag"),
            "Cache-Control": r.headers.get("Cache-Control"),
            "Status": r.status_code,
        }

        if r.status_code == 401:
            raise AuthError("Invalid or missing API key.")
        if r.status_code == 403:
            # Pro-required case in your API returns a structured JSON detail
            try:
                detail = r.json()
            except Exception:
                detail = {"detail": "Forbidden"}
            raise AuthError(detail.get("detail") if isinstance(detail, dict) else "Forbidden")
        if r.status_code == 404:
            try:
                msg = r.json().get("detail", "Resource not found")
            except Exception:
                msg = "Resource not found"
            raise NotFoundError(msg)
        if r.status_code == 429:
            raise RateLimitError("Rate limit exceeded", reset_at=r.headers.get("X-RateLimit-Reset"))
        if 400 <= r.status_code < 500:
            try:
                detail = r.json().get("detail")
            except Exception:
                detail = None
            raise ClientError(detail or f"Client error ({r.status_code})")
        if 500 <= r.status_code < 600:
            raise ClientError(f"Server error ({r.status_code})")

        try:
            return r.json()
        except Exception as exc:
            raise ClientError(f"Invalid JSON response: {exc}") from exc