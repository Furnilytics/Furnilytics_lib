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

        # Parse JSON once (if possible). Many error responses are JSON too.
        parsed: Any = None
        is_json = False
        try:
            parsed = r.json()
            is_json = True
        except Exception:
            parsed = None
            is_json = False

        # Store helpful response meta for debugging / caching
        self._last_meta = {
            "method": "GET",
            "url": url,
            "params": params or {},
            "status": r.status_code,
            "ETag": r.headers.get("ETag"),
            "Cache-Control": r.headers.get("Cache-Control"),
            "Retry-After": r.headers.get("Retry-After"),
            "X-RateLimit-Reset": r.headers.get("X-RateLimit-Reset"),
        }

        def _detail_fallback(default: str) -> str:
            """
            Normalize API error detail into a friendly string.
            Accepts:
            - {"detail": "..."}
            - {"detail": {"msg": "..."}}
            - "..."
            """
            if is_json:
                if isinstance(parsed, dict):
                    d = parsed.get("detail")
                    if isinstance(d, str) and d.strip():
                        return d
                    if isinstance(d, dict):
                        # common FastAPI patterns
                        if isinstance(d.get("msg"), str) and d["msg"].strip():
                            return d["msg"]
                        # last resort: compact dict
                        return str(d)
                    # sometimes APIs just return {"message": "..."}
                    m = parsed.get("message")
                    if isinstance(m, str) and m.strip():
                        return m
                elif isinstance(parsed, str) and parsed.strip():
                    return parsed
            return default

        # Auth / access control
        if r.status_code == 401:
            raise AuthError(_detail_fallback("Invalid or missing API key."))
        if r.status_code == 403:
            raise AuthError(_detail_fallback("Forbidden."))

        # Not found
        if r.status_code == 404:
            raise NotFoundError(_detail_fallback("Resource not found."))

        # Rate limit
        if r.status_code == 429:
            reset = r.headers.get("X-RateLimit-Reset") or r.headers.get("Retry-After")
            raise RateLimitError(_detail_fallback("Rate limit exceeded."), reset_at=reset)

        # Other 4xx
        if 400 <= r.status_code < 500:
            raise ClientError(_detail_fallback(f"Client error ({r.status_code})."))

        # 5xx
        if 500 <= r.status_code < 600:
            raise ClientError(_detail_fallback(f"Server error ({r.status_code})."))

        # Success but not JSON (unexpected for this client)
        if not is_json:
            snippet = (r.text or "")[:200].strip()
            if snippet:
                raise ClientError(f"Invalid JSON response (HTTP {r.status_code}): {snippet}")
            raise ClientError(f"Invalid JSON response (HTTP {r.status_code}).")

        return parsed