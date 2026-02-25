"""
Traefik rawdata client.

Fetches the effective configuration from a Traefik instance via /api/rawdata.
"""

import logging

import requests

from .config import Config

logger = logging.getLogger("consul_aggregator")


class TraefikClient:
    """Fetches Traefik rawdata (effective runtime configuration)."""

    def __init__(self, config: Config) -> None:
        self._base_url = config.traefik_url
        self._host_header = config.traefik_host
        logger.debug(f"TraefikClient initialized: base_url={self._base_url}, host_header={self._host_header or '(none)'}")

    # ── Internal ──────────────────────────────────────────────

    @staticmethod
    def _rawdata_url(base: str) -> str:
        if base.endswith("/api/rawdata"):
            return base
        return f"{base}/api/rawdata"

    # ── Public API ────────────────────────────────────────────

    def fetch_rawdata(self) -> dict:
        """GET /api/rawdata from the configured Traefik instance."""
        headers: dict = {}
        if self._host_header:
            headers["Host"] = self._host_header

        url = self._rawdata_url(self._base_url)
        logger.debug(f"fetch_rawdata: GET {url} (headers={headers})")
        try:
            r = requests.get(url, headers=headers, timeout=10)
            logger.debug(f"fetch_rawdata: status_code={r.status_code}, content_length={len(r.content)}")
            r.raise_for_status()
            data = r.json()
            logger.debug(f"fetch_rawdata: parsed JSON with {len(data)} top-level keys")
            return data
        except Exception as e:
            logger.debug(f"fetch_rawdata: exception: {e}")
            raise RuntimeError(
                f"Unable to fetch Traefik rawdata from {self._base_url} "
                f"(tried /api/rawdata): {e}"
            ) from e
