"""
Consul HTTP client.

Handles all communication with the Consul agent API:
health checks, service registration, and connection state tracking.
"""

import logging
import threading
from typing import Optional

import requests

from .config import Config

logger = logging.getLogger("consul_aggregator")


class ConsulClient:
    """Thread-safe Consul HTTP client with health tracking."""

    def __init__(self, config: Config) -> None:
        self._addr = config.consul_addr
        self._alive = True
        self._lock = threading.Lock()

    # ── Alive state (thread-safe) ─────────────────────────────

    @property
    def is_alive(self) -> bool:
        with self._lock:
            return self._alive

    def _set_alive(self, value: bool) -> None:
        with self._lock:
            self._alive = value

    # ── Health check ──────────────────────────────────────────

    def health_check(self) -> bool:
        """Ping Consul leader endpoint; update alive state."""
        try:
            r = requests.get(f"{self._addr}/v1/status/leader", timeout=5)
            alive = r.status_code == 200

            if alive and not self.is_alive:
                logger.info("Consul is back UP — will push full snapshot")
            if not alive and self.is_alive:
                logger.warning(
                    f"Consul health check returned {r.status_code} — cache mode"
                )

            self._set_alive(alive)
            return alive
        except Exception as e:
            if self.is_alive:
                logger.warning(f"Consul health check failed — cache mode: {e}")
            self._set_alive(False)
            return False

    # ── HTTP helpers ──────────────────────────────────────────

    def put(self, path: str, payload: Optional[dict] = None) -> bool:
        """Generic PUT to Consul."""
        try:
            kwargs: dict = {"timeout": 10}
            if payload is not None:
                kwargs["json"] = payload

            r = requests.put(f"{self._addr}{path}", **kwargs)

            if r.status_code in (200, 201, 204):
                if not self.is_alive:
                    logger.info("Consul is back UP")
                    self._set_alive(True)
                return True

            logger.warning(
                f"Consul {path} returned {r.status_code}: {r.text[:200]}"
            )
            return False
        except Exception as e:
            if self.is_alive:
                logger.warning(f"Consul unreachable: {e}")
                self._set_alive(False)
            return False

    def register_service(self, payload: dict) -> bool:
        """Register (or update) a service in the local Consul agent."""
        return self.put("/v1/agent/service/register", payload)
