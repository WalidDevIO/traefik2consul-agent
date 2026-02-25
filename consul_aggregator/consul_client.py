"""
Consul HTTP client.

Handles all communication with the Consul agent API:
health checks, service registration, KV store, and connection state tracking.
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
        logger.debug(f"ConsulClient initialized with addr={self._addr}")

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
        logger.debug(f"health_check: pinging {self._addr}/v1/status/leader")
        try:
            r = requests.get(f"{self._addr}/v1/status/leader", timeout=5)
            alive = r.status_code == 200
            logger.debug(f"health_check: status_code={r.status_code}, alive={alive}")

            if alive and not self.is_alive:
                logger.info("Consul is back UP — will push full snapshot")
            if not alive and self.is_alive:
                logger.warning(
                    f"Consul health check returned {r.status_code} — cache mode"
                )

            self._set_alive(alive)
            return alive
        except Exception as e:
            logger.debug(f"health_check: exception occurred: {e}")
            if self.is_alive:
                logger.warning(f"Consul health check failed — cache mode: {e}")
            self._set_alive(False)
            return False

    # ── Generic HTTP helpers ──────────────────────────────────

    def put(self, path: str, payload: Optional[dict] = None) -> bool:
        """Generic PUT to Consul (JSON body)."""
        logger.debug(f"put: path={path}, payload={payload}")
        try:
            kwargs: dict = {"timeout": 10}
            if payload is not None:
                kwargs["json"] = payload

            r = requests.put(f"{self._addr}{path}", **kwargs)
            logger.debug(f"put: {path} returned status_code={r.status_code}")

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
            logger.debug(f"put: exception on {path}: {e}")
            if self.is_alive:
                logger.warning(f"Consul unreachable: {e}")
                self._set_alive(False)
            return False

    # ── Service registration ──────────────────────────────────

    def register_service(self, payload: dict) -> bool:
        """Register (or update) a service in the local Consul agent."""
        logger.debug(
            f"register_service: ID={payload.get('ID')}, Name={payload.get('Name')}, "
            f"Address={payload.get('Address')}:{payload.get('Port')}"
        )
        return self.put("/v1/agent/service/register", payload)

    # ── KV store ──────────────────────────────────────────────

    def kv_put(self, key: str, value: str) -> bool:
        """Write a single key/value pair to the Consul KV store."""
        logger.debug(f"kv_put: key={key}, value={value[:120]}")
        try:
            r = requests.put(
                f"{self._addr}/v1/kv/{key}",
                data=value.encode("utf-8"),
                timeout=10,
            )
            logger.debug(f"kv_put: {key} returned status_code={r.status_code}")
            if r.status_code == 200:
                if not self.is_alive:
                    self._set_alive(True)
                return True
            logger.warning(f"Consul KV PUT {key} returned {r.status_code}")
            return False
        except Exception as e:
            logger.debug(f"kv_put: exception for {key}: {e}")
            if self.is_alive:
                logger.warning(f"Consul KV unreachable: {e}")
                self._set_alive(False)
            return False

    def kv_delete(self, key: str) -> bool:
        """Delete a single key from the Consul KV store."""
        logger.debug(f"kv_delete: key={key}")
        try:
            r = requests.delete(f"{self._addr}/v1/kv/{key}", timeout=10)
            logger.debug(f"kv_delete: {key} returned status_code={r.status_code}")
            return r.status_code == 200
        except Exception as e:
            logger.debug(f"kv_delete: exception for {key}: {e}")
            return False

    def kv_delete_tree(self, prefix: str) -> bool:
        """Delete all keys under a prefix (recurse)."""
        logger.debug(f"kv_delete_tree: prefix={prefix}")
        try:
            r = requests.delete(
                f"{self._addr}/v1/kv/{prefix}",
                params={"recurse": "true"},
                timeout=10,
            )
            logger.debug(f"kv_delete_tree: {prefix} returned status_code={r.status_code}")
            return r.status_code == 200
        except Exception as e:
            logger.debug(f"kv_delete_tree: exception for {prefix}: {e}")
            return False

    def kv_acquire(self, key: str, value: str, session_id: str) -> bool:
        """Write a KV pair bound to a session (auto-deleted on session expiry)."""
        logger.debug(f"kv_acquire: key={key}, session={session_id}, value={value[:120]}")
        try:
            r = requests.put(
                f"{self._addr}/v1/kv/{key}",
                params={"acquire": session_id},
                data=value.encode("utf-8"),
                timeout=10,
            )
            logger.debug(f"kv_acquire: {key} returned status_code={r.status_code}, body={r.text.strip()}")
            if r.status_code == 200:
                # Consul returns "true" or "false" in body
                ok = r.text.strip().lower() == "true"
                if not ok:
                    logger.debug(
                        f"⚠️ failed to acquire KV key: {key}, value: {value}, session: {session_id}"
                    )
                return ok
            logger.warning(f"Consul KV acquire {key} returned {r.status_code}")
            return False
        except Exception as e:
            logger.debug(f"kv_acquire: exception for {key}: {e}")
            if self.is_alive:
                logger.warning(f"Consul KV unreachable: {e}")
                self._set_alive(False)
            return False

    # ── Sessions ──────────────────────────────────────────────

    def session_create(self, name: str, ttl: str = "30s") -> Optional[str]:
        """
        Create a Consul session with Behavior=delete.
        When the session expires (agent dies), all acquired KV keys are deleted.
        Returns the session ID, or None on failure.
        """
        logger.debug(f"session_create: name={name}, ttl={ttl}")
        try:
            r = requests.put(
                f"{self._addr}/v1/session/create",
                json={
                    "Name": name,
                    "TTL": ttl,
                    "Behavior": "delete",
                },
                timeout=10,
            )
            logger.debug(f"session_create: status_code={r.status_code}")
            if r.status_code == 200:
                sid = r.json().get("ID")
                logger.info(f"🔑 session created: {sid} (TTL={ttl})")
                return sid
            logger.warning(f"session create returned {r.status_code}: {r.text[:200]}")
            return None
        except Exception as e:
            logger.debug(f"session_create: exception: {e}")
            logger.warning(f"session create failed: {e}")
            return None

    def session_renew(self, session_id: str) -> bool:
        """Renew a session to prevent it from expiring."""
        logger.debug(f"session_renew: session_id={session_id}")
        try:
            r = requests.put(
                f"{self._addr}/v1/session/renew/{session_id}",
                timeout=10,
            )
            logger.debug(f"session_renew: status_code={r.status_code}")
            return r.status_code == 200
        except Exception as e:
            logger.debug(f"session_renew: exception for {session_id}: {e}")
            return False

    def session_destroy(self, session_id: str) -> bool:
        """Destroy a session (triggers Behavior=delete on all acquired keys)."""
        logger.debug(f"session_destroy: session_id={session_id}")
        try:
            r = requests.put(
                f"{self._addr}/v1/session/destroy/{session_id}",
                timeout=10,
            )
            logger.debug(f"session_destroy: status_code={r.status_code}")
            if r.status_code == 200:
                logger.info(f"🔑 session destroyed: {session_id}")
            return r.status_code == 200
        except Exception as e:
            logger.debug(f"session_destroy: exception for {session_id}: {e}")
            return False
