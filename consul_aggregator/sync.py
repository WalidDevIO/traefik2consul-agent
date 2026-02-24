"""
Synchronization engine.

Manages the sync loop between Traefik and Consul.
Supports two modes:
  - "kv"   : writes config to Consul KV store (differential updates)
  - "tags" : registers services with Traefik tags in Consul catalog
"""

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Set

from .config import Config
from .consul_client import ConsulClient
from .traefik_client import TraefikClient

logger = logging.getLogger("consul_aggregator")


class SyncEngine:
    """Orchestrates the sync loop between Traefik and Consul."""

    def __init__(
        self,
        config: Config,
        consul: ConsulClient,
        traefik: TraefikClient,
        builder: Any,  # KVBuilder or TagBuilder
    ) -> None:
        self._config = config
        self._consul = consul
        self._traefik = traefik
        self._builder = builder
        self._mode = config.mode

        # KV mode: track known keys for differential updates
        self._known_keys: Set[str] = set()
        self._keys_lock = threading.Lock()

        # Cache for retry on Consul recovery
        self._cache: Optional[Any] = None
        self._cache_lock = threading.Lock()

    # ── Cache ─────────────────────────────────────────────────

    def _cache_set(self, data: Any) -> None:
        with self._cache_lock:
            self._cache = data

    def _cache_get(self) -> Optional[Any]:
        with self._cache_lock:
            return self._cache

    # ── KV mode sync ──────────────────────────────────────────

    def _sync_kv(self, entries: Dict[str, str]) -> bool:
        """Write KV entries, deleting stale keys from previous sync."""
        new_keys = set(entries.keys())

        with self._keys_lock:
            stale_keys = self._known_keys - new_keys

        for key in stale_keys:
            self._consul.kv_delete(key)

        if stale_keys:
            logger.info(f"🗑️  deleted {len(stale_keys)} stale KV keys")

        ok = True
        for key, value in entries.items():
            if not self._consul.kv_put(key, value):
                ok = False

        with self._keys_lock:
            self._known_keys = new_keys

        return ok

    def _push_kv(self, entries: Dict[str, str]) -> None:
        """Cache KV entries and attempt sync."""
        self._cache_set(entries)

        if not self._consul.is_alive:
            logger.warning("📦 cached snapshot (Consul down)")
            return

        ok = self._sync_kv(entries)
        if ok:
            logger.info(f"✅ synced {len(entries)} KV entries")
        else:
            logger.warning("⚠️ some KV writes failed (cached for retry)")

        # Register lightweight services (health checks only)
        for payload in self._builder.build_service_payloads():
            svc_ok = self._consul.register_service(payload)
            if svc_ok:
                logger.info(
                    f"✅ service: {payload['Name']} "
                    f"@ {payload['Address']}:{payload['Port']}"
                )
            else:
                logger.warning(f"⚠️ service registration failed: {payload['Name']}")

    # ── Tags mode sync ────────────────────────────────────────

    def _push_tags(self, payloads: List[dict]) -> None:
        """Cache payloads and attempt service registration."""
        self._cache_set(payloads)

        if not self._consul.is_alive:
            logger.warning("📦 cached snapshot (Consul down)")
            return

        for payload in payloads:
            ok = self._consul.register_service(payload)
            if ok:
                logger.info(
                    f"✅ registered: {payload['Name']} "
                    f"@ {payload['Address']}:{payload['Port']} "
                    f"(tags={len(payload['Tags'])})"
                )
            else:
                logger.warning(
                    f"⚠️ register failed for {payload['Name']} (cached for retry)"
                )

    # ── Mode-agnostic push ────────────────────────────────────

    def _push(self, data: Any) -> None:
        if self._mode == "kv":
            self._push_kv(data)
        else:
            self._push_tags(data)

    def _build(self, rawdata: dict) -> Any:
        if self._mode == "kv":
            return self._builder.build_kv_entries(rawdata)
        else:
            tags_by_proto = self._builder.build_tags(rawdata)
            return self._builder.build_consul_payloads(tags_by_proto)

    def _push_cached_if_any(self) -> None:
        data = self._cache_get()
        if not data:
            return
        logger.info("🔄 Consul back — pushing cached snapshot")
        self._push(data)

    # ── Background threads ────────────────────────────────────

    def _consul_monitor(self) -> None:
        was_alive = True
        while True:
            time.sleep(10)
            alive = self._consul.health_check()
            if alive and not was_alive:
                self._push_cached_if_any()
            was_alive = alive

    def _periodic_resync(self) -> None:
        while True:
            time.sleep(self._config.resync_seconds)
            try:
                raw = self._traefik.fetch_rawdata()
                data = self._build(raw)
                self._push(data)
            except Exception as e:
                logger.warning(f"resync failed: {e}")

    # ── Public API ────────────────────────────────────────────

    def initial_sync(self) -> None:
        try:
            logger.info("🔍 Initial rawdata snapshot...")
            raw = self._traefik.fetch_rawdata()
            data = self._build(raw)
            self._push(data)
        except Exception as e:
            logger.warning(f"initial snapshot failed: {e}")

    def start(self) -> None:
        self._consul.health_check()
        self.initial_sync()

        threading.Thread(target=self._consul_monitor, daemon=True).start()
        threading.Thread(target=self._periodic_resync, daemon=True).start()

        logger.info("👂 Running (periodic snapshot mode)...")
        while True:
            time.sleep(3600)
