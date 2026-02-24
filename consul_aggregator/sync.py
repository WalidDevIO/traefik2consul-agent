"""
Synchronization engine.

Manages the KV sync, service registration, and background threads
for Consul monitoring and periodic Traefik resync.

Strategy:
  - On each sync: compute new KV entries, diff with previous set,
    DELETE stale keys, PUT new/changed keys.
  - Also register lightweight services (no tags) for health checks.
"""

import logging
import threading
import time
from typing import Dict, Optional, Set

from .config import Config
from .consul_client import ConsulClient
from .kv_builder import KVBuilder
from .traefik_client import TraefikClient

logger = logging.getLogger("consul_aggregator")


class SyncEngine:
    """Orchestrates the sync loop between Traefik and Consul KV."""

    def __init__(
        self,
        config: Config,
        consul: ConsulClient,
        traefik: TraefikClient,
        kv_builder: KVBuilder,
    ) -> None:
        self._config = config
        self._consul = consul
        self._traefik = traefik
        self._kv_builder = kv_builder

        # Track known KV keys for differential updates (thread-safe)
        self._known_keys: Set[str] = set()
        self._keys_lock = threading.Lock()

        # Cache last KV entries for retry on Consul recovery
        self._cached_entries: Optional[Dict[str, str]] = None
        self._cache_lock = threading.Lock()

    # ── Cache ─────────────────────────────────────────────────

    def _cache_set(self, entries: Dict[str, str]) -> None:
        with self._cache_lock:
            self._cached_entries = entries

    def _cache_get(self) -> Optional[Dict[str, str]]:
        with self._cache_lock:
            return self._cached_entries

    # ── KV sync (differential) ────────────────────────────────

    def _sync_kv(self, entries: Dict[str, str]) -> bool:
        """
        Write KV entries to Consul, deleting stale keys from previous sync.
        Returns True if all writes succeeded.
        """
        new_keys = set(entries.keys())

        with self._keys_lock:
            stale_keys = self._known_keys - new_keys

        # Delete stale keys
        for key in stale_keys:
            if not self._consul.kv_delete(key):
                logger.warning(f"failed to delete stale KV key: {key}")

        if stale_keys:
            logger.info(f"🗑️  deleted {len(stale_keys)} stale KV keys")

        # Put new/updated entries
        ok = True
        for key, value in entries.items():
            if not self._consul.kv_put(key, value):
                logger.warning(f"failed to write KV key: {key}")
                ok = False

        with self._keys_lock:
            self._known_keys = new_keys

        return ok

    # ── Snapshot push ─────────────────────────────────────────

    def _push_snapshot(self, entries: Dict[str, str]) -> None:
        """Cache entries and attempt to sync KV + register services."""
        self._cache_set(entries)

        if not self._consul.is_alive:
            logger.warning("📦 cached snapshot (Consul down)")
            return

        # Sync KV entries
        ok = self._sync_kv(entries)
        if ok:
            logger.info(f"✅ synced {len(entries)} KV entries")
        else:
            logger.warning("⚠️ some KV writes failed (cached for retry)")

        # Register lightweight services (health checks)
        for payload in self._kv_builder.build_service_payloads():
            svc_ok = self._consul.register_service(payload)
            if svc_ok:
                logger.info(
                    f"✅ service: {payload['Name']} "
                    f"@ {payload['Address']}:{payload['Port']}"
                )
            else:
                logger.warning(f"⚠️ service registration failed: {payload['Name']}")

    def _push_cached_if_any(self) -> None:
        entries = self._cache_get()
        if not entries:
            return
        logger.info("🔄 Consul back — pushing cached snapshot")
        self._push_snapshot(entries)

    # ── Build pipeline ────────────────────────────────────────

    def _build_entries(self, rawdata: dict) -> Dict[str, str]:
        """Fetch rawdata → KV entries pipeline."""
        return self._kv_builder.build_kv_entries(rawdata)

    # ── Background threads ────────────────────────────────────

    def _consul_monitor(self) -> None:
        """Periodically check Consul health; push cache when it comes back."""
        was_alive = True
        while True:
            time.sleep(10)
            alive = self._consul.health_check()
            if alive and not was_alive:
                self._push_cached_if_any()
            was_alive = alive

    def _periodic_resync(self) -> None:
        """Periodically fetch rawdata and push a fresh snapshot."""
        while True:
            time.sleep(self._config.resync_seconds)
            try:
                raw = self._traefik.fetch_rawdata()
                entries = self._build_entries(raw)
                self._push_snapshot(entries)
            except Exception as e:
                logger.warning(f"resync failed: {e}")

    # ── Public API ────────────────────────────────────────────

    def initial_sync(self) -> None:
        """Perform one immediate fetch + push on startup."""
        try:
            logger.info("🔍 Initial rawdata snapshot...")
            raw = self._traefik.fetch_rawdata()
            entries = self._build_entries(raw)
            self._push_snapshot(entries)
        except Exception as e:
            logger.warning(f"initial snapshot failed: {e}")

    def start(self) -> None:
        """Run the initial sync and spin up background threads (blocking)."""
        self._consul.health_check()
        self.initial_sync()

        threading.Thread(target=self._consul_monitor, daemon=True).start()
        threading.Thread(target=self._periodic_resync, daemon=True).start()

        logger.info("👂 Running (periodic snapshot mode)...")
        while True:
            time.sleep(3600)
