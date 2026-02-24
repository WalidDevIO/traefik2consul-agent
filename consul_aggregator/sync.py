"""
Synchronization engine.

Manages the cache, snapshot push logic, and background threads
for Consul monitoring and periodic Traefik resync.
"""

import logging
import threading
import time
from typing import Optional

from .config import Config
from .consul_client import ConsulClient
from .tag_builder import TagBuilder
from .traefik_client import TraefikClient

logger = logging.getLogger("consul_aggregator")


class SyncEngine:
    """Orchestrates the sync loop between Traefik and Consul."""

    def __init__(
        self,
        config: Config,
        consul: ConsulClient,
        traefik: TraefikClient,
        tag_builder: TagBuilder,
    ) -> None:
        self._config = config
        self._consul = consul
        self._traefik = traefik
        self._tag_builder = tag_builder

        # Cached payload (thread-safe)
        self._last_payload: Optional[dict] = None
        self._payload_lock = threading.Lock()

    # ── Cache ─────────────────────────────────────────────────

    def _cache_set(self, payload: dict) -> None:
        with self._payload_lock:
            self._last_payload = payload

    def _cache_get(self) -> Optional[dict]:
        with self._payload_lock:
            return self._last_payload

    # ── Snapshot push ─────────────────────────────────────────

    def push_snapshot(self, payload: dict) -> None:
        """Cache the payload and attempt to register it in Consul."""
        self._cache_set(payload)

        if self._consul.is_alive:
            ok = self._consul.register_service(payload)
            if ok:
                logger.info(
                    f"✅ registered snapshot: {payload['Name']} "
                    f"@ {payload['Address']}:{payload['Port']} "
                    f"(tags={len(payload['Tags'])})"
                )
            else:
                logger.warning("⚠️ register failed (cached for retry)")
        else:
            logger.warning("📦 cached snapshot (Consul down)")

    def _push_cached_if_any(self) -> None:
        payload = self._cache_get()
        if not payload:
            return
        logger.info("🔄 Consul back — pushing cached snapshot")
        self.push_snapshot(payload)

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
                tags = self._tag_builder.build_tags(raw)
                payload = self._tag_builder.build_consul_payload(tags)
                self.push_snapshot(payload)
            except Exception as e:
                logger.warning(f"resync failed: {e}")

    # ── Public API ────────────────────────────────────────────

    def initial_sync(self) -> None:
        """Perform one immediate fetch + push on startup."""
        try:
            logger.info("🔍 Initial rawdata snapshot...")
            raw = self._traefik.fetch_rawdata()
            tags = self._tag_builder.build_tags(raw)
            payload = self._tag_builder.build_consul_payload(tags)
            self.push_snapshot(payload)
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
