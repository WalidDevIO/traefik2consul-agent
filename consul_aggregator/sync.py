"""
Synchronization engine.

Manages the sync loop between Traefik and Consul.
Supports two modes:
  - "kv"   : writes config to Consul KV store (session-bound, auto-cleanup)
  - "tags" : registers services with Traefik tags in Consul catalog

KV mode uses Consul Sessions (Behavior=delete):
  - Session TTL = HC_DEREGISTER_AFTER (same as service deregister timeout)
  - Renew interval = HC_INTERVAL (same as health check interval)
  - All KV keys are acquired with the session
  - If the agent dies, the session expires and Consul deletes all keys
"""

import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from .config import Config, InstanceConfig
from .consul_client import ConsulClient
from .traefik_client import TraefikClient

logger = logging.getLogger("consul_aggregator")


def _parse_duration(s: str) -> int:
    """Parse a Go-style duration string (e.g. '30s', '5m') to seconds."""
    m = re.match(r"^(\d+)(s|m|h)?$", s.strip())
    if not m:
        logger.debug(f"_parse_duration: could not parse '{s}', falling back to 30s")
        return 30  # fallback
    val = int(m.group(1))
    unit = m.group(2) or "s"
    if unit == "m":
        result = val * 60
    elif unit == "h":
        result = val * 3600
    else:
        result = val
    logger.debug(f"_parse_duration: '{s}' -> {result}s")
    return result


class SyncEngine:
    """Orchestrates the sync loop between Traefik and Consul."""

    def __init__(
        self,
        config: Config,
        consul: ConsulClient,
    ) -> None:
        self._config = config
        self._consul = consul
        # cluster mode uses kv under the hood
        self._mode = "kv" if config.mode == "cluster" else config.mode
        
        self._instances: List[Tuple[TraefikClient, Any, InstanceConfig]] = []
        self._instances_lock = threading.Lock()
        for inst in config.instances:
            self._add_instance_internal(inst)

        # Session config — reuse HC values for consistency
        self._session_ttl = config.hc_deregister_after   # e.g. "30s"
        self._renew_interval = _parse_duration(config.hc_interval)  # e.g. 10

        # KV mode: track known keys for differential updates
        self._known_keys: Set[str] = set()
        self._keys_lock = threading.Lock()

        # KV mode: session ID (thread-safe)
        self._session_id: Optional[str] = None
        self._session_lock = threading.Lock()

        # Cache for retry on Consul recovery
        self._cache: Optional[Any] = None
        self._cache_lock = threading.Lock()

        # Sync status tracking
        self._last_sync_time: Optional[float] = None
        self._last_sync_ok: bool = False
        self._sync_count: int = 0

        logger.debug(
            f"SyncEngine initialized: mode={self._mode}, session_ttl={self._session_ttl}, "
            f"renew_interval={self._renew_interval}s, resync={config.resync_seconds}s"
        )

    # ── Instance management ───────────────────────────────────

    def _add_instance_internal(self, inst: InstanceConfig) -> None:
        """Create traefik client + builder for an instance (no lock)."""
        traefik = TraefikClient(inst)
        if self._mode == "kv":
            from .kv_builder import KVBuilder
            builder = KVBuilder(self._config, inst)
        else:
            from .tag_builder import TagBuilder
            builder = TagBuilder(self._config, inst)
        self._instances.append((traefik, builder, inst))

    def add_instance(self, inst: InstanceConfig) -> None:
        """Add a new Traefik instance at runtime."""
        with self._instances_lock:
            # Check for duplicate name
            for _, _, existing in self._instances:
                if existing.name == inst.name:
                    raise ValueError(f"Instance '{inst.name}' already exists")
            self._add_instance_internal(inst)
            # Also add to config.instances for consistency
            self._config.instances.append(inst)
        logger.info(f"➕ Instance added: {inst.name} ({inst.url})")

    def remove_instance(self, name: str) -> bool:
        """Remove a Traefik instance by name at runtime."""
        with self._instances_lock:
            for i, (_, _, inst) in enumerate(self._instances):
                if inst.name == name:
                    self._instances.pop(i)
                    self._config.instances = [
                        ic for ic in self._config.instances if ic.name != name
                    ]
                    logger.info(f"➖ Instance removed: {name}")
                    return True
        return False

    def get_instances(self) -> List[InstanceConfig]:
        """Return a snapshot of current instances."""
        with self._instances_lock:
            return [inst for _, _, inst in self._instances]

    def get_status(self) -> dict:
        """Return engine status for the API."""
        return {
            "mode": self._mode,
            "consul_alive": self._consul.is_alive,
            "instance_count": len(self._instances),
            "last_sync_time": self._last_sync_time,
            "last_sync_ok": self._last_sync_ok,
            "sync_count": self._sync_count,
            "session_id": self._get_session() if self._mode == "kv" else None,
            "cluster_name": self._config.cluster_name,
            "resync_seconds": self._config.resync_seconds,
        }

    def manual_resync(self) -> bool:
        """Trigger a manual resync cycle."""
        try:
            data = self._fetch_and_build_all()
            self._push(data)
            return True
        except Exception as e:
            logger.warning(f"Manual resync failed: {e}")
            return False

    # ── Cache ─────────────────────────────────────────────────

    def _cache_set(self, data: Any) -> None:
        logger.debug(f"_cache_set: storing data (type={type(data).__name__})")
        with self._cache_lock:
            self._cache = data

    def _cache_get(self) -> Optional[Any]:
        with self._cache_lock:
            has_data = self._cache is not None
            logger.debug(f"_cache_get: has_data={has_data}")
            return self._cache

    # ── Session management (KV mode) ─────────────────────────

    def _get_session(self) -> Optional[str]:
        with self._session_lock:
            logger.debug(f"_get_session: session_id={self._session_id}")
            return self._session_id

    def _ensure_session(self) -> Optional[str]:
        """Create a session if we don't have one."""
        with self._session_lock:
            if self._session_id:
                logger.debug(f"_ensure_session: reusing existing session {self._session_id}")
                return self._session_id

            logger.debug(f"_ensure_session: creating new session for cluster={self._config.cluster_name}")
            sid = self._consul.session_create(
                name=f"consul-aggregator-{self._config.cluster_name}",
                ttl=self._session_ttl,
            )
            self._session_id = sid
            logger.debug(f"_ensure_session: new session_id={sid}")
            return sid

    def _session_renew_loop(self) -> None:
        """Background thread: renew the session at HC_INTERVAL."""
        logger.debug("_session_renew_loop: thread started")
        while True:
            time.sleep(self._renew_interval)
            sid = self._get_session()
            if not sid:
                logger.debug("_session_renew_loop: no session to renew, skipping")
                continue
            logger.debug(f"_session_renew_loop: renewing session {sid}")
            ok = self._consul.session_renew(sid)
            if not ok:
                logger.warning("🔑 session renew failed — will recreate")
                with self._session_lock:
                    self._session_id = None

    # ── KV mode sync ──────────────────────────────────────────

    def _sync_kv(self, entries: Dict[str, str]) -> bool:
        """Write KV entries with session acquire, delete stale keys."""
        logger.debug(f"_sync_kv: syncing {len(entries)} entries")
        session = self._ensure_session()
        if not session:
            logger.warning("⚠️ no session — cannot write KV (cached for retry)")
            return False

        new_keys = set(entries.keys())

        with self._keys_lock:
            stale_keys = self._known_keys - new_keys

        if stale_keys:
            logger.debug(f"_sync_kv: deleting {len(stale_keys)} stale keys: {list(stale_keys)[:5]}...")
        for key in stale_keys:
            self._consul.kv_delete(key)

        if stale_keys:
            logger.info(f"🗑️  deleted {len(stale_keys)} stale KV keys")

        ok = True
        failed_count = 0
        for key, value in entries.items():
            if not self._consul.kv_acquire(key, value, session):
                ok = False
                failed_count += 1

        logger.debug(f"_sync_kv: done — ok={ok}, total={len(entries)}, failed={failed_count}")

        with self._keys_lock:
            self._known_keys = new_keys

        return ok

    def _push_kv(self, data: Tuple[Dict[str, str], List[dict]]) -> None:
        """Cache KV entries and services, and attempt sync."""
        entries, payloads = data
        logger.debug(f"_push_kv: pushing {len(entries)} KV entries and {len(payloads)} services")
        self._cache_set(data)

        if not self._consul.is_alive:
            logger.warning("📦 cached snapshot (Consul down)")
            return

        ok = self._sync_kv(entries)
        if ok:
            logger.info(f"✅ synced {len(entries)} KV entries (session-bound)")
        else:
            logger.warning("⚠️ some KV writes failed (cached for retry)")

        # Register lightweight services (health checks only)
        logger.debug(f"_push_kv: registering {len(payloads)} lightweight services")
        for payload in payloads:
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
        logger.debug(f"_push_tags: pushing {len(payloads)} service payloads")
        self._cache_set(payloads)

        if not self._consul.is_alive:
            logger.warning("📦 cached snapshot (Consul down)")
            return

        for payload in payloads:
            logger.debug(
                f"_push_tags: registering {payload['Name']} "
                f"({len(payload.get('Tags', []))} tags)"
            )
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
        logger.debug(f"_push: mode={self._mode}")
        if self._mode == "kv":
            self._push_kv(data)
        else:
            self._push_tags(data)

    def _fetch_and_build_all(self) -> Any:
        import time as _time
        if self._mode == "kv":
            all_entries = {}
            all_payloads = []
            with self._instances_lock:
                instances_snapshot = list(self._instances)
            for traefik, builder, inst in instances_snapshot:
                try:
                    raw = traefik.fetch_rawdata()
                    entries = builder.build_kv_entries(raw)
                    payloads = builder.build_service_payloads()
                    all_entries.update(entries)
                    all_payloads.extend(payloads)
                except Exception as e:
                    logger.warning(f"Failed to fetch/build for instance {inst.name}: {e}")
            self._last_sync_time = _time.time()
            self._last_sync_ok = bool(all_entries)
            self._sync_count += 1
            return (all_entries, all_payloads)
        else:
            all_payloads = []
            with self._instances_lock:
                instances_snapshot = list(self._instances)
            for traefik, builder, inst in instances_snapshot:
                try:
                    raw = traefik.fetch_rawdata()
                    tags_by_proto = builder.build_tags(raw)
                    payloads = builder.build_consul_payloads(tags_by_proto)
                    all_payloads.extend(payloads)
                except Exception as e:
                    logger.warning(f"Failed to fetch/build for instance {inst.name}: {e}")
            self._last_sync_time = _time.time()
            self._last_sync_ok = bool(all_payloads)
            self._sync_count += 1
            return all_payloads

    def _push_cached_if_any(self) -> None:
        data = self._cache_get()
        if not data:
            logger.debug("_push_cached_if_any: no cached data")
            return
        logger.info("🔄 Consul back — pushing cached snapshot")
        self._push(data)

    # ── Background threads ────────────────────────────────────

    def _consul_monitor(self) -> None:
        logger.debug("_consul_monitor: thread started")
        was_alive = True
        while True:
            time.sleep(self._renew_interval)
            alive = self._consul.health_check()
            logger.debug(f"_consul_monitor: alive={alive}, was_alive={was_alive}")
            if alive and not was_alive:
                self._push_cached_if_any()
            was_alive = alive

    def _periodic_resync(self) -> None:
        logger.debug(f"_periodic_resync: thread started, interval={self._config.resync_seconds}s")
        while True:
            time.sleep(self._config.resync_seconds)
            logger.debug("_periodic_resync: starting resync cycle")
            try:
                data = self._fetch_and_build_all()
                self._push(data)
                logger.debug("_periodic_resync: resync cycle completed successfully")
            except Exception as e:
                logger.warning(f"resync failed: {e}")

    # ── Public API ────────────────────────────────────────────

    def initial_sync(self) -> None:
        logger.debug("initial_sync: starting initial sync")
        try:
            logger.info("🔍 Initial rawdata snapshot...")
            data = self._fetch_and_build_all()
            self._push(data)
            logger.debug("initial_sync: initial sync completed")
        except Exception as e:
            logger.debug(f"initial_sync: exception: {e}")
            logger.warning(f"initial snapshot failed: {e}")

    def start(self) -> None:
        logger.debug("start: beginning engine startup")
        self._consul.health_check()
        self.initial_sync()

        logger.debug("start: launching consul_monitor thread")
        threading.Thread(target=self._consul_monitor, daemon=True).start()
        logger.debug("start: launching periodic_resync thread")
        threading.Thread(target=self._periodic_resync, daemon=True).start()

        # KV mode: start session renew loop
        if self._mode == "kv":
            logger.debug("start: launching session_renew_loop thread (KV mode)")
            threading.Thread(target=self._session_renew_loop, daemon=True).start()

        logger.info("👂 Running (periodic snapshot mode)...")
        if self._config.mode == "cluster":
            # In cluster mode, don't block — the API server controls the lifecycle
            return
        while True:
            time.sleep(3600)
