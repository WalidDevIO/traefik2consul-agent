#!/usr/bin/env python3
"""
traefik-rawdata-consul-bridge (final)
====================================

But:
- Lire la config effective d'un Traefik "gateway" via /rawdata (ou /api/rawdata)
- Publier dans Consul Catalog UN SEUL service par node: gw-<NODE_NAME>
- Ce service porte N tags Traefik correspondant à:
    - N routers (exportés)
    - N middlewares (exportés)
- L'edge Traefik consomme Consul Catalog et génère les routes.
- On "strip" systématiquement les suffixes @provider des noms (rawdata),
  et on namespace tout pour éviter collisions.
- En fallback sans provider, on suffixe __no-provider (comme demandé).

IMPORTANT:
- On n'exporte PAS les services du gateway (backends locaux), car côté edge
  on veut seulement X services (= X gateways). Les routers edge viseront donc
  le service Consul par défaut (= le gw-<NODE_NAME>).
- On réécrit donc:
    - routers[].middlewares: chaque ref "mw@file" -> "<NODE>-mw__file"
    - si pas de "@", -> "<NODE>-mw__no-provider"

Env vars:
  CONSUL_ADDR = http://100.64.0.4:8500
  NODE_NAME = hostname
  RESYNC_SECONDS = 30
  HC_INTERVAL = 10s
  HC_TIMEOUT = 5s
  HC_DEREGISTER_AFTER = 30s

  TRAEFIK_URL  = http://127.0.0.1:8080  (ou l'IP headscale + port du gateway)
  TRAEFIK_HOST = optional; si défini, ajouté en header Host lors du GET /rawdata

Notes:
- Le service Consul est enregistré à l'adresse/port déduits de TRAEFIK_URL.
  (Donc TRAEFIK_URL doit idéalement pointer vers l'endpoint joignable par l'edge)
"""

import os
import time
import json
import re
import threading
import urllib.parse
import logging
from typing import Any, Dict, Tuple, Optional, List

import requests

# ── Logging ───────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
debug_handler = logging.FileHandler("debug.log")
debug_handler.addFilter(lambda record: record.levelno == logging.DEBUG)
debug_handler.setLevel(logging.DEBUG)
debug_handler.setFormatter(formatter)
logger.addHandler(debug_handler)
handler.setFormatter(formatter)
logger.addHandler(handler)

# ── Config ────────────────────────────────────────────────────
CONSUL_ADDR = os.environ.get("CONSUL_ADDR", "http://consul:8500").rstrip("/")
NODE_NAME = os.environ.get("NODE_NAME", os.uname().nodename)
RESYNC_SECONDS = int(os.environ.get("RESYNC_SECONDS", "30"))
HC_INTERVAL = os.environ.get("HC_INTERVAL", "10s")
HC_TIMEOUT = os.environ.get("HC_TIMEOUT", "5s")
HC_DEREGISTER_AFTER = os.environ.get("HC_DEREGISTER_AFTER", "30s")

TRAEFIK_URL = os.environ.get("TRAEFIK_URL", "").rstrip("/")
TRAEFIK_HOST = os.environ.get("TRAEFIK_HOST", "").strip()
SERVICE = os.environ.get("SERVICE", "").strip()

if not TRAEFIK_URL:
    raise SystemExit(
        "TRAEFIK_URL is required (e.g. http://traefik:8080)"
    )

if not SERVICE:
    logger.warning("SERVICE is not set, using TRAEFIK_URL")
    SERVICE = "http:" + TRAEFIK_URL.split(":")[1] + ":80"

# ── Consul health state ───────────────────────────────────────
_consul_alive = True
_consul_lock = threading.Lock()


def consul_is_alive() -> bool:
    with _consul_lock:
        return _consul_alive


def set_consul_alive(v: bool):
    global _consul_alive
    with _consul_lock:
        _consul_alive = v


def consul_health_check() -> bool:
    try:
        r = requests.get(f"{CONSUL_ADDR}/v1/status/leader", timeout=5)
        alive = (r.status_code == 200)
        if alive and not consul_is_alive():
            logger.info("Consul is back UP — will push full snapshot")
        if not alive and consul_is_alive():
            logger.warning(f"Consul health check returned {r.status_code} — cache mode")
        set_consul_alive(alive)
        return alive
    except Exception as e:
        if consul_is_alive():
            logger.warning(f"Consul health check failed — cache mode: {e}")
        set_consul_alive(False)
        return False


def consul_put(path: str, payload: Optional[dict] = None) -> bool:
    try:
        kwargs = {"timeout": 10}
        if payload is not None:
            kwargs["json"] = payload
        r = requests.put(f"{CONSUL_ADDR}{path}", **kwargs)
        if r.status_code in (200, 201, 204):
            if not consul_is_alive():
                logger.info("Consul is back UP")
                set_consul_alive(True)
            return True
        logger.warning(f"Consul {path} returned {r.status_code}: {r.text[:200]}")
        return False
    except Exception as e:
        if consul_is_alive():
            logger.warning(f"Consul unreachable: {e}")
            set_consul_alive(False)
        return False


# ── Traefik rawdata fetch ─────────────────────────────────────
def _rawdata_url(base: str) -> str:
    if base.endswith("/api/rawdata"):
        return base
    return f"{base}/api/rawdata"


def fetch_rawdata() -> dict:
    headers = {}
    if TRAEFIK_HOST:
        headers["Host"] = TRAEFIK_HOST

    last_err = None
    url = _rawdata_url(TRAEFIK_URL)
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        last_err = e

    raise RuntimeError(
        f"Unable to fetch Traefik rawdata from {TRAEFIK_URL} "
        f"(tried /api/rawdata): {last_err}"
    )


# ── Helpers ───────────────────────────────────────────────────
def sanitize_name(name: str) -> str:
    # Keep Traefik-compatible-ish names for tags
    # Spaces -> underscore, then replace unsafe chars
    name = name.replace(" ", "_")
    return re.sub(r"[^a-zA-Z0-9_.-]", "_", name)


def split_provider(name: str) -> Tuple[str, str]:
    """
    Split "X@file" into ("X", "file").
    If no "@", returns (name, "no-provider").
    """
    if "@" in name:
        base, prov = name.rsplit("@", 1)
        base = base.strip()
        prov = prov.strip() or "no-provider"
        return base, prov
    return name.strip(), "no-provider"


def ns_with_provider(original_name: str) -> str:
    """
    Namespace + provider suffix (stripped from @...):
      "redirect-to-https@file" -> "<NODE>-redirect-to-https__file"
      "OIDC MZ@http"           -> "<NODE>-OIDC_MZ__http"
      "compress"              -> "<NODE>-compress__no-provider"
    """
    base, prov = split_provider(original_name)
    base = sanitize_name(base)
    prov = sanitize_name(prov)
    return sanitize_name(f"{NODE_NAME}-{base}__{prov}")


def parse_service_endpoint(url: str) -> Tuple[str, int]:
    u = urllib.parse.urlparse(url)
    host = u.hostname or ""
    if not host:
        raise ValueError(f"Cannot parse hostname from SERVICE={url}")
    if u.port:
        port = int(u.port)
    else:
        port = 443 if u.scheme == "https" else 80
    return host, port


# ── Rawdata extraction ─────────────────────────────────────────
def extract_http_routers_middlewares(raw: dict) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    """
    Routers and middlewares are in the root of the rawdata.
    Example:
    {
        "routers": {
            "router1@docker": { ... },
            "router2@http": { ... }
        },
        "middlewares": {
            "mw1@docker": { ... },
            "mw2@file": { ... }
        }
    }
    """
    routers = {}
    mws = {}

    # routers
    routers = raw.get("routers", {})
    routers = dict(filter(lambda r: not r[0].endswith("@internal"), routers.items()))

    # middlewares
    mws = raw.get("middlewares", {})
    mws = dict(filter(lambda r: not r[0].endswith("@internal"), mws.items()))

    logger.debug(f"Routers: {routers}")
    logger.debug(f"Middlewares: {mws}")

    if not isinstance(routers, dict):
        routers = {}
    if not isinstance(mws, dict):
        mws = {}

    return routers, mws


# ── Normalization / flattening ─────────────────────────────────
def normalize_router(router: dict) -> Dict[str, str]:
    """
    Export minimal safe fields:
      - rule
      - entrypoints
      - middlewares (rewritten later)
      - tls
      - priority
    We DO NOT export "service" on purpose.
    """
    out: Dict[str, str] = {}

    rule = router.get("rule") or router.get("Rule")
    if rule:
        out["rule"] = str(rule)

    eps = router.get("entryPoints") or router.get("entrypoints") or router.get("EntryPoints")
    if eps:
        if isinstance(eps, list):
            out["entrypoints"] = ",".join([str(x) for x in eps])
        else:
            out["entrypoints"] = str(eps)

    mws = router.get("middlewares") or router.get("Middlewares")
    if mws:
        if isinstance(mws, list):
            out["middlewares"] = ",".join([str(x) for x in mws if x])
        else:
            out["middlewares"] = str(mws)

    tls = router.get("tls") or router.get("TLS")
    if tls is not None:
        if isinstance(tls, dict):
            out["tls"] = "true"
        else:
            out["tls"] = "true" if bool(tls) else "false"

    prio = router.get("priority") or router.get("Priority")
    if prio is not None:
        out["priority"] = str(prio)

    return out


def normalize_middlewares(raw_mws: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    Convert each middleware config into label-style flattened props:
      mw_name -> { "forwardauth.address": "...", "headers.customrequestheaders": "{...json...}", ... }
    Strategy:
      Each middleware conf in rawdata is usually:
        { "<type>": { ...conf... }, "status": "enabled", "usedBy": [...] }
      We:
        - ignore "status" and "usedBy"
        - detect the type key (first key that is not status/usedBy)
        - flatten <type>.<key>=value
      Complex values (dict/list) => JSON compact.
    """
    out: Dict[str, Dict[str, str]] = {}

    for mw_name, mw_conf in raw_mws.items():
        if not isinstance(mw_conf, dict):
            continue

        # find middleware type key
        mw_type = None
        for k in mw_conf.keys():
            if k in ("status", "usedBy"):
                continue
            mw_type = k
            break

        props: Dict[str, str] = {}

        if mw_type is None:
            # nothing meaningful; skip
            continue

        conf = mw_conf.get(mw_type, {})
        if conf is None:
            conf = {}

        if isinstance(conf, dict):
            if conf:
                for ck, cv in conf.items():
                    key = f"{mw_type}.{ck}".lower()
                    if isinstance(cv, (dict, list)):
                        props[key] = json.dumps(cv, separators=(",", ":"), ensure_ascii=False)
                    else:
                        props[key] = str(cv)
            else:
                # empty config means "enabled defaults"
                props[mw_type.lower()] = "true"
        else:
            props[mw_type.lower()] = str(conf)

        out[mw_name] = props

    return out


def rewrite_middlewares_list(mw_list_str: str) -> str:
    """
    Rewrite router.middlewares to namespaced, stripped form:
      "redirect-to-https@file,OIDC MZ@http,auth@file" =>
      "<NODE>-redirect-to-https__file,<NODE>-OIDC_MZ__http,<NODE>-auth__file"
    """
    items = [x.strip() for x in mw_list_str.split(",") if x.strip()]
    rewritten = [ns_with_provider(x) for x in items]
    return ",".join(rewritten)


# ── Build Consul tags ─────────────────────────────────────────
def build_tags_from_rawdata(raw: dict) -> List[str]:
    routers_raw, mws_raw = extract_http_routers_middlewares(raw)

    tags: List[str] = ["traefik.enable=true"]

    # Middlewares first (so they exist when routers reference them)
    mws_norm = normalize_middlewares(mws_raw)

    for mw_name, props in mws_norm.items():
        mw_edge_name = ns_with_provider(mw_name)
        for prop, val in props.items():
            # prop already lowercased
            tags.append(f"traefik.http.middlewares.{mw_edge_name}.{prop}={val}")

    # Routers
    for r_name, r_conf in routers_raw.items():
        if not isinstance(r_conf, dict):
            continue

        props = normalize_router(r_conf)
        if not props.get("rule"):
            continue  # no routing rule -> ignore

        # Namespace router name too (avoid collisions)
        r_edge_name = ns_with_provider(r_name)

        # Rewrite middleware refs if present
        if "middlewares" in props and props["middlewares"].strip():
            props["middlewares"] = rewrite_middlewares_list(props["middlewares"])

        # Export router props
        for prop, val in props.items():
            tags.append(f"traefik.http.routers.{r_edge_name}.{prop}={val}")

        # NOTE: we intentionally do NOT export:
        #   traefik.http.routers.<r>.service=...
        # So each router will use the default service of the provider item (the Consul service gw-node).

    return tags


# ── Consul registration payload ────────────────────────────────
def build_consul_payload(tags: List[str]) -> dict:
    addr, port = parse_service_endpoint(SERVICE)
    sid = f"gw:{NODE_NAME}"
    name = f"gw-{NODE_NAME}"

    payload = {
        "ID": sid,
        "Name": name,
        "Address": addr,
        "Port": port,
        "Tags": tags,
        "Check": {
            "TCP": f"{addr}:{port}",
            "Interval": HC_INTERVAL,
            "Timeout": HC_TIMEOUT,
            "DeregisterCriticalServiceAfter": HC_DEREGISTER_AFTER,
        },
    }
    return payload


# ── Cache ─────────────────────────────────────────────────────
_last_payload: Optional[dict] = None
_payload_lock = threading.Lock()


def cache_set(payload: dict):
    global _last_payload
    with _payload_lock:
        _last_payload = payload


def cache_get() -> Optional[dict]:
    with _payload_lock:
        return _last_payload


# ── Sync ──────────────────────────────────────────────────────
def push_snapshot(payload: dict):
    cache_set(payload)
    if consul_is_alive():
        ok = consul_put("/v1/agent/service/register", payload)
        if ok:
            logger.info(
                f"✅ registered snapshot: {payload['Name']} @ {payload['Address']}:{payload['Port']} "
                f"(tags={len(payload['Tags'])})"
            )
        else:
            logger.warning("⚠️ register failed (cached for retry)")
    else:
        logger.warning("📦 cached snapshot (Consul down)")


def push_cached_if_any():
    payload = cache_get()
    if not payload:
        return
    logger.info("🔄 Consul back — pushing cached snapshot")
    push_snapshot(payload)


def consul_monitor():
    was_alive = True
    while True:
        time.sleep(10)
        alive = consul_health_check()
        if alive and not was_alive:
            push_cached_if_any()
        was_alive = alive


def periodic_resync():
    while True:
        time.sleep(RESYNC_SECONDS)
        try:
            raw = fetch_rawdata()
            tags = build_tags_from_rawdata(raw)
            payload = build_consul_payload(tags)
            push_snapshot(payload)
        except Exception as e:
            logger.warning(f"resync failed: {e}")


def main():
    logger.info("=" * 60)
    logger.info("traefik-rawdata-consul-bridge")
    logger.info(f"  node:            {NODE_NAME}")
    logger.info(f"  consul:          {CONSUL_ADDR}")
    logger.info(f"  traefik_url:     {TRAEFIK_URL}")
    logger.info(f"  traefik_host:    {TRAEFIK_HOST or '(none)'}")
    logger.info(f"  service:         {SERVICE}")
    logger.info(f"  resync_interval: {RESYNC_SECONDS}s")
    logger.info(f"  healthcheck:     TCP {HC_INTERVAL}/{HC_TIMEOUT}")
    logger.info("=" * 60)

    consul_health_check()

    # Initial snapshot
    try:
        logger.info("🔍 Initial rawdata snapshot...")
        raw = fetch_rawdata()
        tags = build_tags_from_rawdata(raw)
        payload = build_consul_payload(tags)
        push_snapshot(payload)
    except Exception as e:
        logger.warning(f"initial snapshot failed: {e}")

    # Threads
    threading.Thread(target=consul_monitor, daemon=True).start()
    threading.Thread(target=periodic_resync, daemon=True).start()

    logger.info("👂 Running (periodic snapshot mode)...")
    while True:
        time.sleep(3600)

if __name__ == "__main__":
    main()