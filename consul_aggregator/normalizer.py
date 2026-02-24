"""
Rawdata normalization helpers.

Pure functions for sanitizing names, splitting providers, extracting
and normalizing routers/middlewares from Traefik rawdata.
All functions are stateless — node_name is passed as parameter where needed.
"""

import json
import re
import urllib.parse
from typing import Any, Dict, Tuple


# ── Name helpers ──────────────────────────────────────────────


def sanitize_name(name: str) -> str:
    """Replace spaces and unsafe characters for Traefik-compatible tag names."""
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


def ns_with_provider(original_name: str, node_name: str) -> str:
    """
    Namespace + provider suffix (stripped from @...):
      "redirect-to-https@file" -> "<NODE>-redirect-to-https__file"
      "OIDC MZ@http"           -> "<NODE>-OIDC_MZ__http"
      "compress"               -> "<NODE>-compress__no-provider"
    """
    base, prov = split_provider(original_name)
    base = sanitize_name(base)
    prov = sanitize_name(prov)
    return sanitize_name(f"{node_name}-{base}__{prov}")


def parse_service_endpoint(url: str) -> Tuple[str, int]:
    """Extract (host, port) from a service URL."""
    u = urllib.parse.urlparse(url)
    host = u.hostname or ""
    if not host:
        raise ValueError(f"Cannot parse hostname from SERVICE={url}")
    if u.port:
        port = int(u.port)
    else:
        port = 443 if u.scheme == "https" else 80
    return host, port


# ── Rawdata extraction ────────────────────────────────────────


def extract_http_routers_middlewares(
    raw: dict,
) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    """
    Extract routers and middlewares from rawdata, filtering out @internal entries.
    """
    routers = raw.get("routers", {})
    routers = dict(
        filter(lambda r: not r[0].endswith("@internal"), routers.items())
    )

    mws = raw.get("middlewares", {})
    mws = dict(filter(lambda r: not r[0].endswith("@internal"), mws.items()))

    if not isinstance(routers, dict):
        routers = {}
    if not isinstance(mws, dict):
        mws = {}

    return routers, mws


# ── Router / middleware normalization ─────────────────────────


def normalize_router(router: dict) -> Dict[str, str]:
    """
    Export minimal safe fields from a router config:
    rule, entrypoints, middlewares, tls, priority.
    We intentionally do NOT export "service".
    """
    out: Dict[str, str] = {}

    rule = router.get("rule") or router.get("Rule")
    if rule:
        out["rule"] = str(rule)

    eps = (
        router.get("entryPoints")
        or router.get("entrypoints")
        or router.get("EntryPoints")
    )
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
      mw_name -> { "forwardauth.address": "...", ... }

    Strategy:
      - ignore "status" and "usedBy" keys
      - detect the type key (first remaining key)
      - flatten <type>.<key>=value
      - complex values (dict/list) => JSON compact
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

        if mw_type is None:
            continue

        props: Dict[str, str] = {}
        conf = mw_conf.get(mw_type, {})
        if conf is None:
            conf = {}

        if isinstance(conf, dict):
            if conf:
                for ck, cv in conf.items():
                    key = f"{mw_type}.{ck}".lower()
                    if isinstance(cv, (dict, list)):
                        props[key] = json.dumps(
                            cv, separators=(",", ":"), ensure_ascii=False
                        )
                    else:
                        props[key] = str(cv)
            else:
                # empty config means "enabled defaults"
                props[mw_type.lower()] = "true"
        else:
            props[mw_type.lower()] = str(conf)

        out[mw_name] = props

    return out


def rewrite_middlewares_list(mw_list_str: str, node_name: str) -> str:
    """
    Rewrite router.middlewares to namespaced, stripped form:
      "redirect-to-https@file,OIDC MZ@http" ->
      "<NODE>-redirect-to-https__file,<NODE>-OIDC_MZ__http"
    """
    items = [x.strip() for x in mw_list_str.split(",") if x.strip()]
    rewritten = [ns_with_provider(x, node_name) for x in items]
    return ",".join(rewritten)
