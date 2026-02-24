"""
Rawdata normalization helpers.

Pure functions for sanitizing names, splitting providers, extracting
and normalizing routers/middlewares from Traefik rawdata.
All functions are stateless — node_name is passed as parameter where needed.
"""

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


# ── Router normalization ─────────────────────────────────────


def normalize_router(router: dict) -> Dict[str, Any]:
    """
    Export minimal safe fields from a router config:
    rule, entryPoints, middlewares, tls, priority.
    Values are returned in their native types (lists stay as lists).
    We intentionally do NOT export "service".
    """
    out: Dict[str, Any] = {}

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
            out["entryPoints"] = eps
        else:
            out["entryPoints"] = [str(eps)]

    mws = router.get("middlewares") or router.get("Middlewares")
    if mws:
        if isinstance(mws, list):
            out["middlewares"] = [str(x) for x in mws if x]
        else:
            out["middlewares"] = [str(mws)]

    tls = router.get("tls") or router.get("TLS")
    if tls is not None:
        if isinstance(tls, dict) or bool(tls):
            out["tls"] = True

    prio = router.get("priority") or router.get("Priority")
    if prio is not None:
        out["priority"] = str(prio)

    return out


# ── Recursive KV flattening ──────────────────────────────────


def flatten_to_kv(data: Any, prefix: str, out: Dict[str, str]) -> None:
    """
    Recursively flatten a nested dict/list into Consul KV path→value pairs.

    Examples:
      {"forwardAuth": {"address": "http://..."}}
      → {"prefix/forwardAuth/address": "http://..."}

      ["web", "websecure"]
      → {"prefix/0": "web", "prefix/1": "websecure"}
    """
    if isinstance(data, dict):
        for k, v in data.items():
            flatten_to_kv(v, f"{prefix}/{k}", out)
    elif isinstance(data, list):
        for i, v in enumerate(data):
            flatten_to_kv(v, f"{prefix}/{i}", out)
    elif isinstance(data, bool):
        out[prefix] = "true" if data else "false"
    elif data is not None:
        out[prefix] = str(data)
    else:
        out[prefix] = ""
