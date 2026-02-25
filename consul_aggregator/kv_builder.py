"""
KV builder.

Builds Consul KV entries from Traefik rawdata for the Consul KV provider.

Architecture:
  For each gateway node we register TWO Consul services:
    - gw-<NODE>-http   (port 80)   ← routers with entryPoints containing "web"
    - gw-<NODE>-https  (port 443)  ← routers with entryPoints containing "websecure"

  When a gateway router has BOTH entrypoints ["web", "websecure"],
  it is split into two edge routers:
    - <name>_web       → service=gw-<NODE>-http,  entrypoints=[web]
    - <name>_websecure → service=gw-<NODE>-https, entrypoints=[websecure], tls=true

  All config is written to Consul KV under the prefix (default: "traefik"):
    traefik/http/routers/<name>/rule       = Host(`...`)
    traefik/http/routers/<name>/service    = gw-<NODE>-http
    traefik/http/middlewares/<name>/<type>/<key> = value
    traefik/http/services/<name>/loadBalancer/servers/0/url = http://...
"""

import logging
from typing import Dict, List, Tuple

from .config import Config
from .normalizer import (
    extract_http_routers_middlewares,
    flatten_to_kv,
    normalize_router_kv,
    ns_with_provider,
    parse_service_endpoint,
)

logger = logging.getLogger("consul_aggregator")

EP_WEB = "web"
EP_WEBSECURE = "websecure"

# Keys to skip when flattening middleware rawdata
_MW_SKIP_KEYS = {"status", "usedBy"}


class KVBuilder:
    """Transforms Traefik rawdata into Consul KV entries."""

    def __init__(self, config: Config) -> None:
        self._node_name = config.node_name
        self._service_http = config.service_http
        self._service_https = config.service_https
        self._hc_interval = config.hc_interval
        self._hc_timeout = config.hc_timeout
        self._hc_deregister_after = config.hc_deregister_after
        self._prefix = "traefik"
        logger.debug(
            f"KVBuilder initialized: node={self._node_name}, "
            f"http={self._service_http}, https={self._service_https}"
        )

    # ── Helpers ───────────────────────────────────────────────

    @property
    def svc_name_http(self) -> str:
        return f"gw-{self._node_name}-http"

    @property
    def svc_name_https(self) -> str:
        return f"gw-{self._node_name}-https"

    def _classify_entrypoints(self, eps: list) -> Tuple[bool, bool]:
        """Return (has_web, has_websecure) from an entrypoints list."""
        lower = [e.strip().lower() for e in eps]
        return (EP_WEB in lower, EP_WEBSECURE in lower)

    # ── KV entries generation ─────────────────────────────────

    def build_kv_entries(self, rawdata: dict) -> Dict[str, str]:
        """
        Build a flat dict of {consul_kv_key: value} pairs from rawdata.
        Includes services, middlewares, and routers.
        """
        logger.debug(f"build_kv_entries: starting, rawdata keys={list(rawdata.keys())}")
        entries: Dict[str, str] = {}
        p = self._prefix

        # ── Services (load balancer URLs) ─────────────────────
        entries[f"{p}/http/services/{self.svc_name_http}/loadBalancer/servers/0/url"] = (
            self._service_http
        )
        entries[f"{p}/http/services/{self.svc_name_https}/loadBalancer/servers/0/url"] = (
            self._service_https
        )

        # ── serversTransports ──────────────────────────────────
        # HTTP: keep-alive only
        st_http = "keep-alive"
        stp = f"{p}/http/serversTransports/{st_http}"
        entries[f"{stp}/maxIdleConnsPerHost"] = "32"
        entries[f"{stp}/forwardingTimeouts/idleConnTimeout"] = "90s"
        entries[f"{p}/http/services/{self.svc_name_http}/loadBalancer/serversTransport"] = (
            st_http
        )

        # HTTPS: keep-alive + skip TLS verification
        st_https = "insecure-skip-verify"
        stp = f"{p}/http/serversTransports/{st_https}"
        entries[f"{stp}/insecureSkipVerify"] = "true"
        entries[f"{stp}/maxIdleConnsPerHost"] = "32"
        entries[f"{stp}/forwardingTimeouts/idleConnTimeout"] = "90s"
        entries[f"{p}/http/services/{self.svc_name_https}/loadBalancer/serversTransport"] = (
            st_https
        )

        routers_raw, mws_raw = extract_http_routers_middlewares(rawdata)
        logger.debug(f"build_kv_entries: found {len(routers_raw)} routers, {len(mws_raw)} middlewares")

        # ── Middlewares ───────────────────────────────────────
        for mw_name, mw_conf in mws_raw.items():
            if not isinstance(mw_conf, dict):
                logger.debug(f"build_kv_entries: skipping middleware '{mw_name}' (not a dict)")
                continue

            mw_edge_name = ns_with_provider(mw_name, self._node_name)
            mw_prefix = f"{p}/http/middlewares/{mw_edge_name}"
            logger.debug(f"build_kv_entries: processing middleware '{mw_name}' -> '{mw_edge_name}'")

            # Flatten the middleware config, skipping status/usedBy
            cleaned = {
                k: v for k, v in mw_conf.items() if k not in _MW_SKIP_KEYS
            }
            flatten_to_kv(cleaned, mw_prefix, entries)

        # ── Routers ───────────────────────────────────────────
        for r_name, r_conf in routers_raw.items():
            if not isinstance(r_conf, dict):
                logger.debug(f"build_kv_entries: skipping router '{r_name}' (not a dict)")
                continue

            props = normalize_router_kv(r_conf)
            if not props.get("rule"):
                logger.debug(f"build_kv_entries: skipping router '{r_name}' (no rule)")
                continue

            # Rewrite middleware refs to namespaced names
            if "middlewares" in props:
                props["middlewares"] = [
                    ns_with_provider(m, self._node_name)
                    for m in props["middlewares"]
                ]

            eps = props.pop("entryPoints", [EP_WEB])
            has_web, has_websecure = self._classify_entrypoints(eps)

            # Remove tls from base — we control it per variant
            props.pop("tls", None)

            r_base_name = ns_with_provider(r_name, self._node_name)
            logger.debug(
                f"build_kv_entries: router '{r_name}' -> '{r_base_name}' "
                f"(has_web={has_web}, has_websecure={has_websecure})"
            )

            if has_web and has_websecure:
                self._emit_router_kv(
                    entries, f"{r_base_name}_web", props,
                    entrypoints=[EP_WEB],
                    service=self.svc_name_http,
                    tls=False,
                )
                self._emit_router_kv(
                    entries, f"{r_base_name}_websecure", props,
                    entrypoints=[EP_WEBSECURE],
                    service=self.svc_name_https,
                    tls=True,
                )
            elif has_websecure:
                self._emit_router_kv(
                    entries, r_base_name, props,
                    entrypoints=[EP_WEBSECURE],
                    service=self.svc_name_https,
                    tls=True,
                )
            else:
                self._emit_router_kv(
                    entries, r_base_name, props,
                    entrypoints=[EP_WEB],
                    service=self.svc_name_http,
                    tls=False,
                )

        logger.debug(f"build_kv_entries: total {len(entries)} KV entries built")
        return entries

    def _emit_router_kv(
        self,
        entries: Dict[str, str],
        edge_name: str,
        base_props: dict,
        *,
        entrypoints: list,
        service: str,
        tls: bool,
    ) -> None:
        """Write all KV entries for a single edge router."""
        logger.debug(f"_emit_router_kv: edge_name={edge_name}, service={service}, tls={tls}")
        rp = f"{self._prefix}/http/routers/{edge_name}"

        entries[f"{rp}/service"] = service

        if tls:
            entries[f"{rp}/tls"] = "true"

        # Flatten remaining props (rule, middlewares, priority)
        for key, val in base_props.items():
            flatten_to_kv(val, f"{rp}/{key}", entries)

        # Entrypoints (list → indexed keys)
        for i, ep in enumerate(entrypoints):
            entries[f"{rp}/entryPoints/{i}"] = ep

    # ── Consul service payloads (health checks only, no tags) ─

    def build_service_payloads(self) -> List[dict]:
        """
        Build lightweight service registration payloads for Consul.
        These carry NO traefik tags — config is in KV.
        They exist for health checking only.
        """
        logger.debug("build_service_payloads: building lightweight payloads")
        http_addr, http_port = parse_service_endpoint(self._service_http)
        https_addr, https_port = parse_service_endpoint(self._service_https)
        logger.debug(
            f"build_service_payloads: http={http_addr}:{http_port}, https={https_addr}:{https_port}"
        )

        return [
            self._make_service_payload(
                sid=f"gw:{self._node_name}:http",
                name=self.svc_name_http,
                addr=http_addr,
                port=http_port,
            ),
            self._make_service_payload(
                sid=f"gw:{self._node_name}:https",
                name=self.svc_name_https,
                addr=https_addr,
                port=https_port,
            ),
        ]

    def _make_service_payload(
        self, *, sid: str, name: str, addr: str, port: int
    ) -> dict:
        logger.debug(f"_make_service_payload: sid={sid}, name={name}, addr={addr}:{port}")
        return {
            "ID": sid,
            "Name": name,
            "Address": addr,
            "Port": port,
            "Tags": [],
            "Check": {
                "TCP": f"{addr}:{port}",
                "Interval": self._hc_interval,
                "Timeout": self._hc_timeout,
                "DeregisterCriticalServiceAfter": self._hc_deregister_after,
            },
        }
