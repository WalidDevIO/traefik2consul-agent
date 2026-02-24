"""
Tag builder.

Builds Consul service tags from Traefik rawdata, and constructs
the full Consul registration payloads.

Architecture:
  For each gateway node we register TWO Consul services:
    - gw-<NODE>-http   (port 80)   ← routers with entryPoints containing "web"
    - gw-<NODE>-https  (port 443)  ← routers with entryPoints containing "websecure"

  When a gateway router has BOTH entrypoints ["web", "websecure"],
  it is split into two edge routers:
    - <name>_web       → service=gw-<NODE>-http,  entrypoints=web
    - <name>_websecure → service=gw-<NODE>-https, entrypoints=websecure, tls=true

  Middlewares are shared on both.
"""

import logging
from typing import Dict, List, Tuple

from .config import Config
from .normalizer import (
    extract_http_routers_middlewares,
    normalize_middlewares,
    normalize_router,
    ns_with_provider,
    parse_service_endpoint,
    rewrite_middlewares_list,
)

logger = logging.getLogger("consul_aggregator")

# Entrypoint constants
EP_WEB = "web"
EP_WEBSECURE = "websecure"


class TagBuilder:
    """Transforms Traefik rawdata into Consul-ready tags and payloads."""

    def __init__(self, config: Config) -> None:
        self._node_name = config.node_name
        self._service_http = config.service_http
        self._service_https = config.service_https
        self._hc_interval = config.hc_interval
        self._hc_timeout = config.hc_timeout
        self._hc_deregister_after = config.hc_deregister_after

    # ── Helpers ───────────────────────────────────────────────

    @property
    def _svc_name_http(self) -> str:
        return f"gw-{self._node_name}-http"

    @property
    def _svc_name_https(self) -> str:
        return f"gw-{self._node_name}-https"

    def _classify_entrypoints(self, eps_str: str) -> Tuple[bool, bool]:
        """Return (has_web, has_websecure) from a comma-separated entrypoints string."""
        eps = [e.strip().lower() for e in eps_str.split(",") if e.strip()]
        return (EP_WEB in eps, EP_WEBSECURE in eps)

    # ── Tag generation ────────────────────────────────────────

    def build_tags(self, rawdata: dict) -> Dict[str, List[str]]:
        """
        Extract routers + middlewares from rawdata and return tags
        grouped by service:
          {"http": [...tags for gw-node-http...],
           "https": [...tags for gw-node-https...]}
        """
        routers_raw, mws_raw = extract_http_routers_middlewares(rawdata)

        http_tags: List[str] = [
            "traefik.enable=true",
            # serversTransport: keep-alive for HTTP backends
            "traefik.http.serversTransports.keep-alive.maxIdleConnsPerHost=32",
            "traefik.http.serversTransports.keep-alive.forwardingTimeouts.idleConnTimeout=90s",
            f"traefik.http.services.{self._svc_name_http}.loadBalancer.serversTransport=keep-alive",
        ]
        https_tags: List[str] = [
            "traefik.enable=true",
            # serversTransport: skip TLS verification + keep-alive for HTTPS backends
            "traefik.http.serversTransports.insecure-skip-verify.insecureSkipVerify=true",
            "traefik.http.serversTransports.insecure-skip-verify.maxIdleConnsPerHost=32",
            "traefik.http.serversTransports.insecure-skip-verify.forwardingTimeouts.idleConnTimeout=90s",
            f"traefik.http.services.{self._svc_name_https}.loadBalancer.serversTransport=insecure-skip-verify",
        ]

        # ── Middlewares (shared on both services) ─────────────
        mws_norm = normalize_middlewares(mws_raw)

        for mw_name, props in mws_norm.items():
            mw_edge_name = ns_with_provider(mw_name, self._node_name)
            for prop, val in props.items():
                tag = f"traefik.http.middlewares.{mw_edge_name}.{prop}={val}"
                http_tags.append(tag)
                https_tags.append(tag)

        # ── Routers ───────────────────────────────────────────
        for r_name, r_conf in routers_raw.items():
            if not isinstance(r_conf, dict):
                continue

            props = normalize_router(r_conf)
            if not props.get("rule"):
                continue

            # Rewrite middleware refs
            if "middlewares" in props and props["middlewares"].strip():
                props["middlewares"] = rewrite_middlewares_list(
                    props["middlewares"], self._node_name
                )

            eps_str = props.pop("entrypoints", "web")
            has_web, has_websecure = self._classify_entrypoints(eps_str)

            # Remove tls from base props — we control it per variant
            base_tls = props.pop("tls", None)

            # Namespace the router name
            r_base_name = ns_with_provider(r_name, self._node_name)

            if has_web and has_websecure:
                # Split into two routers with unique suffixes
                self._emit_router(
                    http_tags, f"{r_base_name}_web", props,
                    entrypoints=EP_WEB,
                    service=self._svc_name_http,
                    tls=False,
                )
                self._emit_router(
                    https_tags, f"{r_base_name}_websecure", props,
                    entrypoints=EP_WEBSECURE,
                    service=self._svc_name_https,
                    tls=True,
                )
            elif has_websecure:
                self._emit_router(
                    https_tags, r_base_name, props,
                    entrypoints=EP_WEBSECURE,
                    service=self._svc_name_https,
                    tls=True,
                )
            else:
                # Default: web
                self._emit_router(
                    http_tags, r_base_name, props,
                    entrypoints=EP_WEB,
                    service=self._svc_name_http,
                    tls=False,
                )

        return {"http": http_tags, "https": https_tags}

    @staticmethod
    def _emit_router(
        tags: List[str],
        edge_name: str,
        base_props: dict,
        *,
        entrypoints: str,
        service: str,
        tls: bool,
    ) -> None:
        """Append all tags for a single edge router."""
        prefix = f"traefik.http.routers.{edge_name}"
        tags.append(f"{prefix}.entrypoints={entrypoints}")
        tags.append(f"{prefix}.service={service}")

        if tls:
            tags.append(f"{prefix}.tls=true")

        for prop, val in base_props.items():
            tags.append(f"{prefix}.{prop}={val}")

    # ── Consul payloads ───────────────────────────────────────

    def build_consul_payloads(
        self, tags_by_proto: Dict[str, List[str]]
    ) -> List[dict]:
        """
        Build TWO service registration payloads for Consul:
          - gw-<NODE>-http   (port 80)
          - gw-<NODE>-https  (port 443)
        """
        http_addr, http_port = parse_service_endpoint(self._service_http)
        https_addr, https_port = parse_service_endpoint(self._service_https)

        return [
            self._make_payload(
                sid=f"gw:{self._node_name}:http",
                name=self._svc_name_http,
                addr=http_addr,
                port=http_port,
                tags=tags_by_proto.get("http", []),
            ),
            self._make_payload(
                sid=f"gw:{self._node_name}:https",
                name=self._svc_name_https,
                addr=https_addr,
                port=https_port,
                tags=tags_by_proto.get("https", []),
            ),
        ]

    def _make_payload(
        self, *, sid: str, name: str, addr: str, port: int, tags: List[str]
    ) -> dict:
        return {
            "ID": sid,
            "Name": name,
            "Address": addr,
            "Port": port,
            "Tags": tags,
            "Check": {
                "TCP": f"{addr}:{port}",
                "Interval": self._hc_interval,
                "Timeout": self._hc_timeout,
                "DeregisterCriticalServiceAfter": self._hc_deregister_after,
            },
        }
