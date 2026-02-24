"""
Tag builder.

Builds Consul service tags from Traefik rawdata, and constructs
the full Consul registration payload.
"""

import logging
from typing import List

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


class TagBuilder:
    """Transforms Traefik rawdata into Consul-ready tags and payloads."""

    def __init__(self, config: Config) -> None:
        self._node_name = config.node_name
        self._service_url = config.service
        self._hc_interval = config.hc_interval
        self._hc_timeout = config.hc_timeout
        self._hc_deregister_after = config.hc_deregister_after

    # ── Tag generation ────────────────────────────────────────

    def build_tags(self, rawdata: dict) -> List[str]:
        """Extract routers + middlewares from rawdata and return Consul tags."""
        routers_raw, mws_raw = extract_http_routers_middlewares(rawdata)

        tags: List[str] = ["traefik.enable=true"]

        # Middlewares first (so they exist when routers reference them)
        mws_norm = normalize_middlewares(mws_raw)

        for mw_name, props in mws_norm.items():
            mw_edge_name = ns_with_provider(mw_name, self._node_name)
            for prop, val in props.items():
                tags.append(
                    f"traefik.http.middlewares.{mw_edge_name}.{prop}={val}"
                )

        # Routers
        for r_name, r_conf in routers_raw.items():
            if not isinstance(r_conf, dict):
                continue

            props = normalize_router(r_conf)
            if not props.get("rule"):
                continue  # no routing rule -> ignore

            # Namespace router name too (avoid collisions)
            r_edge_name = ns_with_provider(r_name, self._node_name)

            # Rewrite middleware refs if present
            if "middlewares" in props and props["middlewares"].strip():
                props["middlewares"] = rewrite_middlewares_list(
                    props["middlewares"], self._node_name
                )

            # Export router props
            for prop, val in props.items():
                tags.append(
                    f"traefik.http.routers.{r_edge_name}.{prop}={val}"
                )

            # NOTE: we intentionally do NOT export
            #   traefik.http.routers.<r>.service=...
            # So each router will use the default service of the Consul provider.

        return tags

    # ── Consul payload ────────────────────────────────────────

    def build_consul_payload(self, tags: List[str]) -> dict:
        """Build the full service registration payload for Consul."""
        addr, port = parse_service_endpoint(self._service_url)
        sid = f"gw:{self._node_name}"
        name = f"gw-{self._node_name}"

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
