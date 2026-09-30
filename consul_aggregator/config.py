"""
Configuration and logging setup.

Loads all configuration from environment variables into a typed dataclass.
"""

import os
import logging
import urllib.parse
from dataclasses import dataclass


@dataclass(frozen=True)
class InstanceConfig:
    name: str
    url: str
    host: str
    service_http: str
    service_https: str


@dataclass
class Config:
    """Configuration loaded from environment variables."""

    consul_addr: str
    cluster_name: str
    resync_seconds: int
    hc_interval: str
    hc_timeout: str
    hc_deregister_after: str
    instances: list[InstanceConfig]
    mode: str  # "kv", "tags", or "cluster"
    api_port: int
    enable_ui: bool

    @classmethod
    def from_env(cls) -> "Config":
        """Build a Config from the current environment, with validation."""
        consul_addr = os.environ.get("CONSUL_ADDR", "http://consul:8500").rstrip("/")
        mode = os.environ.get("MODE", "cluster").strip().lower()
        if mode not in ("kv", "tags", "cluster"):
            raise SystemExit(f"MODE must be 'kv', 'tags', or 'cluster', got '{mode}'")

        default_name = "aggregator" if mode == "cluster" else os.uname().nodename
        cluster_name = os.environ.get("CLUSTER_NAME", os.environ.get("NODE_NAME", default_name))
        resync_seconds = int(os.environ.get("RESYNC_SECONDS", "30"))
        hc_interval = os.environ.get("HC_INTERVAL", "10s")
        hc_timeout = os.environ.get("HC_TIMEOUT", "5s")
        hc_deregister_after = os.environ.get("HC_DEREGISTER_AFTER", "30s")
        api_port = int(os.environ.get("API_PORT", "8099"))
        enable_ui = os.environ.get("DISABLE_UI", "false").strip().lower() not in ("1", "true", "yes")

        traefik_url = os.environ.get("TRAEFIK_URL", "").rstrip("/")
        traefik_host = os.environ.get("TRAEFIK_HOST", "").strip()
        service = os.environ.get("SERVICE", "").strip()

        import json

        instances = []
        instances_env = os.environ.get("TRAEFIK_INSTANCES")
        if instances_env:
            try:
                parsed_instances = json.loads(instances_env)
                for i, inst in enumerate(parsed_instances):
                    name = inst.get("name", f"inst{i}")
                    url = inst.get("url", "").rstrip("/")
                    if not url:
                        raise SystemExit(f"Instance {name} missing 'url'")
                    
                    host_hdr = inst.get("host", "").strip()
                    svc = inst.get("service", "").strip()
                    if not svc:
                        logging.getLogger(__name__).warning(f"Instance {name}: service not set, using url")
                        svc = "http:" + url.split(":")[1] + ":80"
                    
                    parsed = urllib.parse.urlparse(svc)
                    host = parsed.hostname or ""
                    svc_http = f"http://{host}:80"
                    svc_https = f"https://{host}:443"
                    
                    instances.append(InstanceConfig(
                        name=name,
                        url=url,
                        host=host_hdr,
                        service_http=svc_http,
                        service_https=svc_https,
                    ))
            except json.JSONDecodeError as e:
                raise SystemExit(f"Failed to parse TRAEFIK_INSTANCES JSON: {e}")
        else:
            if not traefik_url and mode != "cluster":
                raise SystemExit("TRAEFIK_URL or TRAEFIK_INSTANCES is required")

            if not traefik_url:
                # cluster mode with no initial instances
                pass
            else:

                if not service:
                    logging.getLogger(__name__).warning("SERVICE is not set, using TRAEFIK_URL")
                    service = "http:" + traefik_url.split(":")[1] + ":80"

                parsed = urllib.parse.urlparse(service)
                host = parsed.hostname or ""
                service_http = f"http://{host}:80"
                service_https = f"https://{host}:443"

                instances.append(InstanceConfig(
                    name="default",
                    url=traefik_url,
                    host=traefik_host,
                    service_http=service_http,
                    service_https=service_https,
                ))

        cfg = cls(
            consul_addr=consul_addr,
            cluster_name=cluster_name,
            resync_seconds=resync_seconds,
            hc_interval=hc_interval,
            hc_timeout=hc_timeout,
            hc_deregister_after=hc_deregister_after,
            instances=instances,
            mode=mode,
            api_port=api_port,
            enable_ui=enable_ui,
        )
        logger = logging.getLogger("consul_aggregator")
        logger.debug(
            f"Config loaded: consul_addr={consul_addr}, cluster={cluster_name}, "
            f"mode={mode}, resync={resync_seconds}s, instances={len(instances)}"
        )
        return cfg


def setup_logging() -> logging.Logger:
    """Configure and return the application logger."""
    logger = logging.getLogger("consul_aggregator")
    logger.setLevel(logging.DEBUG)

    # Console handler — INFO and above
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console.setFormatter(fmt)
    logger.addHandler(console)

    # File handler — DEBUG only
    if os.environ.get("DEBUG", "false").lower() in ("1", "true", "yes"):
        debug_file = logging.FileHandler("debug.log")
        debug_file.setLevel(logging.DEBUG)
        debug_file.addFilter(lambda record: record.levelno == logging.DEBUG)
        debug_file.setFormatter(fmt)
        logger.addHandler(debug_file)
        logger.debug("setup_logging: DEBUG file handler enabled (debug.log)")

    logger.debug("setup_logging: logging configured")

    return logger
