"""
Configuration and logging setup.

Loads all configuration from environment variables into a typed dataclass.
"""

import os
import logging
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Immutable configuration loaded from environment variables."""

    consul_addr: str
    node_name: str
    resync_seconds: int
    hc_interval: str
    hc_timeout: str
    hc_deregister_after: str
    traefik_url: str
    traefik_host: str
    service: str

    @classmethod
    def from_env(cls) -> "Config":
        """Build a Config from the current environment, with validation."""
        consul_addr = os.environ.get("CONSUL_ADDR", "http://consul:8500").rstrip("/")
        node_name = os.environ.get("NODE_NAME", os.uname().nodename)
        resync_seconds = int(os.environ.get("RESYNC_SECONDS", "30"))
        hc_interval = os.environ.get("HC_INTERVAL", "10s")
        hc_timeout = os.environ.get("HC_TIMEOUT", "5s")
        hc_deregister_after = os.environ.get("HC_DEREGISTER_AFTER", "30s")

        traefik_url = os.environ.get("TRAEFIK_URL", "").rstrip("/")
        traefik_host = os.environ.get("TRAEFIK_HOST", "").strip()
        service = os.environ.get("SERVICE", "").strip()

        if not traefik_url:
            raise SystemExit("TRAEFIK_URL is required (e.g. http://traefik:8080)")

        if not service:
            logging.getLogger(__name__).warning("SERVICE is not set, using TRAEFIK_URL")
            service = "http:" + traefik_url.split(":")[1] + ":80"

        return cls(
            consul_addr=consul_addr,
            node_name=node_name,
            resync_seconds=resync_seconds,
            hc_interval=hc_interval,
            hc_timeout=hc_timeout,
            hc_deregister_after=hc_deregister_after,
            traefik_url=traefik_url,
            traefik_host=traefik_host,
            service=service,
        )


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

    return logger
