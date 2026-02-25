"""
Entry point for the consul_aggregator package.

Usage: python -m consul_aggregator

Env var MODE controls the operating mode:
  - "kv"   (default): writes config to Consul KV store
  - "tags"          : registers services with Traefik tags
"""

from .config import Config, setup_logging
from .consul_client import ConsulClient
from .sync import SyncEngine
from .traefik_client import TraefikClient


def main() -> None:
    config = Config.from_env()
    logger = setup_logging()
    logger.debug("main: application starting")

    logger.info("=" * 60)
    logger.info(f"traefik-rawdata-consul-bridge ({config.mode} mode)")
    logger.info(f"  node:            {config.node_name}")
    logger.info(f"  consul:          {config.consul_addr}")
    logger.info(f"  mode:            {config.mode}")
    logger.info(f"  traefik_url:     {config.traefik_url}")
    logger.info(f"  traefik_host:    {config.traefik_host or '(none)'}")
    logger.info(f"  service_http:    {config.service_http}")
    logger.info(f"  service_https:   {config.service_https}")
    logger.info(f"  resync_interval: {config.resync_seconds}s")
    logger.info(f"  healthcheck:     TCP {config.hc_interval}/{config.hc_timeout}")
    logger.info("=" * 60)

    consul = ConsulClient(config)
    traefik = TraefikClient(config)

    if config.mode == "kv":
        from .kv_builder import KVBuilder
        builder = KVBuilder(config)
        logger.debug("main: using KVBuilder")
    else:
        from .tag_builder import TagBuilder
        builder = TagBuilder(config)
        logger.debug("main: using TagBuilder")

    engine = SyncEngine(config, consul, traefik, builder)
    logger.debug("main: starting SyncEngine")
    engine.start()


if __name__ == "__main__":
    main()
