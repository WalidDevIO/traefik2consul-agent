"""
Entry point for the consul_aggregator package.

Usage: python -m consul_aggregator
"""

from .config import Config, setup_logging
from .consul_client import ConsulClient
from .sync import SyncEngine
from .tag_builder import TagBuilder
from .traefik_client import TraefikClient


def main() -> None:
    config = Config.from_env()
    logger = setup_logging()

    logger.info("=" * 60)
    logger.info("traefik-rawdata-consul-bridge")
    logger.info(f"  node:            {config.node_name}")
    logger.info(f"  consul:          {config.consul_addr}")
    logger.info(f"  traefik_url:     {config.traefik_url}")
    logger.info(f"  traefik_host:    {config.traefik_host or '(none)'}")
    logger.info(f"  service:         {config.service}")
    logger.info(f"  resync_interval: {config.resync_seconds}s")
    logger.info(f"  healthcheck:     TCP {config.hc_interval}/{config.hc_timeout}")
    logger.info("=" * 60)

    consul = ConsulClient(config)
    traefik = TraefikClient(config)
    builder = TagBuilder(config)

    engine = SyncEngine(config, consul, traefik, builder)
    engine.start()


if __name__ == "__main__":
    main()
