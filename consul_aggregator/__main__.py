"""
Entry point for the consul_aggregator package.

Usage: python -m consul_aggregator

Env var MODE controls the operating mode:
  - "cluster" (default): KV mode + REST API + management dashboard
  - "kv"               : writes config to Consul KV store
  - "tags"             : registers services with Traefik tags
"""

from .config import Config, setup_logging
from .consul_client import ConsulClient
from .sync import SyncEngine


def main() -> None:
    config = Config.from_env()
    logger = setup_logging()
    logger.debug("main: application starting")

    logger.info("=" * 60)
    logger.info(f"traefik-rawdata-consul-bridge ({config.mode} mode)")
    logger.info(f"  cluster_name:    {config.cluster_name}")
    logger.info(f"  consul:          {config.consul_addr}")
    logger.info(f"  mode:            {config.mode}")
    logger.info(f"  instances:       {len(config.instances)}")
    for inst in config.instances:
        logger.info(f"    - {inst.name}: {inst.url} -> {inst.service_http}")
    logger.info(f"  resync_interval: {config.resync_seconds}s")
    logger.info(f"  healthcheck:     TCP {config.hc_interval}/{config.hc_timeout}")
    if config.mode == "cluster":
        logger.info(f"  api_port:        {config.api_port}")
        logger.info(f"  ui:              {'enabled' if config.enable_ui else 'disabled'}")
    logger.info("=" * 60)

    consul = ConsulClient(config)
    engine = SyncEngine(config, consul)
    logger.debug("main: starting SyncEngine")
    engine.start()

    if config.mode == "cluster":
        from .api import create_app
        app = create_app(config, engine)
        logger.info(f"🌐 Dashboard: http://0.0.0.0:{config.api_port}")
        app.run(host="0.0.0.0", port=config.api_port, debug=False)
    # kv/tags modes block inside engine.start()


if __name__ == "__main__":
    main()
