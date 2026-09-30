"""
REST API for cluster mode.

Provides endpoints to manage Traefik instances, view status, and trigger resyncs.
Serves the micro-frontend dashboard as static files.
"""

import logging
import os
import time
import urllib.parse
from dataclasses import asdict
from typing import TYPE_CHECKING

from flask import Flask, jsonify, request, send_from_directory

if TYPE_CHECKING:
    from .sync import SyncEngine

from .config import Config, InstanceConfig

logger = logging.getLogger("consul_aggregator")


def create_app(config: Config, engine: "SyncEngine") -> Flask:
    """Create and configure the Flask app for cluster mode."""
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    app = Flask(__name__, static_folder=static_dir, static_url_path="/static")

    # ── Frontend ──────────────────────────────────────────────

    if config.enable_ui:
        @app.route("/")
        def index():
            return send_from_directory(static_dir, "index.html")
    else:
        @app.route("/")
        def index():
            return jsonify({"status": "ok", "ui": "disabled", "api": "/api/"})

    # ── API: Status ───────────────────────────────────────────

    @app.route("/api/status")
    def api_status():
        status = engine.get_status()
        return jsonify(status)

    # ── API: Instances CRUD ───────────────────────────────────

    @app.route("/api/instances", methods=["GET"])
    def list_instances():
        instances = engine.get_instances()
        return jsonify([asdict(i) for i in instances])

    @app.route("/api/instances", methods=["POST"])
    def add_instance():
        data = request.get_json(force=True)
        name = data.get("name", "").strip()
        url = data.get("url", "").strip().rstrip("/")
        host = data.get("host", "").strip()
        service = data.get("service", "").strip()

        if not name:
            return jsonify({"error": "name is required"}), 400
        if not url:
            return jsonify({"error": "url is required"}), 400

        # Derive service HTTP/HTTPS
        if not service:
            service = "http:" + url.split(":")[1] + ":80"

        parsed = urllib.parse.urlparse(service)
        svc_host = parsed.hostname or ""
        svc_http = f"http://{svc_host}:80"
        svc_https = f"https://{svc_host}:443"

        inst = InstanceConfig(
            name=name,
            url=url,
            host=host,
            service_http=svc_http,
            service_https=svc_https,
        )

        try:
            engine.add_instance(inst)
        except ValueError as e:
            return jsonify({"error": str(e)}), 409

        return jsonify(asdict(inst)), 201

    @app.route("/api/instances/<name>", methods=["DELETE"])
    def delete_instance(name: str):
        removed = engine.remove_instance(name)
        if not removed:
            return jsonify({"error": f"Instance '{name}' not found"}), 404
        return jsonify({"ok": True, "removed": name})

    # ── API: Resync ───────────────────────────────────────────

    @app.route("/api/resync", methods=["POST"])
    def api_resync():
        ok = engine.manual_resync()
        return jsonify({"ok": ok})

    # ── API: Config (read-only) ───────────────────────────────

    @app.route("/api/config")
    def api_config():
        return jsonify({
            "consul_addr": config.consul_addr,
            "cluster_name": config.cluster_name,
            "mode": config.mode,
            "resync_seconds": config.resync_seconds,
            "hc_interval": config.hc_interval,
            "hc_timeout": config.hc_timeout,
            "hc_deregister_after": config.hc_deregister_after,
            "api_port": config.api_port,
            "enable_ui": config.enable_ui,
        })

    # ── API: Config update ────────────────────────────────────

    @app.route("/api/config", methods=["PATCH"])
    def update_config():
        data = request.get_json(force=True)

        if "resync_seconds" in data:
            try:
                config.resync_seconds = int(data["resync_seconds"])
            except (ValueError, TypeError):
                return jsonify({"error": "resync_seconds must be an integer"}), 400

        return jsonify({"ok": True})

    return app
