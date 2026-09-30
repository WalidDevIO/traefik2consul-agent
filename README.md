# Consul Aggregator

Bridge between **Traefik** Layer 2 reverse proxies and a **Consul**-backed **Traefik Edge** (Layer 1).

The agent watches one or more internal Traefik instances via their `/api/rawdata` endpoint, extracts the live routing configuration (routers, middlewares), and publishes it to Consul so that the edge Traefik can serve it.

> ⚠️ **SECURITY WARNING:** This service (and its web dashboard / REST API) is designed strictly for internal, trusted network environments. **It is absolutely NOT designed to be exposed in a DMZ or to the public Internet.** There is no authentication mechanism. Exposing it could allow attackers to manipulate your routing infrastructure.

> **Multi-instance** — a single agent can aggregate multiple Traefik L2 instances into the same Consul cluster.  
> **Cluster mode** — provides a REST API and a web dashboard to manage instances at runtime without restarting.

## Architecture

```
                     Internet
                        │
                ┌───────▼───────┐
                │  Edge Traefik │  (L1) — public-facing
                │  consul prov. │
                └───┬───────┬───┘
                    │       │
           ┌────────▼──┐ ┌──▼────────┐
           │  gw-http  │ │ gw-https  │   ◄── Consul services (per instance)
           │  :80      │ │ :443      │
           └────┬──────┘ └──────┬────┘
                │               │
    ┌───────────▼───┐   ┌───────▼───────────┐
    │ L2 Traefik #1 │   │ L2 Traefik #2 ... │   — internal gateways
    │ :8080         │   │ :8080             │
    └───────▲───────┘   └───────▲───────────┘
            │ GET /api/rawdata  │
    ┌───────┴───────────────────┴───────┐
    │       consul_aggregator          │  ◄── this agent
    │   (aggregates all L2 instances)  │
    │   dashboard :8099 (cluster mode) │
    └───────────────┬──────────────────┘
                    │ PUT /v1/kv/...  or  PUT /v1/agent/service/register
                    ▼
               ┌──────────┐
               │  Consul  │
               └──────────┘
```

### Entrypoint splitting

Each L2 gateway instance registers **two services** in Consul:

| Service | Port | Routers with entrypoint |
|---|---|---|
| `gw-<NODE>-<INSTANCE>-http` | 80 | `web` |
| `gw-<NODE>-<INSTANCE>-https` | 443 | `websecure` (tls=true) |

> When using a single instance named `default` (legacy config), the `<INSTANCE>` part is omitted: `gw-<NODE>-http`.

When a router has **both** `web` and `websecure` entrypoints, it is split into two edge routers:
- `<name>_web` → routes to the HTTP service
- `<name>_websecure` → routes to the HTTPS service with `tls=true`

This ensures the edge Traefik knows exactly which port and protocol to use.

---

## Modes

The agent supports three operating modes, controlled by the `MODE` env var:

### `MODE=tags` (Consul Catalog)

- Publishes routing config as **service tags** in the Consul catalog
- Edge Traefik uses `providers.consulCatalog`
- Cleanup is automatic: when the agent dies, the health check fails → service is deregistered after `HC_DEREGISTER_AFTER` → tags disappear

### `MODE=kv` (Consul KV — default)

- Publishes routing config as **key-value pairs** under the `traefik/` prefix
- Edge Traefik uses `providers.consul` (KV provider)
- Uses **Consul Sessions** for automatic cleanup (see below)

Edge Traefik static configuration for KV mode:
```yaml
providers:
  consul:
    endpoints:
      - "consul:8500"
    rootKey: "traefik"
```

### `MODE=cluster` (KV + Dashboard + REST API)

- Same sync behavior as `kv` mode
- Exposes a **REST API** on `API_PORT` (default `8099`) for managing instances
- Serves a **web dashboard** at `http://<host>:<API_PORT>/`
- Instances can be **added and removed at runtime** without restarting the agent
- Instances from `TRAEFIK_INSTANCES` or `TRAEFIK_URL` env vars are loaded as initial state

---

## Consul Sessions (KV mode auto-cleanup)

### The problem

In tags mode, Consul natively handles cleanup: when a service's health check fails, the service (and its tags) are deregistered. In KV mode, keys have **no link** to service health — they persist forever, even if the agent and L2 gateway are dead. The edge Traefik keeps routing traffic to a dead backend.

### The solution — sessions with `Behavior=delete`

Consul has a **sessions** mechanism designed for exactly this:

```
Agent starts
  │
  ├─ 1. PUT /v1/session/create
  │     body: { "Name": "consul-aggregator-node1",
  │             "TTL": "30s",
  │             "Behavior": "delete" }
  │     → returns session ID: "abc-123"
  │
  ├─ 2. PUT /v1/kv/traefik/http/routers/foo/rule?acquire=abc-123
  │     body: "Host(`example.com`)"
  │     → key is now BOUND to session abc-123
  │
  ├─ 3. Every 10s: PUT /v1/session/renew/abc-123
  │     → resets the TTL countdown
  │
  └─ (repeat 2-3 for all keys, forever)
```

**When the agent dies:**

```
Agent dies
  │
  ├─ No more renew calls
  │
  ├─ 30s later: session "abc-123" expires
  │
  └─ Consul sees Behavior=delete
     → ALL keys acquired by abc-123 are automatically DELETED
     → Edge Traefik sees keys disappear → routes removed
     → Traffic stops going to the dead backend ✅
```

### Key concepts

| Concept | What it does |
|---|---|
| **Session** | A lease with a TTL. Must be renewed periodically or it expires. |
| **TTL** | How long Consul waits after the last renew before killing the session. Uses `HC_DEREGISTER_AFTER` from config. |
| **Renew interval** | How often the agent renews. Uses `HC_INTERVAL` from config. |
| **Behavior=delete** | When session expires, delete all KV keys bound to it (alternative: `release` which just clears the lock but keeps keys). |
| **acquire** | When writing a KV key with `?acquire=<session>`, the key becomes bound to that session. |

### Timing

With default config (`HC_DEREGISTER_AFTER=30s`, `HC_INTERVAL=10s`):

```
 0s   10s   20s   30s
 │     │     │     │
 ├──R──┼──R──┼──?──┤
 │     │     │     │
create ren   ren   expire (if no renew)
              │
         agent dies here → keys deleted at 30s
```

The renew happens every `HC_INTERVAL` (10s) and resets the TTL countdown to `HC_DEREGISTER_AFTER` (30s). There's a comfortable margin.

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `MODE` | `kv` | Operating mode: `kv`, `tags`, or `cluster` |
| `CONSUL_ADDR` | `http://consul:8500` | Consul HTTP API address |
| `NODE_NAME` | hostname | Unique name for this gateway node |
| `TRAEFIK_URL` | *(required¹)* | Traefik API endpoint (single instance) |
| `TRAEFIK_HOST` | *(empty)* | Optional Host header for Traefik API requests |
| `SERVICE` | derived from `TRAEFIK_URL` | L2 Traefik address (host extracted for `:80` / `:443`) |
| `TRAEFIK_INSTANCES` | *(empty)* | JSON array of instances (overrides `TRAEFIK_URL`, see below) |
| `API_PORT` | `8099` | Port for the dashboard/API (cluster mode only) |
| `HC_INTERVAL` | `10s` | Health check + session renew interval |
| `HC_TIMEOUT` | `5s` | Health check timeout |
| `HC_DEREGISTER_AFTER` | `30s` | Deregister timeout + session TTL |
| `RESYNC_SECONDS` | `30` | Full resync interval (seconds) |
| `DEBUG` | `false` | Write debug logs to `debug.log` |

> ¹ `TRAEFIK_URL` is required in `kv`/`tags` modes unless `TRAEFIK_INSTANCES` is set. In `cluster` mode, both are optional (instances can be added via the dashboard).

### Multi-instance configuration (`TRAEFIK_INSTANCES`)

To aggregate multiple Traefik L2 gateways in a single agent, set `TRAEFIK_INSTANCES` to a JSON array:

```bash
TRAEFIK_INSTANCES='[
  {"name": "site-a", "url": "http://traefik-a:8080", "service": "http://192.168.1.10:80"},
  {"name": "site-b", "url": "http://traefik-b:8080", "host": "api.internal"}
]'
```

Each object supports:

| Field | Required | Description |
|---|---|---|
| `name` | yes | Unique instance identifier |
| `url` | yes | Traefik API endpoint |
| `host` | no | Custom `Host` header for the API request |
| `service` | no | L2 address (auto-derived from `url` if omitted) |

---

## Project structure

```
consul_aggregator/
├── __init__.py         # Package marker
├── __main__.py         # Entry point
├── config.py           # Env vars → Config + InstanceConfig
├── consul_client.py    # Consul HTTP client
├── traefik_client.py   # Fetches /api/rawdata
├── normalizer.py       # Pure functions for tag/kv names
├── kv_builder.py       # Rawdata → Consul KV entries
├── tag_builder.py      # Rawdata → Consul service tags
├── sync.py             # Sync engine (multi-instance, sessions)
├── api.py              # REST API (Flask)
└── static/
    └── index.html      # Dashboard micro-frontend

deploy/
├── docker/
│   ├── Dockerfile
│   ├── compose.yml
│   └── test.compose.yaml
└── standalone/
    ├── run.sh
    ├── run.bat
    └── consul-aggregator.service
```

---

## Quick start

### Docker (Cluster mode or Legacy)

```bash
cp .env.template .env
# Edit .env (set TRAEFIK_URL or TRAEFIK_INSTANCES)

docker compose -f deploy/docker/compose.yml up -d --build
# Open http://localhost:8099
```

### Standalone (No Docker)

You can run the agent directly on a Linux or Windows host using the provided runner scripts. They will automatically create a python virtual environment, install dependencies, and start the agent.

```bash
cp .env.template .env

# On Linux / macOS / LXC:
./deploy/standalone/run.sh

# On Windows:
.\deploy\standalone\run.bat
```

> **Note for systemd users**: A ready-to-use template `consul-aggregator.service` is available in the `deploy/standalone/` folder.

### Run locally

```bash
pip install -r requirements.txt
MODE=cluster python -m consul_aggregator
```

---

## Differential sync

On each resync cycle the agent:

1. Fetches fresh `/api/rawdata` from **each** registered L2 Traefik instance
2. Builds the new config (KV entries or tags) per instance
3. **Aggregates** all entries/payloads into a single consolidated set
4. **KV mode**: compares new keys vs previously known keys → deletes stale keys → writes new keys with `acquire`
5. **Tags mode**: re-registers the services (Consul replaces all tags on PUT)
6. Caches the last snapshot for replay if Consul goes down and comes back

> If one instance fails to respond, the others are still synced. The failed instance is logged as a warning.

---

## REST API (cluster mode)

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Dashboard UI |
| `GET` | `/api/status` | Engine status (consul alive, sync count, etc.) |
| `GET` | `/api/instances` | List all registered instances |
| `POST` | `/api/instances` | Add instance (`{name, url, host?, service?}`) |
| `DELETE` | `/api/instances/<name>` | Remove instance |
| `POST` | `/api/resync` | Trigger manual resync |
| `GET` | `/api/config` | Current configuration (read-only) |
| `PATCH` | `/api/config` | Update `resync_seconds` at runtime |

---

## Resilience

| Scenario | Behavior |
|---|---|
| **Consul goes down** | Snapshot is cached. When Consul comes back (detected by health monitor), cached snapshot is replayed. |
| **Traefik L2 unreachable** | Resync fails for that instance only, other instances still sync. Previous config stays in Consul. Logged as warning. |
| **Agent dies (KV mode)** | Session expires after `HC_DEREGISTER_AFTER` → all KV keys deleted automatically. |
| **Agent dies (tags mode)** | Health check fails → service deregistered after `HC_DEREGISTER_AFTER` → tags gone. |
| **Agent restarts** | New session created, old session eventually expires cleaning old keys. New keys written immediately. |
| **Instance added at runtime** | Next resync picks up the new instance. Manual resync via API/dashboard triggers immediately. |
| **Instance removed at runtime** | Stale keys from that instance are cleaned on next sync (differential delete). |
