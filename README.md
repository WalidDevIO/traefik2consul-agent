# Consul Aggregator

Bridge between **Traefik** Layer 2 reverse proxies and a **Consul**-backed **Traefik Edge** (Layer 1).

The agent watches one or more internal Traefik instances via their `/api/rawdata` endpoint, extracts the live routing configuration (routers, middlewares), and publishes it to Consul so that the edge Traefik can serve it.

## Architecture

```
                    Internet
                       │
               ┌───────▼───────┐
               │  Edge Traefik │  (L1) ─ public-facing
               │  consul prov. │
               └───┬───────┬───┘
                   │       │
          ┌────────▼──┐ ┌──▼────────┐
          │  gw-http  │ │ gw-https  │   ◄── Consul services (per node)
          │  :80      │ │ :443      │
          └────┬──────┘ └──────┬────┘
               │               │
         ┌─────▼───────────────▼─────┐
         │     Gateway Traefik (L2)  │  ─ internal, per-node
         │     rawdata API :8080     │
         └─────────▲─────────────────┘
                   │ GET /api/rawdata
         ┌─────────┴─────────────────┐
         │   consul_aggregator       │  ◄── this agent
         │   (runs alongside L2)     │
         └─────────┬─────────────────┘
                   │ PUT /v1/kv/...  or  PUT /v1/agent/service/register
                   ▼
              ┌──────────┐
              │  Consul  │
              └──────────┘
```

### Entrypoint splitting

Each L2 gateway registers **two services** in Consul:

| Service | Port | Routers with entrypoint |
|---|---|---|
| `gw-<NODE>-http` | 80 | `web` |
| `gw-<NODE>-https` | 443 | `websecure` (tls=true) |

When a router has **both** `web` and `websecure` entrypoints, it is split into two edge routers:
- `<name>_web` → routes to `gw-<NODE>-http`
- `<name>_websecure` → routes to `gw-<NODE>-https` with `tls=true`

This ensures the edge Traefik knows exactly which port and protocol to use.

---

## Modes

The agent supports two operating modes, controlled by the `MODE` env var:

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
| `MODE` | `kv` | Operating mode: `kv` or `tags` |
| `CONSUL_ADDR` | `http://consul:8500` | Consul HTTP API address |
| `NODE_NAME` | hostname | Unique name for this gateway node |
| `TRAEFIK_URL` | *(required)* | Traefik API endpoint (e.g. `http://traefik:8080`) |
| `TRAEFIK_HOST` | *(empty)* | Optional Host header for Traefik API requests |
| `SERVICE` | derived from `TRAEFIK_URL` | L2 Traefik address (host extracted for `:80` / `:443`) |
| `HC_INTERVAL` | `10s` | Health check + session renew interval |
| `HC_TIMEOUT` | `5s` | Health check timeout |
| `HC_DEREGISTER_AFTER` | `30s` | Deregister timeout + session TTL |
| `RESYNC_SECONDS` | `30` | Full resync interval (seconds) |
| `DEBUG` | `false` | Write debug logs to `debug.log` |

---

## Project structure

```
consul_aggregator/
├── __init__.py         # Package marker
├── __main__.py         # Entry point — wires components by mode
├── config.py           # Env vars → typed Config dataclass
├── consul_client.py    # Consul HTTP client (services, KV, sessions)
├── traefik_client.py   # Fetches /api/rawdata from Traefik
├── normalizer.py       # Pure functions: sanitize, flatten, normalize
├── kv_builder.py       # Rawdata → Consul KV entries (mode=kv)
├── tag_builder.py      # Rawdata → Consul service tags (mode=tags)
└── sync.py             # Sync engine: differential updates, sessions, cache
```

---

## Quick start

```bash
# Copy and edit config
cp .env.template .env

# Run with Docker
docker build -t consul-aggregator .
docker run --env-file .env consul-aggregator

# Or run locally
pip install requests
python -m consul_aggregator
```

---

## Differential sync

On each resync cycle the agent:

1. Fetches fresh `/api/rawdata` from the L2 Traefik
2. Builds the new config (KV entries or tags)
3. **KV mode**: compares new keys vs previously known keys → deletes stale keys → writes new keys with `acquire`
4. **Tags mode**: re-registers the services (Consul replaces all tags on PUT)
5. Caches the last snapshot for replay if Consul goes down and comes back

---

## Resilience

| Scenario | Behavior |
|---|---|
| **Consul goes down** | Snapshot is cached. When Consul comes back (detected by health monitor), cached snapshot is replayed. |
| **Traefik L2 unreachable** | Resync fails, previous config stays in Consul. Logged as warning. |
| **Agent dies (KV mode)** | Session expires after `HC_DEREGISTER_AFTER` → all KV keys deleted automatically. |
| **Agent dies (tags mode)** | Health check fails → service deregistered after `HC_DEREGISTER_AFTER` → tags gone. |
| **Agent restarts** | New session created, old session eventually expires cleaning old keys. New keys written immediately. |
