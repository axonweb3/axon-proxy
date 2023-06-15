A reverse proxy / load balancer for axon.

# Features

- HTTP and WebSocket. Subscription is supported on WebSocket connections.
- P2C least requests or client ip consistent hashing load balancing.
- Filter id sticky load balancing.
- Health check and failover.
- Per-ip (and per-ip-per-method) rate limiting with redis.
- Caching with redis.
- High availability: running multiple instances of this proxy is supported.
- Metrics.

# Building

Install rust and

```command
$ cargo build --release
```

A dockerfile is also provided.

# Running

```command
$ axon-proxy -c config.toml
```

Specify the number of worker threads with `--threads` or `AXON_PROXY_THREADS`:

```command
$ axon-proxy --threads 8 -c config.toml
```

The default is to use the same number of threads as the number of CPU cores.

# Configuration

```toml
bind = 8000
redis.url = "redis://127.0.0.1/"
# Upstream WebSocket connections are only used for subscription. Random LB is used for now.
ws_nodes = ["ws://127.0.0.1:3001/ws", "ws://127.0.0.2:3001/ws"]
# Default is p2c_least_requests
# lb = "client_ip_hashing"

[[nodes]]
url = "http://127.0.0.1:3000/"

[[nodes]]
url = "http://127.0.0.2:3000/"
# Weight is only effective if lb is client_ip_hashing.
weight = 0.5

# Health check is enabled by default.
# [health_check]
# enabled = true
# timeout_secs = 3
# interval_secs = 3
# concurrency = 8

# Default is no caching.
[cache]
eth_call = true
eth_estimateGas = true

# Default is no rate limit.
# [rate_limit]
# total = 3000
# method."eth_call" = 600

# Override rate limit settings for specific client ips.
# Method rate limits are NOT inherited.
# [rate_limit.ip."127.0.0.1"]
# total = 10000
# method."eth_call" = 2000
```
