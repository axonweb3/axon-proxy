use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use anyhow::{ensure, Result};
use arc_swap::ArcSwap;
use deadpool_redis::{Pool, Runtime};
use prometheus_client::{
    metrics::{counter::Counter, histogram::Histogram},
    registry::Registry,
};
use rand::{thread_rng, Rng};
use reqwest::Client;
use siphasher::sip::SipHasher;

use crate::{
    config::{AddrOrPort, CacheConfig, Config, RateLimit},
    redis::rate_limit::rate_limit,
    rendezvous_hashing::WeightedRendezvousHashing,
};

pub type SharedContext = Arc<ArcSwap<Context>>;

#[derive(Clone)]
pub struct Metrics {
    pub http_requests: Counter,
    pub ws_accepted: Counter,
    pub rpc_requests: Counter,
    /// Unit: seconds.
    pub rpc_response_time: Histogram,
    pub cache_hit: Counter,
    pub cache_miss: Counter,
    pub rpc_rate_limited: Counter,
    pub ws_message_sent: Counter,
    pub ws_message_received: Counter,
}

pub struct Context {
    pub bind: SocketAddr,
    pub rpc_nodes: WeightedRendezvousHashing<SipHasher, String>,
    pub ws_nodes: Vec<String>,
    // Use reqwest/hyper connection pooling for now.
    pub client: Client,
    pub pool: Pool,
    pub rate_limiting: Option<RateLimit>,
    pub cache: CacheConfig,
    pub metrics_registry: Arc<Registry>,
    pub metrics: Metrics,
    pub filter_ttl_secs: usize,
}

impl Context {
    pub fn from_config(config: Config) -> Result<Self> {
        ensure!(!config.nodes.is_empty());
        let pool = config.redis.create_pool(Some(Runtime::Tokio1))?;
        let ws_nodes = config.nodes.iter().flat_map(|n| n.ws.clone()).collect();
        let mut rpc_nodes = WeightedRendezvousHashing::new(SipHasher::new());
        for n in config.nodes {
            rpc_nodes.add(n.rpc, n.weight.unwrap_or(1.));
        }
        let bind = match config.bind {
            AddrOrPort::Port(p) => (Ipv4Addr::UNSPECIFIED, p).into(),
            AddrOrPort::Addr(a) => a,
        };
        let client = Client::builder().pool_max_idle_per_host(100).build()?;
        let mut metrics_registry = Registry::default();
        let metrics = Metrics::new();
        metrics.register(&mut metrics_registry);
        Ok(Self {
            bind,
            rpc_nodes,
            ws_nodes,
            client,
            pool,
            rate_limiting: config.rate_limit,
            cache: config.cache,
            metrics_registry: Arc::new(metrics_registry),
            metrics,
            filter_ttl_secs: config.filter_ttl_secs,
        })
    }

    /// Create a new context, reusing previous client and metrics.
    pub fn from_config_and_previous(config: Config, previous: &Self) -> Result<Self> {
        let new = Self::from_config(config)?;
        Ok(Self {
            client: previous.client.clone(),
            metrics_registry: previous.metrics_registry.clone(),
            metrics: previous.metrics.clone(),
            ..new
        })
    }

    pub async fn rate_limit(&self, ip: Ipv4Addr, method: &str) -> Result<()> {
        if let Some(ref rl) = self.rate_limiting {
            let rl = rl.ip.get(&ip).map(|r| &**r).unwrap_or(rl);

            let total_limit = rl.total;

            let mut con = self.pool.get().await?;
            rate_limit(
                &mut con,
                format!("rate_limit:{ip}"),
                1,
                60000,
                total_limit.into(),
            )
            .await?;

            if let Some(method_limit) = rl.method.get(method).cloned() {
                rate_limit(
                    &mut con,
                    format!("rate_limit:{ip}:{method}"),
                    1,
                    60000,
                    method_limit.into(),
                )
                .await?;
            }
        }
        Ok(())
    }

    pub fn choose_rpc_node(&self, ip: Ipv4Addr) -> &str {
        self.rpc_nodes.choose(ip).unwrap()
    }

    pub fn choose_ws_node(&self) -> Option<&str> {
        if !self.ws_nodes.is_empty() {
            Some(&self.ws_nodes[thread_rng().gen_range(0..self.ws_nodes.len())])
        } else {
            None
        }
    }
}

impl Metrics {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        // // Default values from go client(https://github.com/prometheus/client_golang/blob/5d584e2717ef525673736d72cd1d12e304f243d7/prometheus/histogram.go#L68)
        let custom_buckets = [
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];
        Self {
            cache_hit: Default::default(),
            cache_miss: Default::default(),
            http_requests: Default::default(),
            rpc_rate_limited: Default::default(),
            rpc_requests: Default::default(),
            ws_accepted: Default::default(),
            rpc_response_time: Histogram::new(custom_buckets.into_iter()),
            ws_message_sent: Default::default(),
            ws_message_received: Default::default(),
        }
    }

    pub fn register(&self, registry: &mut Registry) {
        registry.register("cache_hit", "Cache hit", self.cache_hit.clone());
        registry.register("cache_miss", "Cache miss", self.cache_miss.clone());
        registry.register("http_requests", "Http requests", self.http_requests.clone());
        registry.register("rpc_requests", "RPC requests", self.rpc_requests.clone());
        registry.register(
            "rpc_rate_limited",
            "RPC requests rate limited",
            self.rpc_rate_limited.clone(),
        );
        registry.register(
            "ws_acceptted",
            "WebSocket connections accepted",
            self.ws_accepted.clone(),
        );
        registry.register(
            "rpc_response_time",
            "RPC response time",
            self.rpc_response_time.clone(),
        );
        registry.register(
            "ws_message_sent",
            "Sent websocket messages",
            self.ws_message_sent.clone(),
        );
        registry.register(
            "ws_message_received",
            "Received websocket messages",
            self.ws_message_received.clone(),
        );
    }
}
