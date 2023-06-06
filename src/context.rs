use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use anyhow::{ensure, Result};
use arc_swap::ArcSwap;
use deadpool_redis::{Pool, Runtime};
use futures::StreamExt;
use prometheus_client::{
    metrics::{counter::Counter, histogram::Histogram},
    registry::Registry,
};
use reqwest::Client;
use siphasher::sip::SipHasher;

use crate::{
    config::{AddrOrPort, CacheConfig, Config, HealthCheckConfig, RateLimit, LB},
    rendezvous_hashing::{weighted_rendezvous_hashing, NodeWithWeight},
    server::common::get_tip_block_hash,
};

pub type SharedContext = Arc<ArcSwap<Context>>;

pub struct Context {
    pub bind: SocketAddr,
    pub rpc_nodes: Vec<Node>,
    pub ws_nodes: Vec<String>,
    // Use reqwest/hyper connection pooling for now.
    pub client: Client,
    pub pool: Pool,
    pub rate_limiting: Option<RateLimit>,
    pub cache: CacheConfig,
    pub metrics_registry: Arc<Registry>,
    pub metrics: Metrics,
    pub filter_ttl_secs: usize,
    pub lb: LB,
    pub health_check: HealthCheckConfig,
}

impl Context {
    pub fn from_config(config: Config) -> Result<Arc<Self>> {
        let ctx = Arc::new(Self::from_config_inner(config)?);

        if ctx.health_check.enabled {
            tokio::spawn(health_check(Arc::downgrade(&ctx)));
        }

        Ok(ctx)
    }

    /// Create a new context, reusing previous client and metrics.
    pub fn from_config_and_previous(config: Config, previous: &Self) -> Result<Arc<Self>> {
        let new = Self::from_config_inner(config)?;
        let ctx = Arc::new(Self {
            client: previous.client.clone(),
            metrics_registry: previous.metrics_registry.clone(),
            metrics: previous.metrics.clone(),
            ..new
        });
        if ctx.health_check.enabled {
            tokio::spawn(health_check(Arc::downgrade(&ctx)));
        }
        Ok(ctx)
    }

    fn from_config_inner(config: Config) -> Result<Self> {
        ensure!(!config.nodes.is_empty());
        let pool = config.redis.create_pool(Some(Runtime::Tokio1))?;
        let ws_nodes = config.ws_nodes;
        let mut rpc_nodes = Vec::with_capacity(config.nodes.len());
        for n in config.nodes {
            let weight = n.weight.unwrap_or(1.);
            ensure!(weight.is_normal() && weight > 0.);
            if matches!(config.lb, LB::P2cLeastRequests) && weight != 1. {
                log::warn!("Node weight has no effect when lb is p2c_least_requests");
            }
            rpc_nodes.push(Node {
                outstanding_requests: AtomicUsize::new(0),
                url: n.url,
                healthy: AtomicBool::new(true),
                weight,
            });
        }
        let bind = match config.bind {
            AddrOrPort::Port(p) => (Ipv4Addr::UNSPECIFIED, p).into(),
            AddrOrPort::Addr(a) => a,
        };
        // Note: don't add whole request timeout unless you understand how
        // that'll affect the retry mechanism.
        let client = Client::builder()
            .pool_max_idle_per_host(100)
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs.into()))
            .build()?;
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
            lb: config.lb,
            health_check: config.health_check,
        })
    }

    pub fn get_rpc_node(&self, node: &str) -> Option<&Node> {
        let node = self.rpc_nodes.iter().find(|n| n.url == node)?;
        Some(node)
    }

    pub fn is_least_requests_lb(&self) -> bool {
        matches!(self.lb, LB::P2cLeastRequests)
    }

    pub fn is_client_ip_hashing_lb(&self) -> bool {
        matches!(self.lb, LB::ClientIpHashing)
    }

    pub fn choose_rpc_node(&self, ip: Ipv4Addr) -> &Node {
        match self.lb {
            LB::P2cLeastRequests => self.choose_rpc_node_p2c_least_requests(),
            LB::ClientIpHashing => self.choose_rpc_node_by_ip_hashing(ip),
        }
    }

    fn choose_rpc_node_by_ip_hashing(&self, ip: Ipv4Addr) -> &Node {
        // Unhealthy nodes are very unlikely to be chosen if there are healthy
        // nodes because their weight is very low.
        weighted_rendezvous_hashing(&self.rpc_nodes, ip, SipHasher::new()).unwrap()
    }

    fn choose_rpc_node_p2c_least_requests(&self) -> &Node {
        let healthy: Vec<&Node> = self
            .rpc_nodes
            .iter()
            .filter(|n| n.healthy.load(Ordering::Relaxed))
            .collect();
        let two: [&Node; 2] = match healthy.len() {
            0 => {
                // No healthy nodes, pick from unhealthy.
                let [idx0, idx1] = random_two_indexes(self.rpc_nodes.len());
                [&self.rpc_nodes[idx0], &self.rpc_nodes[idx1]]
            }
            1 => return healthy[0],
            2 => healthy[..].try_into().unwrap(),
            l => {
                let [idx0, idx1] = random_two_indexes(l);
                [healthy[idx0], healthy[idx1]]
            }
        };
        two.into_iter()
            .min_by_key(|n| n.outstanding_requests.load(Ordering::Relaxed))
            .unwrap()
    }

    /// Randomly choose an RPC node that is different from the first.
    pub fn choose_second_node(&self, first: &Node) -> Option<&Node> {
        let healthy: Vec<&Node> = self
            .rpc_nodes
            .iter()
            // Pointer comparison is enough because first is expected to be one of self.rpc_nodes too.
            .filter(|n| std::ptr::eq(*n, first) && n.healthy.load(Ordering::Relaxed))
            .collect();
        if !healthy.is_empty() {
            Some(healthy[fastrand::usize(0..healthy.len())])
        } else {
            None
        }
    }

    pub fn choose_ws_node(&self) -> Option<&str> {
        if !self.ws_nodes.is_empty() {
            Some(&self.ws_nodes[fastrand::usize(0..self.ws_nodes.len())])
        } else {
            None
        }
    }
}

pub struct Node {
    url: String,
    outstanding_requests: AtomicUsize,
    healthy: AtomicBool,
    weight: f64,
}

impl<'a> NodeWithWeight for &'a Node {
    type T = &'a str;
    fn tag(&self) -> Self::T {
        &self.url
    }
    fn weight(&self) -> f64 {
        if self.healthy.load(Ordering::Relaxed) {
            self.weight
        } else {
            self.weight / 100000.
        }
    }
}

impl Node {
    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn set_unhealthy(&self) {
        self.healthy.store(false, Ordering::Relaxed);
    }

    pub fn in_use(&self) -> impl Drop + '_ {
        self.outstanding_requests.fetch_add(1, Ordering::Relaxed);
        scopeguard::guard(self, |n| {
            n.outstanding_requests.fetch_sub(1, Ordering::Relaxed);
        })
    }
}

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

/// Floyd's Algorithm: https://fermatslibrary.com/s/a-sample-of-brilliance
///
/// # Panics
///
/// If len < 2.
fn random_two_indexes(len: usize) -> [usize; 2] {
    let rng = fastrand::Rng::new();
    let idx0 = rng.usize(0..len - 1);
    let mut idx1 = rng.usize(0..len);
    if idx1 == idx0 {
        idx1 = len - 1;
    }
    [idx0, idx1]
}

async fn health_check(ctx: Weak<Context>) {
    while let Some(ctx) = ctx.upgrade() {
        futures::stream::iter(&ctx.rpc_nodes)
            .for_each_concurrent(ctx.health_check.concurrency as usize, |n| async {
                let result = tokio::time::timeout(
                    Duration::from_secs(ctx.health_check.timeout_secs.into()),
                    get_tip_block_hash(&ctx, n),
                )
                .await;
                let healthy = matches!(result, Ok(Ok(_)));
                n.healthy.store(healthy, Ordering::Relaxed);
            })
            .await;
        tokio::time::sleep(Duration::from_secs(ctx.health_check.interval_secs.into())).await;
    }
}
