use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::{ensure, Result};
use arc_swap::ArcSwap;
use deadpool_redis::{Pool, Runtime};
use prometheus_client::{
    metrics::{counter::Counter, histogram::Histogram},
    registry::Registry,
};
use reqwest::Client;
use siphasher::sip::SipHasher;

use crate::{
    config::{AddrOrPort, CacheConfig, Config, RateLimit, LB},
    rendezvous_hashing::{weighted_rendezvous_hashing, NodeWithWeight},
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
}

impl Context {
    pub fn from_config(config: Config) -> Result<Self> {
        ensure!(!config.nodes.is_empty());
        let pool = config.redis.create_pool(Some(Runtime::Tokio1))?;
        let ws_nodes = config.ws_nodes;
        let mut rpc_nodes = Vec::with_capacity(config.nodes.len());
        for n in config.nodes {
            let weight = n.weight.unwrap_or(1.);
            ensure!(weight.is_normal() && weight > 0.);
            rpc_nodes.push(Node {
                outstanding_requests: AtomicUsize::new(0),
                url: n.url,
                weight,
            });
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
            lb: config.lb,
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

    pub fn get_rpc_node(&self, node: &str) -> Option<ChosenRpcNode<'_>> {
        let node = self.rpc_nodes.iter().find(|n| n.url == node)?;
        node.outstanding_requests.fetch_add(1, Ordering::Relaxed);
        Some(ChosenRpcNode { node })
    }

    pub fn choose_rpc_node(&self, ip: Ipv4Addr) -> ChosenRpcNode<'_> {
        let node = match self.lb {
            LB::P2cLeastRequests => self.choose_rpc_node_p2c_least_requests(),
            LB::ClientIpHashing => self.choose_rpc_node_by_ip_hashing(ip),
        };
        node.outstanding_requests.fetch_add(1, Ordering::Relaxed);
        ChosenRpcNode { node }
    }

    fn choose_rpc_node_by_ip_hashing(&self, ip: Ipv4Addr) -> &Node {
        let idx = weighted_rendezvous_hashing(&self.rpc_nodes, ip, SipHasher::new()).unwrap();
        &self.rpc_nodes[idx]
    }

    fn choose_rpc_node_p2c_least_requests(&self) -> &Node {
        // For a smaller number of nodes, just use least of all.
        if self.rpc_nodes.len() < 4 {
            self.rpc_nodes
                .iter()
                .min_by_key(|n| n.outstanding_requests.load(Ordering::Relaxed))
                .unwrap()
        } else {
            // P2C least requests.
            random_choose_two(&self.rpc_nodes)
                .into_iter()
                .min_by_key(|n| n.outstanding_requests.load(Ordering::Relaxed))
                .unwrap()
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
    weight: f64,
}

impl<'a> NodeWithWeight for &'a Node {
    type T = &'a str;
    fn tag(&self) -> Self::T {
        &self.url
    }
    fn weight(&self) -> f64 {
        self.weight
    }
}

impl Node {
    pub fn url(&self) -> &str {
        &self.url
    }
}

pub struct ChosenRpcNode<'a> {
    node: &'a Node,
}

impl ChosenRpcNode<'_> {
    pub fn url(&self) -> &str {
        self.node.url()
    }
}

impl<'a> Drop for ChosenRpcNode<'a> {
    fn drop(&mut self) {
        self.node
            .outstanding_requests
            .fetch_sub(1, Ordering::Relaxed);
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
/// If x.len() < 2.
fn random_choose_two<T>(x: &[T]) -> [&T; 2] {
    let rng = fastrand::Rng::new();
    let idx0 = rng.usize(0..x.len() - 1);
    let mut idx1 = rng.usize(0..x.len());
    if idx1 == idx0 {
        idx1 = x.len() - 1;
    }
    [&x[idx0], &x[idx1]]
}
