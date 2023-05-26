use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use anyhow::{ensure, Result};
use arc_swap::ArcSwap;
use deadpool_redis::{Pool, Runtime};
use rand::{thread_rng, Rng};
use reqwest::Client;
use siphasher::sip::SipHasher;

use crate::{
    config::{AddrOrPort, CacheConfig, Config, RateLimit},
    redis::rate_limit::rate_limit,
    rendezvous_hashing::WeightedRendezvousHashing,
};

pub type SharedContext = Arc<ArcSwap<Context>>;

pub struct Context {
    pub bind: SocketAddr,
    pub rpc_nodes: WeightedRendezvousHashing<SipHasher, String>,
    pub ws_nodes: Vec<String>,
    // Use reqwest/hyper connection pooling for now.
    pub client: Client,
    pub pool: Pool,
    pub rate_limiting: Option<RateLimit>,
    pub cache: CacheConfig,
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
        Ok(Self {
            bind,
            rpc_nodes,
            ws_nodes,
            client,
            pool,
            rate_limiting: config.rate_limit,
            cache: config.cache,
        })
    }

    /// Create a new context, reuse previous client.
    pub fn from_config_and_previous(config: Config, previous: &Self) -> Result<Self> {
        let new = Self::from_config(config)?;
        Ok(Self {
            client: previous.client.clone(),
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
