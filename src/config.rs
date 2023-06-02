use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub nodes: Vec<Node>,
    pub ws_nodes: Vec<String>,
    /// Does not support reloading.
    pub bind: AddrOrPort,
    pub redis: deadpool_redis::Config,
    pub rate_limit: Option<RateLimit>,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default = "default_filter_ttl_secs")]
    pub filter_ttl_secs: usize,
    #[serde(default)]
    pub lb: LB,
}

fn default_filter_ttl_secs() -> usize {
    90
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum LB {
    #[default]
    P2cLeastRequests,
    ClientIpHashing,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum AddrOrPort {
    Port(u16),
    Addr(SocketAddr),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Node {
    /// E.g. http://host:80/
    pub url: String,
    /// Rpc load balancing weight. Default 1.0.
    ///
    /// Has no effect when lb is p2c_least_requests.
    pub weight: Option<f64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct RateLimit {
    /// Limit per IP per minute.
    pub total: u32,
    /// Limit per IP per minute of some method.
    #[serde(default)]
    pub method: HashMap<String, u32>,
    /// Specific setttings for IPs.
    #[serde(default)]
    pub ip: HashMap<Ipv4Addr, Box<RateLimit>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, default)]
pub struct CacheConfig {
    pub expire_milliseconds: u64,
    pub eth_call: bool,
    #[serde(rename = "eth_estimateGas")]
    pub eth_estimate_gas: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            expire_milliseconds: 30_000,
            eth_call: false,
            eth_estimate_gas: false,
        }
    }
}
