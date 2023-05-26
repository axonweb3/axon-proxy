use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub nodes: Vec<Node>,
    /// Does not support reloading.
    pub bind: AddrOrPort,
    pub redis: deadpool_redis::Config,
    pub rate_limit: Option<RateLimit>,
    #[serde(default)]
    pub cache: CacheConfig,
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
    pub rpc: String,
    /// E.g. ws://host:80/
    pub ws: Option<String>,
    /// Rpc load balancing weight. Default 1.0. Does not affect ws.
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
