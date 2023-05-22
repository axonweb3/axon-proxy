use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub nodes: Vec<Node>,
    /// Consistent hashing key. Should have 128 bit of entropy.
    pub hash_key: String,
    pub bind: AddrOrPort,
    pub redis: deadpool_redis::Config,
    pub rate_limiting: Option<RateLimiting>,
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
    /// Rpc load balancing weight. Does not affect ws.
    pub weight: Option<f64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct RateLimiting {
    /// Limit per IP per minute.
    pub total: u32,
    /// Limit per IP per minute of some method.
    pub methods: HashMap<String, u32>,
    /// Specific setttings for IPs.
    pub ips: HashMap<Ipv4Addr, Box<RateLimiting>>,
}
