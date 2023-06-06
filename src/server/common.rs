use std::{
    borrow::Cow,
    net::{IpAddr, Ipv4Addr},
};

use anyhow::{bail, ensure, Context as _, Result};
use axum::{body::Bytes, http::header::CONTENT_TYPE};
use blake3::Hasher;
use jsonrpsee_types::{
    error::{
        INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG, INVALID_PARAMS_CODE, INVALID_REQUEST_CODE,
        INVALID_REQUEST_MSG, PARSE_ERROR_CODE, PARSE_ERROR_MSG,
    },
    ErrorObject, Id, Response, ResponsePayload, TwoPointZero,
};
use once_cell::sync::Lazy;
use redis::{aio::Connection, AsyncCommands, Expiry, FromRedisValue, ToRedisArgs};
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};
use thiserror::Error;
use tokio::time::Instant;

use crate::{
    context::{Context, Node},
    redis::{
        caching::{get_or_compute_coalesced, request_coalescing::RequestCoalescing},
        rate_limit::{rate_limit, RateLimitError},
    },
};

use super::ws::LazySocket;

// https://github.com/MetaMask/eth-rpc-errors/blob/main/src/error-constants.ts
pub const RESOURCE_NOT_FOUND_ERROR_CODE: i32 = -32001;
pub const METHOD_NOT_SUPPORTED_ERROR_CODE: i32 = -32004;
pub const LIMIT_EXCEEDED_ERROR_CODE: i32 = -32005;

pub const APPLICATION_JSON: &str = "application/json";
static NO_REDIS_CONNECTION: &str = "no redis connection";

// Just Bytes but should have JSON content.
pub type JsonBytes = Bytes;

pub fn error_response<Id: Serialize>(id: Id, code: i32, message: &str) -> JsonBytes {
    #[derive(Serialize)]
    struct ErrResponse<'a, Id> {
        jsonrpc: TwoPointZero,
        id: Id,
        error: ErrorObject<'a>,
    }

    serde_json::to_vec(&ErrResponse {
        jsonrpc: TwoPointZero,
        id,
        error: ErrorObject::borrowed(code, &message, None),
    })
    .unwrap()
    .into()
}

pub fn invalid_request_or_parse_error(req: &[u8]) -> JsonBytes {
    match serde_json::from_slice::<Value>(req) {
        Ok(req) => {
            let id = &req["id"];
            let id = if id.is_number() || id.is_string() {
                id
            } else {
                &Value::Null
            };
            error_response(id, INVALID_REQUEST_CODE, INVALID_REQUEST_MSG)
        }
        _ => error_response(&Value::Null, PARSE_ERROR_CODE, PARSE_ERROR_MSG),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallOrNotification<'a> {
    pub jsonrpc: TwoPointZero,
    #[serde(borrow)]
    pub id: Option<Id<'a>>,
    #[serde(borrow)]
    pub method: Cow<'a, str>,
    #[serde(borrow)]
    pub params: Option<&'a RawValue>,
}

impl CallOrNotification<'_> {
    pub fn is_call(&self) -> bool {
        self.id.is_some()
    }
}

pub enum OnSubscription<'a> {
    ErrorHttp,
    ErrorWsBatch,
    ForwardWs(&'a mut LazySocket),
}

/// Result is valid UTF-8.
pub async fn handle_single_request(
    ctx: &Context,
    // Take connection so the same connection can be reused. Getting a
    // connection from the pool is quite expensive.
    mut con: Option<&mut Connection>,
    ip: IpAddr,
    req_bytes: Bytes,
    on_subscription: OnSubscription<'_>,
) -> Option<JsonBytes> {
    ctx.metrics.rpc_requests.inc();
    let start = Instant::now();
    defer! {
        ctx.metrics.rpc_response_time.observe(start.elapsed().as_secs_f64());
    };

    let Ok(req) = serde_json::from_slice::<CallOrNotification>(&req_bytes) else {
        return Some(invalid_request_or_parse_error(&req_bytes));
    };

    let ip = match ip {
        IpAddr::V4(ip) => ip,
        // Just map ipv6 to 0.0.0.0 for now. Proper IPv6 support would be a lot
        // more complicated.
        IpAddr::V6(_ip) => Ipv4Addr::UNSPECIFIED,
    };

    if let Err(err) = rpc_rate_limit(ctx, con.as_deref_mut(), ip, &req.method).await {
        return if req.is_call() {
            let (code, msg) = match err.downcast_ref::<RateLimitError>() {
                Some(err) => {
                    ctx.metrics.rpc_rate_limited.inc();
                    (LIMIT_EXCEEDED_ERROR_CODE, err.to_string())
                }
                None => {
                    log::warn!("rate limit error: {err}");
                    (INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG.into())
                }
            };
            Some(error_response(req.id, code, &msg))
        } else {
            None
        };
    }

    log::info!("{} {}", ip, req.method);

    let is_new_filter = req.method == "eth_newFilter"
        || req.method == "eth_newBlockFilter"
        || req.method == "eth_newPendingTransactionFilter";

    if req.method == "eth_subscribe" || req.method == "eth_unsubscribe" {
        match on_subscription {
            OnSubscription::ErrorWsBatch => {
                return Some(error_response(
                    req.id,
                    METHOD_NOT_SUPPORTED_ERROR_CODE,
                    "subscription in batch request is not supported",
                ));
            }
            OnSubscription::ErrorHttp => {
                return Some(error_response(
                    req.id,
                    METHOD_NOT_SUPPORTED_ERROR_CODE,
                    "subscription over http is not supported",
                ));
            }
            OnSubscription::ForwardWs(upstream) => {
                // Clone id and drop req to make the borrow checker happy.
                let id = serde_json::to_value(&req.id).unwrap_or(Value::Null);
                drop(req);
                // Safety: safe because req_bytes is JSON => req_bytes is utf-8.
                let req_str = unsafe { String::from_utf8_unchecked(req_bytes.into()) };
                // Just forward to upstream socket, the response is forwarded by the ws handling loop.
                if let Err(e) = upstream.send(ctx, req_str.into()).await {
                    // Return an error if connecting/forwarding to upstream fails.
                    return Some(error_response(&id, INTERNAL_ERROR_CODE, &e.to_string()));
                }
                return None;
            }
        }
    }

    // For filter methods, get node from redis. Otherwise choose an RPC node
    // with the configured method.
    let (node, is_sticky_or_hashing) = if req.method == "eth_getFilterChanges"
        || req.method == "eth_getFilterLogs"
        || req.method == "eth_uninstallFilter"
    {
        let filter_id = match get_filter_id(&req) {
            Ok(filter_id) => filter_id,
            Err(e) => {
                return req
                    .is_call()
                    .then(|| error_response(req.id, INVALID_PARAMS_CODE, &e.to_string()))
            }
        };
        match get_node_by_filter_id(ctx, con.as_deref_mut(), &filter_id).await {
            Ok(Some(n)) => (n, true),
            Ok(None) => {
                return req.is_call().then(|| {
                    error_response(req.id, RESOURCE_NOT_FOUND_ERROR_CODE, "filter id not found")
                });
            }
            Err(e) => {
                log::warn!("failed to get filter id: {e}");
                return req
                    .is_call()
                    .then(|| error_response(req.id, INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG));
            }
        }
    } else {
        (ctx.choose_rpc_node(ip), ctx.is_client_ip_hashing_lb())
    };

    if req.is_call() {
        match cache_get_or_compute(ctx, con, node, &req, req_bytes.clone()).await {
            Ok(r) => return Some(r),
            Err(e) => {
                if !e.is::<NotCached>() {
                    log::warn!("failed to cache get or compute: {e}")
                }
            }
        }
    } else if req.method == "eth_call" || req.method == "eth_estimateGas" || is_new_filter {
        // eth_call / eth_estimateGas notifications are ignored.
        //
        // New filter notifications can be ignored because no one will know the
        // filter ID if the filter is created.
        return None;
    }

    // TODO: timeout?
    let result = request(
        ctx,
        node,
        req_bytes.clone(),
        is_new_filter,
        !is_sticky_or_hashing,
    )
    .await;
    if req.is_call() {
        Some(match result {
            Ok(v) => v,
            Err(e) => {
                log::warn!("Error forwarding request: {e}");
                error_response(req.id, INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG)
            }
        })
    } else {
        None
    }
}

/// Full jsonrpc request -> response.
///
/// # Params
///
/// * `is_new_filter`: should be true for requests that creates a new filter,
/// the filter id -> node mapping is saved in redis.
///
/// * `retry_second`: if true, retry requesting a second node if node cannot be
/// connected.
pub async fn request(
    ctx: &Context,
    mut node: &Node,
    req: JsonBytes,
    is_new_filter: bool,
    mut retry_second: bool,
) -> Result<JsonBytes> {
    let response = loop {
        let _in_use = if ctx.is_least_requests_lb() {
            Some(node.in_use())
        } else {
            None
        };
        let result = ctx
            .client
            .post(node.url())
            .header(CONTENT_TYPE, APPLICATION_JSON)
            .body(req.clone())
            .send()
            .await;
        match result {
            Ok(r) => break r.error_for_status()?.bytes().await?,
            // It's safe to retry on timeout error because we are only using
            // connect timeout.
            Err(e) if retry_second && (e.is_connect() || e.is_timeout()) => {
                if ctx.health_check.enabled {
                    node.set_unhealthy();
                }
                if let Some(second) = ctx.choose_second_node(node) {
                    node = second;
                    retry_second = false;
                } else {
                    bail!(e);
                }
            }
            Err(e) => bail!(e),
        }
    };

    if response.is_empty() {
        return Ok(response);
    }

    if is_new_filter {
        let response_json = serde_json::from_slice::<Value>(&response)?;
        if let Some(filter_id) = response_json["result"].as_str() {
            ctx.pool
                .get()
                .await?
                .set_ex(
                    format!("filter:{filter_id}"),
                    node.url(),
                    ctx.filter_ttl_secs,
                )
                .await?;
        }
        Ok(response)
    } else {
        if serde_json::from_slice::<&RawValue>(&response).is_err() {
            if response.len() > 256 {
                bail!("non-JSON response");
            } else {
                let body_str = std::str::from_utf8(&response).unwrap_or("non utf-8 response");
                bail!("non-JSON response: {body_str}");
            }
        }
        Ok(response)
    }
}

// We are not interested in the actual value of quantities, so we just work with
// the hex string.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CallObj<'a> {
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    from: Option<Cow<'a, str>>,

    #[serde(borrow)]
    to: Cow<'a, str>,

    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    gas: Option<Cow<'a, str>>,

    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    gas_price: Option<Cow<'a, str>>,

    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    value: Option<Cow<'a, str>>,

    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    data: Option<Cow<'a, str>>,

    #[serde(default, borrow, skip_serializing_if = "Option::is_none")]
    access_list: Option<Vec<AccessListItem<'a>>>,

    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    max_priority_fee_per_gas: Option<Cow<'a, str>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AccessListItem<'a> {
    #[serde(borrow)]
    address: Cow<'a, str>,
    #[serde(borrow)]
    storage_keys: Vec<Cow<'a, str>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct EthCallParams<'a>(
    #[serde(borrow)] CallObj<'a>,
    #[serde(default, borrow, skip_serializing_if = "Option::is_none")] Option<Cow<'a, str>>,
);

fn parse_eth_call_params(params: Option<&RawValue>) -> Result<EthCallParams<'_>> {
    let params = params.context("no params")?;
    Ok(serde_json::from_str(params.get())?)
}

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
struct CacheKey([u8; 32]);

impl ToRedisArgs for CacheKey {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        const PREFIX: &[u8; 8] = b"caching:";
        let mut buf = [0u8; PREFIX.len() + 32];
        buf[..PREFIX.len()].copy_from_slice(PREFIX);
        buf[PREFIX.len()..].copy_from_slice(&self.0);
        out.write_arg(&buf);
    }
}

#[derive(Clone)]
struct CacheValue(JsonBytes);

impl ToRedisArgs for CacheValue {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        out.write_arg(&self.0);
    }
}

impl FromRedisValue for CacheValue {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let vec: Vec<u8> = Vec::from_redis_value(v)?;
        Ok(Self(vec.into()))
    }
}

#[derive(Error, Debug)]
#[error("method not cached")]
struct NotCached;

pub async fn cache_get_or_compute(
    ctx: &Context,
    con: Option<&mut Connection>,
    node: &Node,
    req: &CallOrNotification<'_>,
    req_bytes: JsonBytes,
) -> Result<JsonBytes> {
    static COALESCING: Lazy<RequestCoalescing<CacheKey, Result<CacheValue, JsonBytes>>> =
        Lazy::new(RequestCoalescing::default);

    let con = con.context(NO_REDIS_CONNECTION)?;

    let cache_key = if ctx.cache.eth_call && req.method == "eth_call"
        || ctx.cache.eth_estimate_gas && req.method == "eth_estimateGas"
    {
        // De-serialize and re-serialize so that cache key is not affected by serialization differences.
        let params = match parse_eth_call_params(req.params) {
            Ok(p) => p,
            Err(e) => return Ok(error_response(&req.id, INVALID_PARAMS_CODE, &e.to_string())),
        };
        let tip = get_tip_block_hash(ctx, node).await?;
        // TODO: what if the tip changes between getting tip block and eth_call?
        // We should probably replace params.1 with actual block hash to
        // mitigate this race condition.
        let params_json = serde_json::value::to_raw_value(&params)?;
        // Use blake3(tip_block_hash || method || params) as cache key.
        let cache_key = Hasher::new()
            .update(tip.as_bytes())
            .update(req.method.as_bytes())
            .update(params_json.get().as_bytes())
            .finalize();
        CacheKey(*cache_key.as_bytes())
    } else {
        bail!(NotCached)
    };
    let mut computed = false;
    let r = get_or_compute_coalesced(
        con,
        &COALESCING,
        cache_key,
        ctx.cache.expire_milliseconds,
        async {
            computed = true;
            request(ctx, node, req_bytes, false, false)
                .await
                .map(CacheValue)
                .map_err(|e| {
                    // Transient errors are not cached.
                    log::warn!("Error forwarding request: {e}");
                    error_response(Id::Null, INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG)
                })
        },
    )
    .await?;
    if computed {
        ctx.metrics.cache_miss.inc();
    } else {
        ctx.metrics.cache_hit.inc();
    }
    let r = match r {
        Ok(r) => r.0,
        Err(e) => e,
    };
    // Change (cached or coalesced) response id to request id.
    let mut r: Value = serde_json::from_slice(&r)?;
    ensure!(r.is_object());
    r["id"] = serde_json::to_value(&req.id)?;
    Ok(r.to_string().into())
}

pub async fn get_tip_block_hash(ctx: &Context, node: &Node) -> Result<String> {
    let _in_use = if ctx.is_least_requests_lb() {
        Some(node.in_use())
    } else {
        None
    };
    let result = ctx
        .client
        .post(node.url())
        .header(CONTENT_TYPE, APPLICATION_JSON)
        .body(
            r#"{"jsonrpc":"2.0","method":"eth_getBlockByNumber","id":1,"params":["latest",false]}"#,
        )
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    #[derive(Deserialize, Clone)]
    struct GetBlockResult {
        #[serde(rename = "blockHash")]
        block_hash: String,
    }
    let result: Response<GetBlockResult> = serde_json::from_slice(&result)?;
    match result.payload {
        ResponsePayload::Result(r) => Ok(r.into_owned().block_hash),
        ResponsePayload::Error(e) => bail!("{}", e),
    }
}

fn get_filter_id(req: &CallOrNotification) -> Result<String> {
    let (filter_id,): (String,) = serde_json::from_str(req.params.context("no params")?.get())?;
    Ok(filter_id)
}

async fn get_node_by_filter_id<'ctx>(
    ctx: &'ctx Context,
    con: Option<&mut Connection>,
    filter_id: &str,
) -> Result<Option<&'ctx Node>> {
    let node: Option<String> = con
        .context(NO_REDIS_CONNECTION)?
        .get_ex(
            format!("filter:{filter_id}"),
            Expiry::EX(ctx.filter_ttl_secs),
        )
        .await?;
    Ok(if let Some(n) = node {
        ctx.get_rpc_node(&n)
    } else {
        None
    })
}

pub async fn rpc_rate_limit(
    ctx: &Context,
    con: Option<&mut Connection>,
    ip: Ipv4Addr,
    method: &str,
) -> Result<()> {
    let con = con.context(NO_REDIS_CONNECTION)?;
    if let Some(ref rl) = ctx.rate_limiting {
        let rl = rl.ip.get(&ip).map(|r| &**r).unwrap_or(rl);

        let total_limit = rl.total;

        rate_limit(
            con,
            format!("rate_limit:{ip}"),
            1,
            60000,
            total_limit.into(),
        )
        .await?;

        if let Some(method_limit) = rl.method.get(method).cloned() {
            rate_limit(
                con,
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
