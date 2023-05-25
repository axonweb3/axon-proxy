use std::{
    borrow::Cow,
    net::{IpAddr, Ipv4Addr},
};

use anyhow::{bail, Result};
use axum::{body::Bytes, http::header::CONTENT_TYPE};
use jsonrpsee_types::{
    error::{
        INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG, INVALID_REQUEST_CODE, INVALID_REQUEST_MSG,
        PARSE_ERROR_CODE, PARSE_ERROR_MSG,
    },
    ErrorObject, Id, TwoPointZero,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};

use crate::{context::Context, redis::rate_limit::RateLimitError};

pub const RATE_LIMIT_ERROR_CODE: i32 = -37001;

pub const APPLICATION_JSON: &str = "application/json";

// Just Bytes but should have JSON content.
pub type JsonBytes = Bytes;

#[derive(Serialize)]
struct ErrResponse<'a, Id> {
    jsonrpc: TwoPointZero,
    id: Id,
    error: ErrorObject<'a>,
}

pub fn error_response<Id: Serialize>(id: Id, code: i32, message: &str) -> JsonBytes {
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

pub async fn handle_single_request(
    ctx: &Context,
    ip: IpAddr,
    req_bytes: Bytes,
) -> Option<JsonBytes> {
    let Ok(req) = serde_json::from_slice::<CallOrNotification>(&req_bytes) else {
        return Some(invalid_request_or_parse_error(&req_bytes));
    };

    let ip = match ip {
        IpAddr::V4(ip) => ip,
        // Just map ipv6 to 0.0.0.0 for now. Proper IPv6 support would be a lot
        // more complicated.
        IpAddr::V6(_ip) => Ipv4Addr::UNSPECIFIED,
    };

    if let Err(err) = ctx.rate_limit(ip, &req.method).await {
        return if req.is_call() {
            let (code, msg) = match err.downcast_ref::<RateLimitError>() {
                Some(err) => (RATE_LIMIT_ERROR_CODE, err.to_string()),
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

    if let Ok(Some(r)) = get_cached(ctx, &req).await {
        return req.is_call().then_some(r);
    }

    // TODO: retry, circuit breaker, other LBs.
    let node = ctx.choose_rpc_node(ip);
    let result = request(&ctx.client, node, req_bytes.clone()).await;
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

pub async fn request(client: &Client, url: &str, req: Bytes) -> Result<Bytes> {
    let response = client
        .post(url)
        .header(CONTENT_TYPE, APPLICATION_JSON)
        .body(req)
        .send()
        .await?
        .error_for_status()?;
    let is_json = response
        .headers()
        .get(CONTENT_TYPE)
        .map_or(false, |t| t == "application/json");
    let body = response.bytes().await?;
    if body.is_empty() || is_json {
        // Trust the server to return valid json when content type is JSON.
        return Ok(body);
    }
    // Maybe the server forget to set content type.
    if serde_json::from_slice::<&RawValue>(&body).is_err() {
        if body.len() > 256 {
            bail!("non-JSON response");
        } else {
            let body_str = std::str::from_utf8(&body).unwrap_or("non-utf8 body");
            bail!("non-JSON response: {body_str}");
        }
    }
    Ok(body)
}

pub async fn get_cached(
    _ctx: &Context,
    _req: &CallOrNotification<'_>,
) -> Result<Option<JsonBytes>> {
    // TODO
    Ok(None)
}
