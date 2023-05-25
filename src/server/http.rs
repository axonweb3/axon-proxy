use axum::{
    body::Bytes,
    body::StreamBody,
    extract::State,
    http::{header::CONTENT_TYPE, StatusCode},
    response::{IntoResponse, Response},
};
use itertools::{chain, intersperse};
use jsonrpsee_types::error::{INVALID_REQUEST_CODE, INVALID_REQUEST_MSG};
use serde_json::{value::RawValue, Value};

use super::{client_ip::ClientIp, common::*};
use crate::context::SharedContext;

fn json_response(json_bytes: JsonBytes) -> Response {
    ([(CONTENT_TYPE, APPLICATION_JSON)], json_bytes).into_response()
}

fn json_arr_response(
    bytes_iter: impl Iterator<Item = JsonBytes> + Send + Sync + 'static,
) -> Response {
    (
        [(CONTENT_TYPE, APPLICATION_JSON)],
        StreamBody::new(futures::stream::iter(
            chain!(
                std::iter::once(Bytes::from_static(b"[")),
                intersperse(bytes_iter, Bytes::from_static(b",")),
                std::iter::once(Bytes::from_static(b"]")),
            )
            .map(anyhow::Ok),
        )),
    )
        .into_response()
}

pub async fn post_handler(
    State(ctx): State<SharedContext>,
    ClientIp(ip): ClientIp,
    body: Bytes,
) -> Response {
    let ctx = ctx.load();
    if let Ok(reqs) = serde_json::from_slice::<Vec<&RawValue>>(&body[..]) {
        if reqs.is_empty() {
            // Empty batch is invalid.
            return json_response(error_response(
                Value::Null,
                INVALID_REQUEST_CODE,
                INVALID_REQUEST_MSG,
            ));
        }

        let mut results = Vec::with_capacity(reqs.len());
        for raw_req in &reqs {
            // Zero copy subslicing!
            let req_bytes = body.slice_ref(raw_req.get().as_bytes());
            if let Some(res) = handle_single_request(&ctx, ip, req_bytes).await {
                results.push(res);
            }
        }
        if !results.is_empty() {
            json_arr_response(results.into_iter())
        } else {
            // If there are no Response objects contained within the Response
            // array as it is to be sent to the client, the server MUST NOT
            // return an empty Array and should return nothing at all.
            StatusCode::NO_CONTENT.into_response()
        }
    } else if let Some(result) = handle_single_request(&ctx, ip, body).await {
        json_response(result)
    } else {
        StatusCode::NO_CONTENT.into_response()
    }
}
