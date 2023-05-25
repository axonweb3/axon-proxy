mod client_ip;
mod common;
mod http;
mod ws;

use std::{future::Future, net::SocketAddr, time::Duration};

use anyhow::Result;
use axum::{
    extract::{OriginalUri, State},
    http::HeaderMap,
    response::IntoResponse,
    routing::*,
    Json,
};
use serde_json::{json, Map};
use tokio::signal::unix::{signal, SignalKind};
use tower_http::{compression::CompressionLayer, cors::CorsLayer, timeout::TimeoutLayer};

use self::{client_ip::ClientIp, http::*, ws::*};
use crate::context::SharedContext;

pub async fn serve(ctx: SharedContext) -> Result<()> {
    let bind = ctx.load().bind;
    let router = axum::Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/", post(post_handler).get(ws_handler))
        .route("/*path", post(post_handler).get(ws_handler))
        .route("/__debug", get(debug))
        .with_state(ctx)
        .layer(CompressionLayer::new())
        .layer(TimeoutLayer::new(Duration::from_secs(30)))
        .layer(CorsLayer::permissive().max_age(Duration::from_secs(300)));
    log::info!("binding to {}", bind);
    axum::Server::bind(&bind)
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .tcp_keepalive_interval(Some(Duration::from_secs(10)))
        .tcp_keepalive_retries(Some(3))
        .serve(router.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(sigint_or_sigterm()?)
        .await?;
    Ok(())
}

async fn metrics_handler(State(_ctx): State<SharedContext>) -> impl IntoResponse {
    todo!()
}

async fn debug(uri: OriginalUri, headers: HeaderMap, ip: ClientIp) -> impl IntoResponse {
    let headers = Map::from_iter(
        headers
            .into_iter()
            .filter_map(|(n, v)| Some((n?.to_string(), v.to_str().ok()?.into()))),
    );
    Json(json!({
        "uri": uri.0.to_string(),
        "headers": headers,
        "ip": ip.0,
    }))
}

fn sigint_or_sigterm() -> Result<impl Future<Output = ()>> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = sigint.recv() => { log::info!("received SIGINT") }
            _ = sigterm.recv() => { log::info!("received SIGTERM") }
        };
    })
}
