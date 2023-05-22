mod client_ip;

use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Result;
use axum::{
    extract::{OriginalUri, State},
    http::HeaderMap,
    response::IntoResponse,
    routing::*,
    Json,
};
use serde_json::{json, Map, Value};
use tokio::signal::unix::{signal, SignalKind};
use tower_http::{compression::CompressionLayer, cors::CorsLayer, timeout::TimeoutLayer};

use crate::context::Context;

use self::client_ip::ClientIp;

pub async fn post_handler(
    _ctx: State<Arc<Context>>,
    _ip: ClientIp,
    _req: Json<Value>,
) -> impl IntoResponse {
    "TODO"
}

pub async fn ws_handler(_ctx: State<Arc<Context>>, _ip: ClientIp) -> impl IntoResponse {
    "TODO"
}

pub async fn metrics_handler(_ctx: State<Arc<Context>>) -> impl IntoResponse {
    "TODO"
}

pub async fn debug(uri: OriginalUri, headers: HeaderMap, ip: ClientIp) -> impl IntoResponse {
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

pub async fn serve(ctx: Arc<Context>) -> Result<()> {
    let bind = ctx.bind;
    let router = axum::Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/", post(post_handler).get(ws_handler))
        .route("/*", post(post_handler).get(ws_handler))
        .route("/__debug", get(debug))
        .with_state(ctx)
        .layer(CompressionLayer::new())
        .layer(TimeoutLayer::new(Duration::from_secs(30)))
        .layer(CorsLayer::permissive().max_age(Duration::from_secs(300)));
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
