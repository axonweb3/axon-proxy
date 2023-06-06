mod client_ip;
pub mod common;
mod http;
mod ws;

use std::{future::Future, net::SocketAddr, time::Duration};

use anyhow::Result;
use axum::{
    extract::{OriginalUri, State},
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::*,
    Json,
};
use prometheus_client::registry::Registry;
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
        .layer(
            CorsLayer::permissive()
                .vary([])
                .max_age(Duration::from_secs(300)),
        );
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

async fn metrics_handler(State(ctx): State<SharedContext>) -> impl IntoResponse {
    let ctx = ctx.load();
    ctx.metrics.http_requests.inc();

    // Create a new registry to encode the metrics. Unfortunately
    // prometheus-client doesn't have a public encoding API like
    // fn encode(w: impl Writer, desc: &Descriptor, metric: impl EncodeMetric).
    let mut registry = Registry::default();
    ctx.metrics.register(&mut registry);
    ctx.register_self_metrics(&mut registry);

    let mut buf = String::with_capacity(4096);
    if let Err(err) = prometheus_client::encoding::text::encode(&mut buf, &registry) {
        log::warn!("failed to scrape metrics: {err}");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    (
        [(
            CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        buf,
    )
        .into_response()
}

async fn debug(
    State(ctx): State<SharedContext>,
    uri: OriginalUri,
    headers: HeaderMap,
    ip: ClientIp,
) -> impl IntoResponse {
    let ctx = ctx.load();
    ctx.metrics.http_requests.inc();

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
