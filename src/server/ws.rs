use std::{net::IpAddr, time::Duration};

use anyhow::{Context as _, Result};
use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
};
use futures::prelude::*;
use itertools::intersperse;
use jsonrpsee_types::error::{INVALID_REQUEST_CODE, INVALID_REQUEST_MSG};
use serde_json::value::{RawValue, Value};
use tokio::{net::TcpStream, time::Instant};
use tokio_tungstenite::{tungstenite, MaybeTlsStream, WebSocketStream};

use super::{
    client_ip::ClientIp,
    common::{JsonBytes, OnSubscription},
};
use crate::{
    context::{Context, SharedContext},
    server::common::{error_response, handle_single_request},
};

pub async fn ws_handler(
    State(ctx): State<SharedContext>,
    ClientIp(ip): ClientIp,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.max_send_queue(8)
        // 2MB, same as http body limit.
        .max_message_size(2 * 1024 * 1024)
        .on_upgrade(move |socket| real_ws_handler(ctx, ip, socket))
}

pub struct LazySocket {
    socket: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl LazySocket {
    pub fn is_connected(&self) -> bool {
        self.socket.is_some()
    }
    pub async fn recv(&mut self) -> Result<Option<tungstenite::Message>> {
        if let Some(ref mut socket) = self.socket {
            Ok(socket.next().await.transpose()?)
        } else {
            Ok(None)
        }
    }
    pub async fn send(&mut self, ctx: &Context, msg: tungstenite::Message) -> Result<()> {
        if self.socket.is_none() {
            let url = ctx.choose_ws_node().context("no ws nodes")?;
            self.socket = Some(tokio_tungstenite::connect_async(url).await?.0);
        }
        self.socket.as_mut().unwrap().send(msg).await?;
        Ok(())
    }
}

async fn real_ws_handler(ctx: SharedContext, ip: IpAddr, mut socket: WebSocket) {
    ctx.load().metrics.ws_accepted.inc();

    let dead_timer = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(dead_timer);

    let upstream_dead_timer = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(upstream_dead_timer);

    let mut upstream = LazySocket { socket: None };

    let mut ping_interval = tokio::time::interval(Duration::from_secs(19));
    ping_interval.reset();
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut upstream_ping_interval = tokio::time::interval(Duration::from_secs(19));
    upstream_ping_interval.reset();
    upstream_ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            msg = socket.next() => {
                dead_timer.as_mut().reset(Instant::now() + Duration::from_secs(60));
                ping_interval.reset();
                match msg {
                    Some(Ok(msg)) => {
                        let ctx = ctx.load();
                        ctx.metrics.ws_message_received.inc();
                        match msg {
                            Message::Text(msg) => {
                                if let Some(resp) = handle_ws_msg(&ctx, &mut upstream, ip, msg).await {
                                    // Safety: safe becuase response is UTF-8.
                                    let resp = unsafe { String::from_utf8_unchecked(resp.into()) };
                                    if socket.send(Message::Text(resp)).await.is_err() {
                                        break;
                                    }
                                    ctx.metrics.ws_message_sent.inc();
                                }
                            }
                            Message::Close(_) => break,
                            Message::Ping(m) => {
                                if socket.send(Message::Pong(m)).await.is_err() {
                                    break;
                                }
                                ctx.metrics.ws_message_sent.inc();
                            }
                            // Binary messages are ignored.
                            _ => {}
                        }
                    }
                    // Client disconnected.
                    _ => break,
                }
            }
            msg = upstream.recv(), if upstream.is_connected() => {
                upstream_dead_timer.as_mut().reset(Instant::now() + Duration::from_secs(60));
                upstream_ping_interval.reset();
                match msg {
                    Ok(Some(tungstenite::Message::Text(msg))) => {
                        if socket.send(msg.into()).await.is_err() {
                            break;
                        }
                        let ctx = ctx.load();
                        ctx.metrics.ws_message_sent.inc();
                    }
                    Ok(Some(tungstenite::Message::Ping(m))) => {
                        if upstream.send(&ctx.load(), tungstenite::Message::Pong(m)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Some(tungstenite::Message::Close(_))) => break,
                    // Other messages can be ignored.
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(e) => {
                        log::warn!("upstream recv error: {e}");
                        break;
                    }
                }
            }
            _ = &mut dead_timer => {
                break;
            }
            _ = ping_interval.tick() => {
                if socket.send(Message::Ping(vec![])).await.is_err() {
                    break;
                }
                ctx.load().metrics.ws_message_sent.inc();
            }
            _ = &mut upstream_dead_timer, if upstream.is_connected() => {
                break;
            }
            _ = upstream_ping_interval.tick(), if upstream.is_connected() => {
                if upstream.send(&ctx.load(), tungstenite::Message::Ping(vec![])).await.is_err() {
                    break;
                }
            }
        }
    }
}

/// Result is valid UTF-8.
async fn handle_ws_msg(
    ctx: &Context,
    upstream: &mut LazySocket,
    ip: IpAddr,
    msg: String,
) -> Option<JsonBytes> {
    let msg: Bytes = msg.into();
    let mut con = ctx.pool.get().await.ok();
    if let Ok(reqs) = serde_json::from_slice::<Vec<&RawValue>>(&msg) {
        if reqs.is_empty() {
            return Some(error_response(
                &Value::Null,
                INVALID_REQUEST_CODE,
                INVALID_REQUEST_MSG,
            ));
        }
        let mut results = Vec::with_capacity(reqs.len());
        for raw_req in &reqs {
            let req_bytes = msg.slice_ref(raw_req.get().as_bytes());
            if let Some(res) = handle_single_request(
                ctx,
                con.as_deref_mut(),
                ip,
                req_bytes,
                OnSubscription::ErrorWsBatch,
            )
            .await
            {
                results.push(res);
            }
        }
        if !results.is_empty() {
            let mut result = Vec::with_capacity(
                results.iter().map(|r| r.len()).sum::<usize>() + results.len() + 1,
            );
            for x in intersperse(results.iter().map(|r| &**r), b",") {
                result.extend_from_slice(x);
            }
            Some(result.into())
        } else {
            None
        }
    } else {
        handle_single_request(
            ctx,
            con.as_deref_mut(),
            ip,
            msg,
            OnSubscription::ForwardWs(upstream),
        )
        .await
    }
}
