use std::{net::IpAddr, time::Duration};

use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
};
use futures::prelude::*;
use tokio::time::Instant;

use super::client_ip::ClientIp;
use crate::context::SharedContext;

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

async fn real_ws_handler(_ctx: SharedContext, _ip: IpAddr, socket: WebSocket) {
    let (mut sock_tx, mut sock_rx) = socket.split();

    let dead_timer = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(dead_timer);

    let mut ping_interval = tokio::time::interval(Duration::from_secs(19));
    ping_interval.reset();
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            msg = sock_rx.next() => {
                dead_timer.as_mut().reset(Instant::now() + Duration::from_secs(60));
                ping_interval.reset();
                match msg {
                    Some(Ok(msg)) => {
                        match msg {
                            Message::Text(_msg) => {
                                // TODO: Subscription requests must be proxied. Other requests are handled same as HTTP.
                            }
                            Message::Close(_) => break,
                            Message::Ping(m) => {
                                if sock_tx.send(Message::Pong(m)).await.is_err() {
                                    break;
                                }
                            }
                            // Binary messages are ignored.
                            _ => {}
                        }
                    }
                    // Client disconnected.
                    _ => break,
                }
            }
            _ = &mut dead_timer => {
                break;
            }
            _ = ping_interval.tick() => {
                if sock_tx.send(Message::Ping(vec![])).await.is_err() {
                    break;
                }
            }
        }
    }
}
