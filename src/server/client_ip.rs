use std::{
    convert::Infallible,
    net::{IpAddr, SocketAddr},
};

use async_trait::async_trait;
use axum::{
    extract::{ConnectInfo, FromRequestParts},
    http::request::Parts,
};

/// Extract client ip from the last value of the x-forwarded-for header or ConnectInfo.
pub struct ClientIp(pub IpAddr);

#[async_trait]
impl<S> FromRequestParts<S> for ClientIp
where
    S: Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(ip) = parts
            .headers
            .get_all("x-forwarded-for")
            .into_iter()
            .last()
            .and_then(|v| v.as_bytes().rsplitn(2, |b| *b == b',').next())
            .and_then(|last| std::str::from_utf8(last).ok())
            .and_then(|last| last.trim().parse::<IpAddr>().ok())
        {
            return Ok(Self(ip));
        }

        let ip = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .unwrap()
            .0
            .ip();
        Ok(Self(ip))
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, TcpListener};

    use anyhow::{ensure, Result};
    use axum::routing::get;

    use super::*;

    async fn echo_my_ip(ip: ClientIp) -> String {
        ip.0.to_string()
    }

    #[allow(invalid_from_utf8)]
    #[tokio::test]
    async fn test_my_secure_client_ip() -> Result<()> {
        let svc = axum::Router::new()
            .route("/", get(echo_my_ip))
            .into_make_service_with_connect_info::<SocketAddr>();
        let lis = TcpListener::bind((Ipv4Addr::from([127, 0, 0, 1]), 0))?;
        let port = lis.local_addr()?.port();
        let handle = tokio::spawn(async move {
            axum::Server::from_tcp(lis)
                .unwrap()
                .serve(svc)
                .await
                .unwrap();
        })
        .abort_handle();

        let client = reqwest::Client::new();

        let ip1 = client
            .get(format!("http://127.0.0.1:{port}"))
            .send()
            .await?
            .text()
            .await?;
        ensure!(ip1 == "127.0.0.1");

        let ip2 = client
            .get(format!("http://127.0.0.1:{port}"))
            .header("X-Forwarded-For", "1.2.3.4")
            .send()
            .await?
            .text()
            .await?;
        ensure!(ip2 == "1.2.3.4");

        let ip3 = client
            .get(format!("http://127.0.0.1:{port}"))
            .header("X-Forwarded-For", "::1")
            .send()
            .await?
            .text()
            .await?;
        ensure!(ip3 == "::1");

        ensure!(std::str::from_utf8(b"\xff, 1.2.3.4").is_err());
        let ip4 = client
            .get(format!("http://127.0.0.1:{port}"))
            .header("X-Forwarded-For", &b"\xff, 1.2.3.4"[..])
            .send()
            .await?
            .text()
            .await?;
        ensure!(ip4 == "1.2.3.4");

        drop(handle);
        Ok(())
    }
}
