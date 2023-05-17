use anyhow::{bail, Result};
use once_cell::sync::Lazy;
use redis::{aio::Connection, Script, ToRedisArgs};
use thiserror::Error;

#[derive(Error, Debug)]
#[error("rate limit exceeded, retry in {retry_secs} seconds")]
pub struct RateLimitError {
    retry_secs: u64,
}

pub async fn rate_limit(
    con: &mut Connection,
    key: impl ToRedisArgs,
    amount: u32,
    period_millis: u64,
    limit: u64,
) -> Result<()> {
    static SCRIPT: Lazy<Script> = Lazy::new(|| Script::new(include_str!("rate_limit.lua")));

    let (value, ttl_millis): (u64, u64) = SCRIPT
        .key(key)
        .arg(amount)
        .arg(period_millis)
        .invoke_async(con)
        .await?;

    if value > limit {
        bail!(RateLimitError {
            // + 999 to round up.
            retry_secs: (ttl_millis + 999) / 1000
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::{thread_rng, Rng};

    use super::*;

    #[tokio::test]
    async fn test_rate_limit() -> Result<()> {
        let client = deadpool_redis::Config::from_url("redis://127.0.0.1/").create_pool(None)?;
        let con = &mut client.get().await?;
        let key: &[u8; 20] = &thread_rng().gen();
        rate_limit(con, key, 1, 500, 2).await?;
        rate_limit(con, key, 1, 500, 2).await?;
        let err = rate_limit(con, key, 1, 500, 2).await.unwrap_err();
        assert!(err.is::<RateLimitError>());

        tokio::time::sleep(Duration::from_millis(600)).await;
        rate_limit(con, key, 1, 500, 2).await?;

        Ok(())
    }
}
