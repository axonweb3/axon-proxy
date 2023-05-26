pub mod request_coalescing;

use std::{future::IntoFuture, hash::Hash};

use anyhow::Result;
use deadpool_redis::Pool;
use redis::{aio::Connection, AsyncCommands, FromRedisValue, ToRedisArgs};
use scopeguard::defer;

use self::request_coalescing::{CoalescingResult, RequestCoalescing};

pub async fn get_or_compute<K, O, F>(
    con: &mut Connection,
    key: K,
    expire_millis: u64,
    compute: F,
) -> Result<O>
where
    K: ToRedisArgs + Send + Sync + Copy,
    O: ToRedisArgs + FromRedisValue + Clone + Send + Sync,
    F: IntoFuture<Output = O>,
{
    if let Some(o) = con.get(key).await? {
        return Ok(o);
    }

    let o = compute.await;

    // Ignore error.
    let _ = redis::cmd("set")
        .arg(key)
        .arg(o.clone())
        .arg("PX")
        .arg(expire_millis)
        .query_async::<_, String>(con)
        .await;

    Ok(o)
}

/// Get cached value or compute it with in-process request coalescing.
///
/// Return Ok(Ok(o)) if cache hit.
///
/// Otherwise return coalesced computing result.
///
/// Cache is set if compute returns Ok, and not set if compute returns Err.
pub async fn get_or_compute_coalesced<K, O, F, E>(
    pool: &Pool,
    coalescing: &RequestCoalescing<K, Result<O, E>>,
    key: K,
    expire_millis: u64,
    compute: F,
) -> Result<Result<O, E>>
where
    K: ToRedisArgs + Clone + Send + Sync + Hash + Eq,
    O: ToRedisArgs + FromRedisValue + Send + Sync + Clone,
    F: IntoFuture<Output = Result<O, E>>,
    E: Clone,
{
    loop {
        if let Some(o) = pool.get().await?.get(&key).await? {
            return Ok(Ok(o));
        }

        match coalescing.get(key.clone()) {
            CoalescingResult::Sender(tx) => {
                defer! {
                    coalescing.remove(&key);
                }
                let result = compute.await;

                // Set cache if result is Ok.
                if let (Ok(mut con), Ok(o)) = (pool.get().await, &result) {
                    // Ignore error setting cache and broadcasting.
                    let _ = redis::cmd("set")
                        .arg(&key)
                        .arg(o)
                        .arg("PX")
                        .arg(expire_millis)
                        .query_async::<_, String>(&mut con)
                        .await;
                }
                let _ = tx.send(result.clone());

                return Ok(result);
            }
            CoalescingResult::Receiver(mut rx) => {
                if let Ok(result) = rx.recv().await {
                    return Ok(result);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::ensure;
    use rand::{thread_rng, Rng};

    use super::*;

    #[tokio::test]
    async fn test_coalesced() -> Result<()> {
        let pool = deadpool_redis::Config::from_url("redis://127.0.0.1/").create_pool(None)?;
        let col = RequestCoalescing::default();
        let key: [u8; 20] = thread_rng().gen();
        let g1 = get_or_compute_coalesced(&pool, &col, &key, 1000, async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok::<_, i32>(3)
        });
        let g2 = get_or_compute_coalesced(&pool, &col, &key, 1000, async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok::<_, i32>(4)
        });
        let (v1, v2) = tokio::try_join!(g1, g2)?;
        ensure!(v1 == v2);
        ensure!(col.is_empty());

        let key: [u8; 20] = thread_rng().gen();
        let g3 = get_or_compute_coalesced(&pool, &col, &key, 1000, async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err::<i32, i32>(4)
        })
        .await?;
        ensure!(g3 == Err(4));
        ensure!(!pool.get().await?.exists(&key).await?);

        Ok(())
    }
}
