pub mod request_coalescing;

use std::{
    future::IntoFuture,
    hash::Hash,
    sync::atomic::{AtomicBool, Ordering},
};

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
/// Returns Err(_) on redis error.
///
/// If cache is not hit, this returns a coalesced computing result.
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
                let removed = AtomicBool::new(false);
                // Remove it even if compute panics.
                defer! {
                    if !removed.load(Ordering::Relaxed) {
                        coalescing.remove(&key);
                    }
                }
                let result = compute.await;

                // Set cache if result is Ok.
                if let (Ok(mut con), Ok(o)) = (pool.get().await, &result) {
                    // Ignore error setting cache.
                    let _ = redis::cmd("set")
                        .arg(&key)
                        .arg(o)
                        .arg("PX")
                        .arg(expire_millis)
                        .query_async::<_, String>(&mut con)
                        .await;
                }
                // Remove before sending, so others won't subscribe to us after
                // the result is sent.
                coalescing.remove(&key);
                removed.store(true, Ordering::Relaxed);
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
    use std::{sync::Arc, time::Duration};

    use anyhow::ensure;
    use rand::{thread_rng, Rng};

    use super::*;

    fn gen_key() -> Vec<u8> {
        let key: [u8; 20] = thread_rng().gen();
        key.into()
    }

    #[tokio::test]
    async fn test_coalesced() -> Result<()> {
        // Cached and coalesced.
        let pool = deadpool_redis::Config::from_url("redis://127.0.0.1/").create_pool(None)?;
        let col = Arc::new(RequestCoalescing::default());
        let key: Vec<u8> = gen_key();
        let g1 = get_or_compute_coalesced(&pool, &col, key.clone(), 1000, async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok::<_, i32>(3)
        });
        let g2 = get_or_compute_coalesced(&pool, &col, key.clone(), 1000, async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok::<_, i32>(4)
        });
        let (v1, v2) = tokio::try_join!(g1, g2)?;
        ensure!(v1 == v2);
        ensure!(col.is_empty());
        ensure!(pool.get().await?.exists(key).await?);

        // Coalesced but not cached.
        let key: Vec<u8> = gen_key();
        let g3 = get_or_compute_coalesced(&pool, &col, key.clone(), 1000, async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err::<i32, i32>(4)
        });
        let g4 = get_or_compute_coalesced(&pool, &col, key.clone(), 1000, async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err::<i32, i32>(5)
        });
        let (v3, v4) = tokio::try_join!(g3, g4)?;
        ensure!(v3 == v4);
        ensure!(v3.is_err());
        ensure!(!pool.get().await?.exists(key).await?);

        // If the compute future panics, the coalescing entry is still removed.
        let col1 = col.clone();
        let t = tokio::spawn(async move {
            let key: Vec<u8> = gen_key();
            get_or_compute_coalesced(&pool, &col1, key, 1000, async {
                if true {
                    panic!("panic");
                }
                Ok::<i32, i32>(1)
            })
            .await
            .unwrap()
            .unwrap();
        });
        ensure!(t.await.is_err());
        ensure!(col.is_empty());

        Ok(())
    }
}
