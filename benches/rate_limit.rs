use std::cell::RefCell;

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use my_proxy::redis::rate_limit::rate_limit;
use rand::{thread_rng, Rng};

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("rate limit", bench_rate_limit);
}

fn bench_rate_limit(b: &mut Bencher) {
    let client = deadpool_redis::Config::from_url("redis://127.0.0.1/")
        .create_pool(None)
        .unwrap();
    // Benchmark rate limit on single thread, single connection.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let key: [u8; 20] = thread_rng().gen();

    // Using a refcell to reuse the same connection.
    let con = RefCell::new(rt.block_on(client.get()).unwrap());

    #[allow(clippy::await_holding_refcell_ref)]
    b.to_async(rt).iter(|| async {
        rate_limit(&mut con.borrow_mut(), &key, 1, 60000, u64::MAX)
            .await
            .unwrap()
    });
}
