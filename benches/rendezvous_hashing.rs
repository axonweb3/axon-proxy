use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use my_proxy::rendezvous_hashing::*;
use siphasher::sip::SipHasher;

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("rh 100", bench_rh_100);
    c.bench_function("wrh 100", bench_wrh_100);
}

fn bench_rh_100(b: &mut Bencher) {
    let nodes = Vec::from_iter((0..100).map(|i| ("node", i)));
    b.iter(|| rendezvous_hashing(&nodes, 9i32, SipHasher::new()));
}

fn bench_wrh_100(b: &mut Bencher) {
    let nodes = Vec::from_iter((0..100).map(|i| (("node", i), 1.)));
    b.iter(|| weighted_rendezvous_hashing(&nodes, 9i32, SipHasher::new()));
}
