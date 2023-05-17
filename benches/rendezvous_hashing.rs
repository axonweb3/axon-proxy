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
    let mut hrw = RendezvousHashing::new(SipHasher::new());
    for i in 0..100 {
        hrw.add(("node", i));
    }
    b.iter(|| hrw.choose(9i32));
}

fn bench_wrh_100(b: &mut Bencher) {
    let mut hrw = WeightedRendezvousHashing::new(SipHasher::new());
    for i in 0..100 {
        hrw.add(("node", i), 1.);
    }
    b.iter(|| hrw.choose(9i32));
}
