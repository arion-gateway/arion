use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use str_utils::ToLowercase;

const UPGRADE: &str = "upgrade";

fn cmp_to_lowercase(s: &str) -> bool {
    s.to_lowercase() == UPGRADE
}

fn cmp_to_lowercase_cow(s: &str) -> bool {
    s.to_lowercase_cow() == UPGRADE
}

fn cmp_eq_ignore_ascii_case(s: &str) -> bool {
    s.eq_ignore_ascii_case(UPGRADE)
}

fn bench_lowercase_compare(c: &mut Criterion) {
    let cases = [
        ("already_lower", "keep-alive"),
        ("mixed_case", "Keep-Alive"),
        ("already_lower_upgrade", "upgrade"),
        ("mixed_upgrade", "Upgrade"),
    ];

    let mut group = c.benchmark_group("lowercase_then_compare");
    group.sample_size(80);
    group.warm_up_time(std::time::Duration::from_millis(300));
    group.measurement_time(std::time::Duration::from_secs(2));
    for (label, input) in cases {
        group.bench_with_input(BenchmarkId::new("to_lowercase", label), input, |b, s| {
            b.iter(|| black_box(cmp_to_lowercase(black_box(s))));
        });
        group.bench_with_input(BenchmarkId::new("to_lowercase_cow", label), input, |b, s| {
            b.iter(|| black_box(cmp_to_lowercase_cow(black_box(s))));
        });
        group.bench_with_input(BenchmarkId::new("eq_ignore_ascii_case", label), input, |b, s| {
            b.iter(|| black_box(cmp_eq_ignore_ascii_case(black_box(s))));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_lowercase_compare);
criterion_main!(benches);
