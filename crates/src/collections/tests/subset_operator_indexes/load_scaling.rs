//! Initial loads through the subset operators and joins stay linear: appending a new key
//! needs no scans (a quadratic load takes about 16 times as long for 4 times the rows).

use super::super::support::*;

type Row = (u32, u32);
/// Subscribes an operator to a source (and an optional right-hand source).
type Attach = dyn Fn(&SourceCache<u32, Row>, &SourceCache<u32, Row>) -> Subscription;

/// The best of two load times of `rows` rows through the operator `attach` subscribes.
fn load_time(rows: u32, attach: &Attach) -> Duration {
    (0..2)
        .map(|_| {
            let source = SourceCache::new(|row: &Row| row.0);
            let right = SourceCache::new(|row: &Row| row.0);
            let _operator = attach(&source, &right);
            let start = Instant::now();
            source
                .add_or_update_range((0..rows).map(|key| (key, key)))
                .unwrap();
            start.elapsed()
        })
        .min()
        .unwrap()
}

#[test]
fn initial_loads_scale_linearly() {
    let operators: [(&str, &Attach); 3] = [
        ("filter", &|source, _| {
            source
                .connect()
                .filter(|row: &Row| row.1.is_multiple_of(2))
                .subscribe(|_| {})
                .unwrap()
        }),
        ("transform_safe", &|source, _| {
            source
                .connect()
                .transform_safe(|row: &Row| Ok::<_, ()>(row.1), |_| {})
                .subscribe(|_| {})
                .unwrap()
        }),
        ("left_join", &|source, right| {
            source
                .connect()
                .left_join(right.connect(), |_, left: &Row, _: Option<&Row>| left.1)
                .subscribe(|_| {})
                .unwrap()
        }),
    ];
    for (name, attach) in operators {
        let small = load_time(10_000, attach);
        let large = load_time(40_000, attach);
        assert!(
            large < small * 8,
            "{name}: 10,000 rows in {small:?}, 40,000 rows in {large:?} (quadratic growth)"
        );
    }
}
