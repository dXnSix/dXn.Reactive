//! Initial loads through the subset operators and joins stay linear: appending a new key
//! needs no scans (a quadratic load does about 16 times the key work for 4 times the rows).

use super::super::support::*;

type Row = (u32, u32);
/// Subscribes an operator to a source (and an optional right-hand source).
type Attach = dyn Fn(&SourceCache<CountedKey, Row>, &SourceCache<CountedKey, Row>) -> Subscription;

/// The work of loading `rows` rows through the operator `attach` subscribes.
fn load_work(rows: u32, attach: &Attach) -> Work {
    let ops = KeyOps::default();
    let (keys, right_keys) = (ops.clone(), ops.clone());
    let source = SourceCache::new(move |row: &Row| keys.key(row.0));
    let right = SourceCache::new(move |row: &Row| right_keys.key(row.0));
    let _operator = attach(&source, &right);
    ops.work(|| {
        source
            .add_or_update_range((0..rows).map(|key| (key, key)))
            .unwrap();
    })
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
        assert_linear(name, |rows| load_work(rows, attach));
    }
}
