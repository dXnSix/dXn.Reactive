use crate::collections::change::{ListChange, ListChangeReason};
use crate::collections::list::SourceList;
use crate::collections::operators::apply_list_changes;

fn assert_reconstructs(old: &[i32], target: &[i32]) -> Vec<ListChange<i32>> {
    let source = SourceList::new(old.to_vec());
    let batch = source.edit_diff(target.to_vec()).expect("edit diff");
    let mut reconstructed = old.to_vec();
    apply_list_changes(&mut reconstructed, &batch.changes);
    assert_eq!(reconstructed, target);
    assert_eq!(source.items().expect("source items"), target);
    batch.changes
}

#[test]
fn localized_list_diff_equal_length_sparse_changes_emit_only_replace() {
    let old = (0..1_280).collect::<Vec<_>>();
    let mut target = old.clone();
    target[17] = -17;
    target[639] = -639;
    target[1_271] = -1_271;

    let changes = assert_reconstructs(&old, &target);

    assert_eq!(
        changes.iter().map(ListChange::reason).collect::<Vec<_>>(),
        vec![
            ListChangeReason::Replace,
            ListChangeReason::Replace,
            ListChangeReason::Replace
        ]
    );
    assert_eq!(
        changes
            .iter()
            .filter_map(|change| match change {
                ListChange::Replace { index, .. } => Some(*index),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![17, 639, 1_271]
    );
}

#[test]
fn localized_list_diff_length_changes_never_clear_a_non_empty_target() {
    let cases = [
        (vec![1, 2, 3, 4], vec![1, 8, 2, 3, 4]),
        (vec![1, 2, 3, 4], vec![1, 3, 4]),
        (vec![1, 2, 1, 3], vec![1, 1, 4, 3]),
        (vec![1, 2, 3, 4], vec![3, 4, 1, 2]),
        (
            (0..256).collect::<Vec<_>>(),
            (10_000..10_256).collect::<Vec<_>>(),
        ),
    ];

    for (old, target) in cases {
        let changes = assert_reconstructs(&old, &target);
        assert!(
            changes
                .iter()
                .all(|change| !matches!(change, ListChange::Clear { .. })),
            "non-empty targets must not use Clear as a generic fallback"
        );
    }
}

#[test]
fn localized_list_diff_clear_is_reserved_for_empty_target() {
    let changes = assert_reconstructs(&[1, 2, 3], &[]);
    assert!(matches!(changes.as_slice(), [ListChange::Clear { .. }]));
}

#[test]
fn localized_list_diff_deterministic_property_cases_reconstruct_target() {
    let mut seed = 0x5eed_u64;
    for case in 0..512 {
        let old_len = next(&mut seed) as usize % 24;
        let target_len = next(&mut seed) as usize % 24;
        let old = (0..old_len)
            .map(|_| (next(&mut seed) % 9) as i32)
            .collect::<Vec<_>>();
        let target = (0..target_len)
            .map(|_| (next(&mut seed) % 9) as i32)
            .collect::<Vec<_>>();

        let first = assert_reconstructs(&old, &target);
        let second = assert_reconstructs(&old, &target);
        assert_eq!(first, second, "case {case} must be deterministic");
        if !target.is_empty() {
            assert!(
                first
                    .iter()
                    .all(|change| !matches!(change, ListChange::Clear { .. })),
                "case {case} used Clear for a non-empty target"
            );
        }
    }
}

fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}
