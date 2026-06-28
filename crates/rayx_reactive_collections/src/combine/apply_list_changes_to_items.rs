fn apply_list_changes_to_items<T>(items: &mut Vec<T>, changes: &[ListChange<T>])
where
    T: Clone,
{
    for change in changes {
        match change {
            ListChange::Add { index, item } => {
                if *index <= items.len() {
                    items.insert(*index, item.clone());
                }
            }
            ListChange::AddRange {
                index,
                items: added,
            } => {
                if *index <= items.len() {
                    items.splice(*index..*index, added.clone());
                }
            }
            ListChange::Replace { index, new, .. } => {
                if let Some(item) = items.get_mut(*index) {
                    *item = new.clone();
                }
            }
            ListChange::Move { from, to, .. } => {
                if *from < items.len() && *to < items.len() {
                    let item = items.remove(*from);
                    items.insert(*to, item);
                }
            }
            ListChange::Remove { index, .. } => {
                if *index < items.len() {
                    let _ = items.remove(*index);
                }
            }
            ListChange::RemoveRange { index, old } => {
                if *index < items.len() {
                    let end = index.saturating_add(old.len()).min(items.len());
                    let _ = items.drain(*index..end);
                }
            }
            ListChange::Refresh { index, item } => {
                if let Some(existing) = items.get_mut(*index) {
                    *existing = item.clone();
                }
            }
            ListChange::Clear { .. } => {
                items.clear();
            }
        }
    }
}

fn increment_count<T>(counts: &mut HashMap<T, usize>, item: T)
where
    T: Eq + Hash,
{
    *counts.entry(item).or_insert(0) += 1;
}

fn decrement_count<T>(counts: &mut HashMap<T, usize>, item: &T)
where
    T: Eq + Hash,
{
    if let Some(count) = counts.get_mut(item) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            let _ = counts.remove(item);
        }
    }
}
