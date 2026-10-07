fn activate_list_adapter_options<T>(
    backing: &SourceList<T>,
    options: ChangeSetAdapterOptions<T>,
    errors: Option<&ErrorSink>,
) -> Result<Vec<Subscription>>
where
    T: Clone + PartialEq + Send + Sync + 'static,
{
    let mut subscriptions = Vec::new();
    if let Some(expire_after) = options.expire_after {
        let scheduler = options.scheduler.unwrap_or_else(default_scheduler);
        let expiration = if let Some(polling_interval) = options.polling_interval {
            SourceListExpireAfterExt::expire_after_with_polling_with_scheduler(
                backing,
                move |item| expire_after(item),
                polling_interval,
                scheduler,
            )
        } else {
            SourceListExpireAfterExt::expire_after_with_scheduler(
                backing,
                move |item| expire_after(item),
                scheduler,
            )
        };
        subscriptions.push(expiration.subscribe(|_| {})?);
    }

    if let Some(size) = options.limit_size_to {
        let limit = match errors {
            Some(errors) => {
                let errors = Arc::clone(errors);
                SourceListLimitExt::limit_size_to_with_errors(backing, size, move |error| {
                    errors(error);
                })
            }
            None => SourceListLimitExt::limit_size_to(backing, size),
        };
        subscriptions.push(limit.subscribe(|_| {})?);
    }

    Ok(subscriptions)
}

fn activate_cache_adapter_options<K, T>(
    backing: &SourceCache<K, T>,
    options: ChangeSetAdapterOptions<T>,
    errors: Option<&ErrorSink>,
) -> Result<Vec<Subscription>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + PartialEq + Send + Sync + 'static,
{
    let mut subscriptions = Vec::new();
    if let Some(expire_after) = options.expire_after {
        let scheduler = options.scheduler.unwrap_or_else(default_scheduler);
        let expiration = if let Some(polling_interval) = options.polling_interval {
            SourceCacheExpireAfterExt::expire_after_with_polling_with_scheduler(
                backing,
                move |item| expire_after(item),
                polling_interval,
                scheduler,
            )
        } else {
            SourceCacheExpireAfterExt::expire_after_with_scheduler(
                backing,
                move |item| expire_after(item),
                scheduler,
            )
        };
        subscriptions.push(expiration.subscribe(|_| {})?);
    }

    if let Some(size) = options.limit_size_to {
        let limit = match errors {
            Some(errors) => {
                let errors = Arc::clone(errors);
                SourceCacheLimitExt::limit_size_to_with_errors(backing, size, move |error| {
                    errors(error);
                })
            }
            None => SourceCacheLimitExt::limit_size_to(backing, size),
        };
        subscriptions.push(limit.subscribe(|_| {})?);
    }

    Ok(subscriptions)
}

fn apply_list_changes_to_updater<T>(items: &mut ListUpdater<'_, T>, changes: &[ListChange<T>])
where
    T: Clone,
{
    for change in changes {
        match change {
            ListChange::Add { index, item } => {
                let _ = items.insert(*index, item.clone());
            }
            ListChange::AddRange {
                index,
                items: range,
            } => {
                let _ = items.insert_range(*index, range.clone());
            }
            ListChange::Replace { index, new, .. } => {
                let _ = items.replace(*index, new.clone());
            }
            ListChange::Move { from, to, .. } => {
                let _ = items.move_item(*from, *to);
            }
            ListChange::Remove { index, .. } => {
                let _ = items.remove(*index);
            }
            ListChange::RemoveRange { index, old } => {
                let _ = items.remove_range(*index, old.len());
            }
            ListChange::Refresh { index, item } => {
                let _ = items.refresh_with(*index, item.clone());
            }
            ListChange::Clear { .. } => items.clear(),
        }
    }
}

fn apply_cache_changes_to_updater<K, T>(
    cache: &mut CacheUpdater<'_, K, T>,
    changes: &[CacheChange<K, T>],
) where
    K: Clone + Eq + Hash,
    T: Clone,
{
    for change in changes {
        match change {
            CacheChange::Add {
                key,
                current,
                current_index,
            } => cache.add_or_update_key_at(key.clone(), current.clone(), *current_index),
            CacheChange::Update {
                key,
                current,
                current_index,
                ..
            } => cache.add_or_update_key_at(key.clone(), current.clone(), *current_index),
            CacheChange::Refresh { key, current, .. } => {
                let _ = cache.refresh_key_with(key, current.clone());
            }
            CacheChange::Move {
                key,
                current,
                current_index,
                ..
            } => {
                if !cache.move_key(key, *current_index) {
                    cache.add_or_update_key_at(key.clone(), current.clone(), Some(*current_index));
                }
            }
            CacheChange::Remove { key, .. } => {
                let _ = cache.remove_key(key);
            }
            CacheChange::Clear { previous } if previous.is_empty() => cache.clear(),
            CacheChange::Clear { previous } => {
                for (key, _) in previous {
                    let _ = cache.remove_key(key);
                }
            }
        }
    }
}
