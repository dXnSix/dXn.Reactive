impl<T> ValueStreamCacheBatchChangeSetExt<T> for ValueStream<Vec<T>> {
    fn to_cache_change_set_batches<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let key_selector = std::sync::Arc::new(key_selector);
        ChangeStream::new(move |subscriber| {
            let key_selector_for_cache = std::sync::Arc::clone(&key_selector);
            let backing = SourceCache::new(move |item: &T| key_selector_for_cache(item));
            let output = backing
                .changes()
                .subscribe(move |batch| subscriber(batch))?;
            let input_backing = backing.clone();
            let input = source.subscribe(move |items| {
                let _ = input_backing.add_or_update_range(items.clone());
            })?;
            Ok(Subscription::many(vec![output, input]))
        })
    }

    fn to_cache_change_set_batches_with_options<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        options: ChangeSetAdapterOptions<T>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        let key_selector = Arc::new(key_selector);
        ChangeStream::new(move |subscriber| {
            let key_selector_for_cache = Arc::clone(&key_selector);
            let backing = SourceCache::new(move |item: &T| key_selector_for_cache(item));
            let mut subscriptions = Vec::new();
            subscriptions.push(
                backing
                    .changes()
                    .subscribe(move |batch| subscriber(batch))?,
            );
            subscriptions.extend(activate_cache_adapter_options(&backing, options.clone())?);
            let input_backing = backing.clone();
            subscriptions.push(source.subscribe(move |items| {
                let _ = input_backing.add_or_update_range(items.clone());
            })?);
            Ok(Subscription::many(subscriptions))
        })
    }
}

fn activate_list_adapter_options<T>(
    backing: &SourceList<T>,
    options: ChangeSetAdapterOptions<T>,
) -> Result<Vec<Subscription>>
where
    T: Clone + PartialEq + Send + Sync + 'static,
{
    let mut subscriptions = Vec::new();
    if let Some(expire_after) = options.expire_after {
        let expiration = if let Some(polling_interval) = options.polling_interval {
            SourceListExpireAfterExt::expire_after_with_polling(
                backing,
                move |item| expire_after(item),
                polling_interval,
            )
        } else {
            SourceListExpireAfterExt::expire_after(backing, move |item| expire_after(item))
        };
        subscriptions.push(expiration.subscribe(|_| {})?);
    }

    if let Some(size) = options.limit_size_to {
        subscriptions.push(SourceListLimitExt::limit_size_to(backing, size).subscribe(|_| {})?);
    }

    Ok(subscriptions)
}

fn activate_cache_adapter_options<K, T>(
    backing: &SourceCache<K, T>,
    options: ChangeSetAdapterOptions<T>,
) -> Result<Vec<Subscription>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + PartialEq + Send + Sync + 'static,
{
    let mut subscriptions = Vec::new();
    if let Some(expire_after) = options.expire_after {
        let expiration = if let Some(polling_interval) = options.polling_interval {
            SourceCacheExpireAfterExt::expire_after_with_polling(
                backing,
                move |item| expire_after(item),
                polling_interval,
            )
        } else {
            SourceCacheExpireAfterExt::expire_after(backing, move |item| expire_after(item))
        };
        subscriptions.push(expiration.subscribe(|_| {})?);
    }

    if let Some(size) = options.limit_size_to {
        subscriptions.push(SourceCacheLimitExt::limit_size_to(backing, size).subscribe(|_| {})?);
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
            ListChange::Refresh { index, .. } => {
                let _ = items.refresh(*index);
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
            CacheChange::Refresh { key, .. } => {
                let _ = cache.refresh_key(key);
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
