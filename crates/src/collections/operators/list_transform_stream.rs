fn list_transform_stream<T, U>(
    source: &ChangeStream<ListChange<T>>,
    transform: ListTransformFactory<T, U>,
    transform_on_refresh: bool,
) -> ChangeStream<ListChange<U>>
where
    T: Clone + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let state = Arc::new(Mutex::new(ListTransformState::<U>::default()));
        let transform = Arc::clone(&transform);
        source.subscribe(move |batch| {
            let output = {
                let Ok(mut state) = state.lock() else {
                    return;
                };
                state.apply_source(batch, &*transform, transform_on_refresh)
            };
            if let Some(output) = output {
                subscriber(&output);
            }
        })
    })
}

struct ListTransformState<U> {
    destinations: Vec<U>,
    version: u64,
}

impl<U> Default for ListTransformState<U> {
    fn default() -> Self {
        Self {
            destinations: Vec::new(),
            version: 0,
        }
    }
}

impl<U> ListTransformState<U>
where
    U: Clone,
{
    fn apply_source<T>(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        transform: &dyn Fn(&T, Option<&U>, usize) -> U,
        transform_on_refresh: bool,
    ) -> Option<ChangeSet<ListChange<U>>>
    where
        T: Clone,
    {
        let mut changes = Vec::new();

        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let index = (*index).min(self.destinations.len());
                    let destination = transform(item, None, index);
                    self.destinations.insert(index, destination.clone());
                    changes.push(ListChange::Add {
                        index,
                        item: destination,
                    });
                }
                ListChange::AddRange { index, items } => {
                    if items.is_empty() {
                        continue;
                    }
                    let index = (*index).min(self.destinations.len());
                    let destinations = items
                        .iter()
                        .enumerate()
                        .map(|(offset, item)| transform(item, None, index + offset))
                        .collect::<Vec<_>>();
                    self.destinations
                        .splice(index..index, destinations.iter().cloned());
                    changes.push(ListChange::AddRange {
                        index,
                        items: destinations,
                    });
                }
                ListChange::Replace { index, new, .. } => {
                    if *index >= self.destinations.len() {
                        let index = self.destinations.len();
                        let destination = transform(new, None, index);
                        self.destinations.push(destination.clone());
                        changes.push(ListChange::Add {
                            index,
                            item: destination,
                        });
                        continue;
                    }

                    let previous = self.destinations[*index].clone();
                    let destination = transform(new, Some(&previous), *index);
                    self.destinations[*index] = destination.clone();
                    changes.push(ListChange::Replace {
                        index: *index,
                        old: previous,
                        new: destination,
                    });
                }
                ListChange::Refresh { index, item } => {
                    if *index >= self.destinations.len() {
                        continue;
                    }

                    if transform_on_refresh {
                        let previous = self.destinations[*index].clone();
                        let destination = transform(item, Some(&previous), *index);
                        self.destinations[*index] = destination.clone();
                        changes.push(ListChange::Replace {
                            index: *index,
                            old: previous,
                            new: destination,
                        });
                    } else {
                        changes.push(ListChange::Refresh {
                            index: *index,
                            item: self.destinations[*index].clone(),
                        });
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from >= self.destinations.len()
                        || *to >= self.destinations.len()
                        || from == to
                    {
                        continue;
                    }
                    let destination = self.destinations.remove(*from);
                    let to = (*to).min(self.destinations.len());
                    self.destinations.insert(to, destination.clone());
                    changes.push(ListChange::Move {
                        from: *from,
                        to,
                        item: destination,
                    });
                }
                ListChange::Remove { index, .. } => {
                    if *index >= self.destinations.len() {
                        continue;
                    }
                    let old = self.destinations.remove(*index);
                    changes.push(ListChange::Remove { index: *index, old });
                }
                ListChange::RemoveRange { index, old } => {
                    if *index >= self.destinations.len() || old.is_empty() {
                        continue;
                    }
                    let end = index.saturating_add(old.len()).min(self.destinations.len());
                    let old = self.destinations.drain(*index..end).collect::<Vec<_>>();
                    if !old.is_empty() {
                        changes.push(ListChange::RemoveRange { index: *index, old });
                    }
                }
                ListChange::Clear { .. } => {
                    if self.destinations.is_empty() {
                        continue;
                    }
                    let old = std::mem::take(&mut self.destinations);
                    changes.push(ListChange::Clear { old });
                }
            }
        }

        self.emit(changes, batch.version)
    }

    fn emit(
        &mut self,
        changes: Vec<ListChange<U>>,
        source_version: u64,
    ) -> Option<ChangeSet<ListChange<U>>> {
        if source_version > self.version {
            self.version = source_version;
        } else if !changes.is_empty() {
            self.version = self.version.saturating_add(1);
        }
        (!changes.is_empty()).then(|| ChangeSet::new(changes, self.version))
    }
}

fn list_transform_async_stream<T, U, Fut>(
    source: &ChangeStream<ListChange<T>>,
    transform: ListAsyncTransformFactory<T, U, Fut>,
    options: TransformAsyncOptions,
) -> ChangeStream<ListChange<U>>
where
    T: Clone + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
    Fut: Future<Output = U> + Send + 'static,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let (sender, receiver) = mpsc::channel::<ListAsyncWork<T>>();
        let cancellation = Arc::new(Mutex::new(AsyncCancellation::<u64>::default()));

        let worker_cancellation = Arc::clone(&cancellation);
        let worker_transform = Arc::clone(&transform);
        spawn_async_worker(move || {
            let mut state = ListAsyncTransformState::<T, U>::default();
            // Ends when the sender is gone (disposal).
            for work in receiver {
                let output = futures::executor::block_on(state.apply_source(
                    &work,
                    &*worker_transform,
                    options,
                    &worker_cancellation,
                ));
                let mut cancellation = lock_cancellation(&worker_cancellation);
                if cancellation.closed {
                    break;
                }
                cancellation.processed(work.sequence);
                if let Some(output) = output {
                    emitter.push(output);
                }
                drop(cancellation);
                emitter.drain();
            }
        });

        let source_cancellation = Arc::clone(&cancellation);
        let source_sender = Mutex::new(sender);
        let ids = Mutex::new(ListAsyncIds::default());
        let subscription = source.subscribe(move |batch| {
            let work = {
                let mut cancellation = lock_cancellation(&source_cancellation);
                if cancellation.closed {
                    return;
                }
                let sequence = cancellation.next_message();
                let mut ids = ids.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                let change_ids = ids.apply(
                    batch,
                    sequence,
                    &mut cancellation,
                    options.transform_on_refresh,
                );
                ListAsyncWork {
                    batch: batch.clone(),
                    ids: change_ids,
                    sequence,
                }
            };
            if let Ok(sender) = source_sender.lock() {
                let _ = sender.send(work);
            }
        })?;

        Ok(Subscription::new(move || {
            lock_cancellation(&cancellation).close();
            // Dropping the source subscription drops the sender: the worker ends.
            drop(subscription);
            drop(delivery);
        }))
    })
}

/// A source batch for a list async transform's worker: the ids its changes add or replace
/// (`ListAsyncIds`), and its sequence number.
struct ListAsyncWork<T> {
    batch: ChangeSet<ListChange<T>>,
    ids: Vec<Vec<u64>>,
    sequence: u64,
}

/// Stable ids of the source items, kept in source order by the source callback: a later
/// batch that removes or replaces an item cancels the futures still transforming it.
#[derive(Default)]
struct ListAsyncIds {
    ids: Vec<u64>,
    next_id: u64,
}

impl ListAsyncIds {
    fn fresh(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// Applies `batch`, returning for each change the ids of the items it transforms.
    fn apply<T>(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        sequence: u64,
        cancellation: &mut AsyncCancellation<u64>,
        transform_on_refresh: bool,
    ) -> Vec<Vec<u64>> {
        let mut change_ids = Vec::with_capacity(batch.changes.len());
        for change in &batch.changes {
            let mut transformed = Vec::new();
            match change {
                ListChange::Add { index, .. } => {
                    let id = self.fresh();
                    let index = (*index).min(self.ids.len());
                    self.ids.insert(index, id);
                    transformed.push(id);
                }
                ListChange::AddRange { index, items } => {
                    let index = (*index).min(self.ids.len());
                    let added = items.iter().map(|_| self.fresh()).collect::<Vec<_>>();
                    self.ids.splice(index..index, added.iter().copied());
                    transformed = added;
                }
                ListChange::Replace { index, .. } => {
                    let id = self.fresh();
                    match self.ids.get_mut(*index) {
                        Some(slot) => {
                            cancellation.supersede(slot, sequence);
                            *slot = id;
                        }
                        None => self.ids.push(id),
                    }
                    transformed.push(id);
                }
                ListChange::Refresh { index, .. } => {
                    if transform_on_refresh && let Some(id) = self.ids.get(*index).copied() {
                        cancellation.supersede(&id, sequence);
                        transformed.push(id);
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from < self.ids.len() && *to < self.ids.len() {
                        let id = self.ids.remove(*from);
                        self.ids.insert(*to, id);
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index < self.ids.len() {
                        let id = self.ids.remove(*index);
                        cancellation.supersede(&id, sequence);
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index < self.ids.len() {
                        let end = index.saturating_add(old.len()).min(self.ids.len());
                        for id in self.ids.drain(*index..end) {
                            cancellation.supersede(&id, sequence);
                        }
                    }
                }
                ListChange::Clear { .. } => {
                    self.ids.clear();
                    cancellation.clear(sequence);
                }
            }
            change_ids.push(transformed);
        }
        change_ids
    }
}

/// A transformed value: known, or the result of job `.0` of the batch being processed.
/// `Ready(None)` is an item whose transform was cancelled or panicked: it is not in the
/// output.
#[derive(Clone)]
enum ListAsyncSlot<U> {
    Ready(Option<U>),
    Pending(usize),
}

struct ListAsyncContainer<T, U> {
    id: u64,
    source: T,
    destination: ListAsyncSlot<U>,
}

/// One change of a batch, replayed against the output once the batch's jobs are done.
enum ListAsyncStep<U> {
    Insert {
        index: usize,
        slots: Vec<ListAsyncSlot<U>>,
        range: bool,
    },
    Replace {
        index: usize,
        old: ListAsyncSlot<U>,
        new: ListAsyncSlot<U>,
    },
    Refresh {
        index: usize,
        slot: ListAsyncSlot<U>,
    },
    Move {
        from: usize,
        to: usize,
        slot: ListAsyncSlot<U>,
    },
    Remove {
        index: usize,
        slots: Vec<ListAsyncSlot<U>>,
        range: bool,
    },
    Clear {
        slots: Vec<ListAsyncSlot<U>>,
    },
}

struct ListAsyncJob<T, U> {
    id: u64,
    item: T,
    previous: Option<U>,
    index: usize,
}

struct ListAsyncTransformState<T, U> {
    containers: Vec<ListAsyncContainer<T, U>>,
    version: u64,
}

impl<T, U> Default for ListAsyncTransformState<T, U> {
    fn default() -> Self {
        Self {
            containers: Vec::new(),
            version: 0,
        }
    }
}

/// The transformed values of a batch's jobs, filled as they run.
struct ListAsyncResults<T, U> {
    jobs: Vec<Option<ListAsyncJob<T, U>>>,
    values: Vec<Option<U>>,
}

impl<T, U> ListAsyncResults<T, U>
where
    T: Clone,
    U: Clone,
{
    fn queue(&mut self, job: ListAsyncJob<T, U>) -> ListAsyncSlot<U> {
        self.jobs.push(Some(job));
        ListAsyncSlot::Pending(self.jobs.len() - 1)
    }

    fn resolve(&self, slot: &ListAsyncSlot<U>) -> Option<U> {
        match slot {
            ListAsyncSlot::Ready(value) => value.clone(),
            ListAsyncSlot::Pending(job) => self.values.get(*job).cloned().flatten(),
        }
    }

    /// Runs the queued jobs concurrently up to the limit, each abortable and isolated.
    async fn run<Fut>(
        &mut self,
        transform: &dyn Fn(T, Option<U>, usize) -> Fut,
        sequence: u64,
        cancellation: &Mutex<AsyncCancellation<u64>>,
        maximum_concurrency: Option<usize>,
    ) where
        Fut: Future<Output = U>,
    {
        let first = self.values.len();
        let entries = self.jobs[first..]
            .iter_mut()
            .enumerate()
            .filter_map(|(offset, job)| {
                let job = job.take()?;
                let start: Box<dyn FnOnce() -> Fut + '_> =
                    Box::new(move || transform(job.item, job.previous, job.index));
                Some((first + offset, job.id, start))
            })
            .collect::<Vec<_>>();
        self.values.resize(self.jobs.len(), None);
        for (job, outcome) in
            run_async_jobs(entries, sequence, cancellation, maximum_concurrency).await
        {
            if let AsyncJobOutcome::Done(value) = outcome {
                self.values[job] = Some(value);
            }
        }
    }
}

impl<T, U> ListAsyncTransformState<T, U>
where
    T: Clone,
    U: Clone,
{
    /// Applies a source batch: queues the transforms its changes need (running the queued
    /// ones first when a change needs a pending result as its previous value), runs them
    /// concurrently up to the limit, then emits the batch with output indexes that skip
    /// items whose transform was cancelled or panicked.
    async fn apply_source<Fut>(
        &mut self,
        work: &ListAsyncWork<T>,
        transform: &dyn Fn(T, Option<U>, usize) -> Fut,
        options: TransformAsyncOptions,
        cancellation: &Mutex<AsyncCancellation<u64>>,
    ) -> Option<ChangeSet<ListChange<U>>>
    where
        Fut: Future<Output = U>,
    {
        let present_before = self
            .containers
            .iter()
            .map(|container| matches!(container.destination, ListAsyncSlot::Ready(Some(_))))
            .collect::<Vec<_>>();
        let mut results = ListAsyncResults {
            jobs: Vec::new(),
            values: Vec::new(),
        };
        let mut steps = Vec::with_capacity(work.batch.changes.len());

        for (change, ids) in work.batch.changes.iter().zip(&work.ids) {
            match change {
                ListChange::Add { index, item } => {
                    let index = (*index).min(self.containers.len());
                    let id = ids.first().copied().unwrap_or_default();
                    let slot = results.queue(ListAsyncJob {
                        id,
                        item: item.clone(),
                        previous: None,
                        index,
                    });
                    self.containers.insert(
                        index,
                        ListAsyncContainer {
                            id,
                            source: item.clone(),
                            destination: slot.clone(),
                        },
                    );
                    steps.push(ListAsyncStep::Insert {
                        index,
                        slots: vec![slot],
                        range: false,
                    });
                }
                ListChange::AddRange { index, items } => {
                    if items.is_empty() {
                        continue;
                    }
                    let index = (*index).min(self.containers.len());
                    let mut slots = Vec::with_capacity(items.len());
                    let mut containers = Vec::with_capacity(items.len());
                    for (offset, (item, id)) in items.iter().zip(ids).enumerate() {
                        let slot = results.queue(ListAsyncJob {
                            id: *id,
                            item: item.clone(),
                            previous: None,
                            index: index + offset,
                        });
                        slots.push(slot.clone());
                        containers.push(ListAsyncContainer {
                            id: *id,
                            source: item.clone(),
                            destination: slot,
                        });
                    }
                    self.containers.splice(index..index, containers);
                    steps.push(ListAsyncStep::Insert {
                        index,
                        slots,
                        range: true,
                    });
                }
                ListChange::Replace { index, new, .. } => {
                    let id = ids.first().copied().unwrap_or_default();
                    if *index >= self.containers.len() {
                        let index = self.containers.len();
                        let slot = results.queue(ListAsyncJob {
                            id,
                            item: new.clone(),
                            previous: None,
                            index,
                        });
                        self.containers.push(ListAsyncContainer {
                            id,
                            source: new.clone(),
                            destination: slot.clone(),
                        });
                        steps.push(ListAsyncStep::Insert {
                            index,
                            slots: vec![slot],
                            range: false,
                        });
                        continue;
                    }
                    let previous = self
                        .previous_value(
                            *index,
                            &mut results,
                            transform,
                            work.sequence,
                            cancellation,
                            options,
                        )
                        .await;
                    let slot = results.queue(ListAsyncJob {
                        id,
                        item: new.clone(),
                        previous,
                        index: *index,
                    });
                    let container = &mut self.containers[*index];
                    let old = std::mem::replace(&mut container.destination, slot.clone());
                    container.id = id;
                    container.source = new.clone();
                    steps.push(ListAsyncStep::Replace {
                        index: *index,
                        old,
                        new: slot,
                    });
                }
                ListChange::Refresh { index, item } => {
                    if *index >= self.containers.len() {
                        continue;
                    }
                    if options.transform_on_refresh {
                        let previous = self
                            .previous_value(
                                *index,
                                &mut results,
                                transform,
                                work.sequence,
                                cancellation,
                                options,
                            )
                            .await;
                        let id = self.containers[*index].id;
                        let slot = results.queue(ListAsyncJob {
                            id,
                            item: item.clone(),
                            previous,
                            index: *index,
                        });
                        let container = &mut self.containers[*index];
                        let old = std::mem::replace(&mut container.destination, slot.clone());
                        container.source = item.clone();
                        steps.push(ListAsyncStep::Replace {
                            index: *index,
                            old,
                            new: slot,
                        });
                    } else {
                        let container = &mut self.containers[*index];
                        container.source = item.clone();
                        steps.push(ListAsyncStep::Refresh {
                            index: *index,
                            slot: container.destination.clone(),
                        });
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from >= self.containers.len() || *to >= self.containers.len() || from == to
                    {
                        continue;
                    }
                    let container = self.containers.remove(*from);
                    let slot = container.destination.clone();
                    self.containers.insert(*to, container);
                    steps.push(ListAsyncStep::Move {
                        from: *from,
                        to: *to,
                        slot,
                    });
                }
                ListChange::Remove { index, .. } => {
                    if *index >= self.containers.len() {
                        continue;
                    }
                    let container = self.containers.remove(*index);
                    steps.push(ListAsyncStep::Remove {
                        index: *index,
                        slots: vec![container.destination],
                        range: false,
                    });
                }
                ListChange::RemoveRange { index, old } => {
                    if *index >= self.containers.len() || old.is_empty() {
                        continue;
                    }
                    let end = index.saturating_add(old.len()).min(self.containers.len());
                    let slots = self
                        .containers
                        .drain(*index..end)
                        .map(|container| container.destination)
                        .collect();
                    steps.push(ListAsyncStep::Remove {
                        index: *index,
                        slots,
                        range: true,
                    });
                }
                ListChange::Clear { .. } => {
                    let slots = self
                        .containers
                        .drain(..)
                        .map(|container| container.destination)
                        .collect();
                    steps.push(ListAsyncStep::Clear { slots });
                }
            }
        }

        results
            .run(
                transform,
                work.sequence,
                cancellation,
                options.maximum_concurrency,
            )
            .await;
        for container in &mut self.containers {
            if let ListAsyncSlot::Pending(_) = container.destination {
                container.destination =
                    ListAsyncSlot::Ready(results.resolve(&container.destination));
            }
        }
        let changes = replay_list_async_steps(steps, present_before, &results);
        self.emit(changes, work.batch.version)
    }

    /// The current value at `index`, running the queued jobs first when it is still pending.
    async fn previous_value<Fut>(
        &self,
        index: usize,
        results: &mut ListAsyncResults<T, U>,
        transform: &dyn Fn(T, Option<U>, usize) -> Fut,
        sequence: u64,
        cancellation: &Mutex<AsyncCancellation<u64>>,
        options: TransformAsyncOptions,
    ) -> Option<U>
    where
        Fut: Future<Output = U>,
    {
        let slot = &self.containers[index].destination;
        if let ListAsyncSlot::Pending(job) = slot
            && *job >= results.values.len()
        {
            results
                .run(
                    transform,
                    sequence,
                    cancellation,
                    options.maximum_concurrency,
                )
                .await;
        }
        results.resolve(slot)
    }

    fn emit(
        &mut self,
        changes: Vec<ListChange<U>>,
        source_version: u64,
    ) -> Option<ChangeSet<ListChange<U>>> {
        if source_version > self.version {
            self.version = source_version;
        } else if !changes.is_empty() {
            self.version = self.version.saturating_add(1);
        }
        (!changes.is_empty()).then(|| ChangeSet::new(changes, self.version))
    }
}

/// Replays a batch's steps against the output's presence flags, emitting changes whose
/// indexes count only the items in the output.
fn replay_list_async_steps<T, U: Clone>(
    steps: Vec<ListAsyncStep<U>>,
    mut present: Vec<bool>,
    results: &ListAsyncResults<T, U>,
) -> Vec<ListChange<U>>
where
    T: Clone,
{
    let output_index = |present: &[bool], index: usize| -> usize {
        present[..index.min(present.len())]
            .iter()
            .filter(|present| **present)
            .count()
    };
    let mut changes = Vec::new();
    for step in steps {
        match step {
            ListAsyncStep::Insert {
                index,
                slots,
                range,
            } => {
                let at = output_index(&present, index);
                let values = slots
                    .iter()
                    .map(|slot| results.resolve(slot))
                    .collect::<Vec<_>>();
                present.splice(index..index, values.iter().map(Option::is_some));
                let items = values.into_iter().flatten().collect::<Vec<_>>();
                match (items.len(), range) {
                    (0, _) => {}
                    (1, false) => changes.extend(
                        items
                            .into_iter()
                            .map(|item| ListChange::Add { index: at, item }),
                    ),
                    _ => changes.push(ListChange::AddRange { index: at, items }),
                }
            }
            ListAsyncStep::Replace { index, old, new } => {
                let at = output_index(&present, index);
                let new = results.resolve(&new);
                let old = if present[index] {
                    results.resolve(&old)
                } else {
                    None
                };
                present[index] = new.is_some();
                match (old, new) {
                    (Some(old), Some(new)) => changes.push(ListChange::Replace {
                        index: at,
                        old,
                        new,
                    }),
                    (Some(old), None) => changes.push(ListChange::Remove { index: at, old }),
                    (None, Some(item)) => changes.push(ListChange::Add { index: at, item }),
                    (None, None) => {}
                }
            }
            ListAsyncStep::Refresh { index, slot } => {
                if present[index]
                    && let Some(item) = results.resolve(&slot)
                {
                    changes.push(ListChange::Refresh {
                        index: output_index(&present, index),
                        item,
                    });
                }
            }
            ListAsyncStep::Move { from, to, slot } => {
                let shown = present.remove(from);
                let output_from = output_index(&present, from);
                present.insert(to, shown);
                if shown && let Some(item) = results.resolve(&slot) {
                    let output_to = output_index(&present, to);
                    if output_from != output_to {
                        changes.push(ListChange::Move {
                            from: output_from,
                            to: output_to,
                            item,
                        });
                    }
                }
            }
            ListAsyncStep::Remove {
                index,
                slots,
                range,
            } => {
                let at = output_index(&present, index);
                let shown = present
                    .drain(index..index + slots.len())
                    .collect::<Vec<_>>();
                let old = slots
                    .iter()
                    .zip(shown)
                    .filter(|(_, shown)| *shown)
                    .filter_map(|(slot, _)| results.resolve(slot))
                    .collect::<Vec<_>>();
                match (old.len(), range) {
                    (0, _) => {}
                    (1, false) => changes.extend(
                        old.into_iter()
                            .map(|old| ListChange::Remove { index: at, old }),
                    ),
                    _ => changes.push(ListChange::RemoveRange { index: at, old }),
                }
            }
            ListAsyncStep::Clear { slots } => {
                let old = slots
                    .iter()
                    .zip(present.iter())
                    .filter(|(_, shown)| **shown)
                    .filter_map(|(slot, _)| results.resolve(slot))
                    .collect::<Vec<_>>();
                present.clear();
                if !old.is_empty() {
                    changes.push(ListChange::Clear { old });
                }
            }
        }
    }
    changes
}
