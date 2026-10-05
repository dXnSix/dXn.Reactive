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
    transform_on_refresh: bool,
) -> ChangeStream<ListChange<U>>
where
    T: Clone + Send + Sync + 'static,
    U: Clone + PartialEq + Send + Sync + 'static,
    Fut: Future<Output = U> + Send + 'static,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let (sender, receiver) = mpsc::channel::<ListAsyncMessage<T>>();
        let active = Arc::new(AtomicBool::new(true));

        let worker_active = Arc::clone(&active);
        let worker_subscriber = Arc::clone(&subscriber);
        let worker_transform = Arc::clone(&transform);
        thread::spawn(move || {
            let mut state = ListAsyncTransformState::<T, U>::default();
            while worker_active.load(AtomicOrdering::Acquire) {
                let Ok(message) = receiver.recv() else {
                    break;
                };
                let ListAsyncMessage::Source(batch) = message else {
                    break;
                };

                let output = futures::executor::block_on(state.apply_source(
                    &batch,
                    &*worker_transform,
                    transform_on_refresh,
                ));
                if let Some(output) = output
                    && worker_active.load(AtomicOrdering::Acquire)
                {
                    worker_subscriber(&output);
                }
            }
        });

        let source_active = Arc::clone(&active);
        let source_sender = sender.clone();
        let subscription = source.subscribe(move |batch| {
            if source_active.load(AtomicOrdering::Acquire) {
                let _ = source_sender.send(ListAsyncMessage::Source(batch.clone()));
            }
        })?;

        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::Release);
            let _ = sender.send(ListAsyncMessage::Stop);
            drop(subscription);
        }))
    })
}

enum ListAsyncMessage<T> {
    Source(ChangeSet<ListChange<T>>),
    Stop,
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

struct ListAsyncContainer<T, U> {
    source: T,
    destination: U,
}

impl<T, U> ListAsyncTransformState<T, U>
where
    T: Clone,
    U: Clone + PartialEq,
{
    async fn apply_source<Fut>(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        transform: &dyn Fn(T, Option<U>, usize) -> Fut,
        transform_on_refresh: bool,
    ) -> Option<ChangeSet<ListChange<U>>>
    where
        Fut: Future<Output = U>,
    {
        let mut changes = Vec::new();

        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let index = (*index).min(self.containers.len());
                    let destination = transform(item.clone(), None, index).await;
                    self.containers.insert(
                        index,
                        ListAsyncContainer {
                            source: item.clone(),
                            destination: destination.clone(),
                        },
                    );
                    changes.push(ListChange::Add {
                        index,
                        item: destination,
                    });
                }
                ListChange::AddRange { index, items } => {
                    if items.is_empty() {
                        continue;
                    }
                    let index = (*index).min(self.containers.len());
                    let destinations = join_all(
                        items
                            .iter()
                            .cloned()
                            .enumerate()
                            .map(|(offset, item)| transform(item, None, index + offset)),
                    )
                    .await;
                    let containers = items
                        .iter()
                        .cloned()
                        .zip(destinations.iter().cloned())
                        .map(|(source, destination)| ListAsyncContainer {
                            source,
                            destination,
                        })
                        .collect::<Vec<_>>();
                    self.containers.splice(index..index, containers);
                    changes.push(ListChange::AddRange {
                        index,
                        items: destinations,
                    });
                }
                ListChange::Replace { index, new, .. } => {
                    if *index >= self.containers.len() {
                        let index = self.containers.len();
                        let destination = transform(new.clone(), None, index).await;
                        self.containers.push(ListAsyncContainer {
                            source: new.clone(),
                            destination: destination.clone(),
                        });
                        changes.push(ListChange::Add {
                            index,
                            item: destination,
                        });
                        continue;
                    }

                    let previous = self.containers[*index].destination.clone();
                    let destination = transform(new.clone(), Some(previous.clone()), *index).await;
                    self.containers[*index] = ListAsyncContainer {
                        source: new.clone(),
                        destination: destination.clone(),
                    };
                    changes.push(ListChange::Replace {
                        index: *index,
                        old: previous,
                        new: destination,
                    });
                }
                ListChange::Refresh { index, item } => {
                    if *index >= self.containers.len() {
                        continue;
                    }

                    if transform_on_refresh {
                        let previous = self.containers[*index].destination.clone();
                        let destination =
                            transform(item.clone(), Some(previous.clone()), *index).await;
                        self.containers[*index] = ListAsyncContainer {
                            source: item.clone(),
                            destination: destination.clone(),
                        };
                        changes.push(ListChange::Replace {
                            index: *index,
                            old: previous,
                            new: destination,
                        });
                    } else {
                        self.containers[*index].source = item.clone();
                        changes.push(ListChange::Refresh {
                            index: *index,
                            item: self.containers[*index].destination.clone(),
                        });
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from >= self.containers.len() || *to >= self.containers.len() || from == to
                    {
                        continue;
                    }
                    let container = self.containers.remove(*from);
                    let destination = container.destination.clone();
                    self.containers.insert(*to, container);
                    changes.push(ListChange::Move {
                        from: *from,
                        to: *to,
                        item: destination,
                    });
                }
                ListChange::Remove { index, .. } => {
                    if *index >= self.containers.len() {
                        continue;
                    }
                    let old = self.containers.remove(*index).destination;
                    changes.push(ListChange::Remove { index: *index, old });
                }
                ListChange::RemoveRange { index, old } => {
                    if *index >= self.containers.len() || old.is_empty() {
                        continue;
                    }
                    let end = index.saturating_add(old.len()).min(self.containers.len());
                    let old = self
                        .containers
                        .drain(*index..end)
                        .map(|container| container.destination)
                        .collect::<Vec<_>>();
                    if !old.is_empty() {
                        changes.push(ListChange::RemoveRange { index: *index, old });
                    }
                }
                ListChange::Clear { .. } => {
                    if self.containers.is_empty() {
                        continue;
                    }
                    let old = self
                        .containers
                        .drain(..)
                        .map(|container| container.destination)
                        .collect::<Vec<_>>();
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

