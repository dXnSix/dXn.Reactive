use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::primitives::{ReactiveError, Result};

use crate::disposal::{DisposeBag, DisposeWith};
use crate::signal::{ReadSignal, Signal, SignalChange};

pub struct Computed<T> {
    signal: Signal<T>,
    _subscriptions: DisposeBag,
}

impl<T> Computed<T>
where
    T: Clone,
{
    pub fn get(&self) -> Result<T> {
        self.signal.get()
    }

    pub fn read_signal(&self) -> ReadSignal<T> {
        self.signal.read_only()
    }

    pub fn values(&self) -> crate::primitives::ValueStream<T>
    where
        T: Send + Sync + 'static,
    {
        self.signal.values()
    }

    pub fn changes(&self) -> crate::primitives::ValueStream<SignalChange<T>>
    where
        T: Send + Sync + 'static,
    {
        self.signal.changes()
    }
}

#[derive(Clone)]
struct VersionedValue<T> {
    value: T,
    version: u64,
}

fn snapshot<S>(source: &ReadSignal<S>) -> Result<VersionedValue<S>>
where
    S: Clone,
{
    loop {
        let version = source.version()?;
        let value = source.get()?;
        if source.version()? == version {
            return Ok(VersionedValue { value, version });
        }
    }
}

struct DeriveState<S> {
    baseline: Option<u64>,
    pending: VecDeque<VersionedValue<S>>,
    processing: bool,
}

impl<S> DeriveState<S> {
    fn new() -> Self {
        Self {
            baseline: None,
            pending: VecDeque::new(),
            processing: false,
        }
    }

    fn push_change(&mut self, change: &SignalChange<S>) -> bool
    where
        S: Clone,
    {
        if self
            .baseline
            .is_none_or(|baseline| change.version > baseline)
        {
            self.pending.push_back(VersionedValue {
                value: change.current.clone(),
                version: change.version,
            });
        }
        self.start_processing()
    }

    fn initialize(&mut self, baseline: VersionedValue<S>) -> bool {
        self.baseline = Some(baseline.version);
        self.pending
            .retain(|change| change.version > baseline.version);
        self.pending.push_front(baseline);
        self.start_processing()
    }

    fn start_processing(&mut self) -> bool {
        if self.processing || self.pending.is_empty() {
            false
        } else {
            self.processing = true;
            true
        }
    }
}

fn drain_derive<S, T>(
    state: &Arc<Mutex<DeriveState<S>>>,
    signal: &Signal<T>,
    derive: &(impl Fn(&S) -> T + ?Sized),
) where
    S: Clone,
    T: Clone + Send + 'static,
{
    loop {
        let next = match state.lock() {
            Ok(mut state) => match state.pending.pop_front() {
                Some(next) => next,
                None => {
                    state.processing = false;
                    return;
                }
            },
            Err(_) => return,
        };

        let _ = signal.replace(derive(&next.value));
    }
}

enum CombineChange<A, B> {
    Initial,
    First(VersionedValue<A>),
    Second(VersionedValue<B>),
}

struct CombineState<A, B> {
    first: Option<VersionedValue<A>>,
    second: Option<VersionedValue<B>>,
    pending: VecDeque<CombineChange<A, B>>,
    processing: bool,
}

impl<A, B> CombineState<A, B> {
    fn new() -> Self {
        Self {
            first: None,
            second: None,
            pending: VecDeque::new(),
            processing: false,
        }
    }

    fn push_first(&mut self, change: &SignalChange<A>) -> bool
    where
        A: Clone,
    {
        if self
            .first
            .as_ref()
            .is_none_or(|first| change.version > first.version)
        {
            self.pending.push_back(CombineChange::First(VersionedValue {
                value: change.current.clone(),
                version: change.version,
            }));
        }
        self.start_processing()
    }

    fn push_second(&mut self, change: &SignalChange<B>) -> bool
    where
        B: Clone,
    {
        if self
            .second
            .as_ref()
            .is_none_or(|second| change.version > second.version)
        {
            self.pending
                .push_back(CombineChange::Second(VersionedValue {
                    value: change.current.clone(),
                    version: change.version,
                }));
        }
        self.start_processing()
    }

    fn initialize(&mut self, first: VersionedValue<A>, second: VersionedValue<B>) -> bool {
        self.pending.retain(|change| match change {
            CombineChange::Initial => false,
            CombineChange::First(change) => change.version > first.version,
            CombineChange::Second(change) => change.version > second.version,
        });
        self.first = Some(first);
        self.second = Some(second);
        self.pending.push_front(CombineChange::Initial);
        self.start_processing()
    }

    fn start_processing(&mut self) -> bool {
        if self.first.is_none()
            || self.second.is_none()
            || self.processing
            || self.pending.is_empty()
        {
            false
        } else {
            self.processing = true;
            true
        }
    }

    fn next(&mut self) -> Option<(A, B)>
    where
        A: Clone,
        B: Clone,
    {
        let change = self.pending.pop_front()?;
        match change {
            CombineChange::Initial => {}
            CombineChange::First(first) => self.first = Some(first),
            CombineChange::Second(second) => self.second = Some(second),
        }

        match (&self.first, &self.second) {
            (Some(first), Some(second)) => Some((first.value.clone(), second.value.clone())),
            _ => None,
        }
    }
}

fn drain_combine<A, B, T>(
    state: &Arc<Mutex<CombineState<A, B>>>,
    signal: &Signal<T>,
    combine: &(impl Fn(&A, &B) -> T + ?Sized),
) where
    A: Clone,
    B: Clone,
    T: Clone + Send + 'static,
{
    loop {
        let next = match state.lock() {
            Ok(mut state) => match state.next() {
                Some(next) => next,
                None => {
                    state.processing = false;
                    return;
                }
            },
            Err(_) => return,
        };

        let _ = signal.replace(combine(&next.0, &next.1));
    }
}

impl<T> Computed<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn derive<S>(
        source: ReadSignal<S>,
        derive: impl Fn(&S) -> T + Send + Sync + 'static,
    ) -> Result<Self>
    where
        S: Clone + Send + Sync + 'static,
    {
        let derive = Arc::new(derive);
        let initial = snapshot(&source)?;
        let signal = Signal::new(derive(&initial.value));
        let subscriptions = DisposeBag::new();
        let state = Arc::new(Mutex::new(DeriveState::new()));

        {
            let state = Arc::clone(&state);
            let signal = signal.clone();
            let derive = Arc::clone(&derive);
            source
                .changes()
                .subscribe(move |change| {
                    let should_drain = state
                        .lock()
                        .map(|mut state| state.push_change(change))
                        .unwrap_or(false);
                    if should_drain {
                        drain_derive(&state, &signal, derive.as_ref());
                    }
                })?
                .dispose_with(&subscriptions);
        }

        let baseline = snapshot(&source)?;
        let should_drain = state
            .lock()
            .map(|mut state| state.initialize(baseline))
            .map_err(|_| ReactiveError::Poisoned)?;
        if should_drain {
            drain_derive(&state, &signal, derive.as_ref());
        }

        Ok(Self {
            signal,
            _subscriptions: subscriptions,
        })
    }

    pub fn combine2<A, B>(
        first: ReadSignal<A>,
        second: ReadSignal<B>,
        combine: impl Fn(&A, &B) -> T + Send + Sync + 'static,
    ) -> Result<Self>
    where
        A: Clone + Send + Sync + 'static,
        B: Clone + Send + Sync + 'static,
    {
        let combine = Arc::new(combine);
        let initial_first = snapshot(&first)?;
        let initial_second = snapshot(&second)?;
        let signal = Signal::new(combine(&initial_first.value, &initial_second.value));
        let subscriptions = DisposeBag::new();
        let state = Arc::new(Mutex::new(CombineState::new()));

        {
            let state = Arc::clone(&state);
            let signal = signal.clone();
            let combine = Arc::clone(&combine);
            first
                .changes()
                .subscribe(move |change| {
                    let should_drain = state
                        .lock()
                        .map(|mut state| state.push_first(change))
                        .unwrap_or(false);
                    if should_drain {
                        drain_combine(&state, &signal, combine.as_ref());
                    }
                })?
                .dispose_with(&subscriptions);
        }

        {
            let state = Arc::clone(&state);
            let signal = signal.clone();
            let combine = Arc::clone(&combine);
            second
                .changes()
                .subscribe(move |change| {
                    let should_drain = state
                        .lock()
                        .map(|mut state| state.push_second(change))
                        .unwrap_or(false);
                    if should_drain {
                        drain_combine(&state, &signal, combine.as_ref());
                    }
                })?
                .dispose_with(&subscriptions);
        }

        let latest_first = snapshot(&first)?;
        let latest_second = snapshot(&second)?;
        let should_drain = state
            .lock()
            .map(|mut state| state.initialize(latest_first, latest_second))
            .map_err(|_| ReactiveError::Poisoned)?;
        if should_drain {
            drain_combine(&state, &signal, combine.as_ref());
        }

        Ok(Self {
            signal,
            _subscriptions: subscriptions,
        })
    }
}
