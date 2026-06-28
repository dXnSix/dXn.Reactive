use std::sync::{Arc, Mutex};

use rayx_reactive_primitives::{ReactiveError, Result};

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

    pub fn values(&self) -> rayx_reactive_primitives::ValueStream<T>
    where
        T: Send + Sync + 'static,
    {
        self.signal.values()
    }

    pub fn changes(&self) -> rayx_reactive_primitives::ValueStream<SignalChange<T>>
    where
        T: Send + Sync + 'static,
    {
        self.signal.changes()
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
        let initial = source.read(|value| derive(value))?;
        let signal = Signal::new(initial);
        let subscriptions = DisposeBag::new();
        let signal_for_source = signal.clone();
        let derive_for_source = Arc::clone(&derive);
        source
            .changes()
            .subscribe(move |change| {
                let _ = signal_for_source.replace(derive_for_source(&change.current));
            })?
            .dispose_with(&subscriptions);

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
        let first_value = first.get()?;
        let second_value = second.get()?;
        let combine = Arc::new(combine);
        let initial = combine(&first_value, &second_value);
        let latest = Arc::new(Mutex::new((first_value, second_value)));
        let signal = Signal::new(initial);
        let subscriptions = DisposeBag::new();

        {
            let latest = Arc::clone(&latest);
            let signal = signal.clone();
            let combine = Arc::clone(&combine);
            first
                .changes()
                .subscribe(move |change| {
                    let next =
                        latest
                            .lock()
                            .map_err(|_| ReactiveError::Poisoned)
                            .map(|mut latest| {
                                latest.0 = change.current.clone();
                                combine(&latest.0, &latest.1)
                            });
                    if let Ok(next) = next {
                        let _ = signal.replace(next);
                    }
                })?
                .dispose_with(&subscriptions);
        }

        {
            let latest = Arc::clone(&latest);
            let signal = signal.clone();
            let combine = Arc::clone(&combine);
            second
                .changes()
                .subscribe(move |change| {
                    let next =
                        latest
                            .lock()
                            .map_err(|_| ReactiveError::Poisoned)
                            .map(|mut latest| {
                                latest.1 = change.current.clone();
                                combine(&latest.0, &latest.1)
                            });
                    if let Ok(next) = next {
                        let _ = signal.replace(next);
                    }
                })?
                .dispose_with(&subscriptions);
        }

        Ok(Self {
            signal,
            _subscriptions: subscriptions,
        })
    }
}
