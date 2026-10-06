use std::cell::RefCell;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use crate::primitives::{Result, ValueStream};

use crate::disposal::{DisposeBag, DisposeWith};
use crate::signal::{ReadSignal, Signal, SignalChange, SignalRefresh};

/// A read-only value derived from one or more signals.
///
/// A `Computed` is glitch-free: every published value is computed from the latest value
/// of each input, and an input write that reaches it through several paths (a diamond)
/// publishes once. Reads (`get`, and `get`/`read`/`version` on its
/// [`read_signal`](Self::read_signal)) compare the inputs' versions with the ones the
/// current value was computed from and recompute when they differ, so a read right after
/// an input write is fresh even while another thread is still delivering that write.
pub struct Computed<T> {
    core: Arc<ComputedCore<T>>,
    _subscriptions: DisposeBag,
}

impl<T> Computed<T>
where
    T: Clone,
{
    pub fn get(&self) -> Result<T> {
        self.core.signal.get()
    }

    pub fn read_signal(&self) -> ReadSignal<T> {
        self.core.signal.read_only()
    }

    pub fn values(&self) -> ValueStream<T>
    where
        T: Send + Sync + 'static,
    {
        self.core.signal.values()
    }

    #[cfg(test)]
    pub(crate) fn set_evaluate_hook_for_test(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *self
            .core
            .test
            .evaluate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = hook;
    }

    #[cfg(test)]
    pub(crate) fn handbacks_for_test(&self) -> usize {
        self.core
            .test
            .handbacks
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn changes(&self) -> ValueStream<SignalChange<T>>
    where
        T: Send + Sync + 'static,
    {
        self.core.signal.changes()
    }
}

/// Input versions a value was computed from (unused slots stay 0).
type Versions = [u64; 3];

type Evaluate<T> = Box<dyn Fn() -> Result<(Versions, T)> + Send + Sync>;
/// Reads the inputs' versions; `true` first refreshes derived inputs.
type ReadVersions = Box<dyn Fn(bool) -> Result<Versions> + Send + Sync>;

struct CoreState {
    versions: Versions,
    dirty: bool,
    processing: bool,
}

struct ComputedCore<T> {
    signal: Signal<T>,
    evaluate: Evaluate<T>,
    read_versions: ReadVersions,
    /// `Some` for distinct values: publish only when the value changed.
    equal: Option<fn(&T, &T) -> bool>,
    state: Mutex<CoreState>,
    #[cfg(test)]
    test: CoreTestHooks,
}

#[cfg(test)]
#[derive(Default)]
struct CoreTestHooks {
    /// Runs inside every evaluation, before the inputs are read.
    evaluate: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// Times a re-entrant processor handed pending changes back to a refreshing frame.
    handbacks: std::sync::atomic::AtomicUsize,
}

thread_local! {
    /// Cores evaluating on this thread; a read of a core's own signal from inside its
    /// closure must not recurse into another evaluation.
    static EVALUATING: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

struct EvaluatingGuard(usize);

impl EvaluatingGuard {
    fn enter(address: usize) -> Option<Self> {
        EVALUATING.with(|evaluating| {
            let mut evaluating = evaluating.borrow_mut();
            if evaluating.contains(&address) {
                None
            } else {
                evaluating.push(address);
                Some(Self(address))
            }
        })
    }
}

impl Drop for EvaluatingGuard {
    fn drop(&mut self) {
        EVALUATING.with(|evaluating| {
            let mut evaluating = evaluating.borrow_mut();
            if let Some(position) = evaluating.iter().rposition(|entry| *entry == self.0) {
                evaluating.remove(position);
            }
        });
    }
}

/// Resets `processing` when the processing loop unwinds (a panicking closure), so
/// later input changes recompute again.
struct ProcessingGuard<'a> {
    state: &'a Mutex<CoreState>,
    armed: bool,
}

impl Drop for ProcessingGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            lock(self.state).processing = false;
        }
    }
}

fn lock(state: &Mutex<CoreState>) -> MutexGuard<'_, CoreState> {
    state.lock().unwrap_or_else(|poisoned| {
        state.clear_poison();
        poisoned.into_inner()
    })
}

/// `candidate` is newer than `published` when no input went backwards and one moved on.
fn supersedes(candidate: &Versions, published: &Versions) -> bool {
    candidate != published
        && candidate
            .iter()
            .zip(published)
            .all(|(candidate, published)| candidate >= published)
}

impl<T> ComputedCore<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn address(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    fn evaluate(&self) -> Option<Result<(Versions, T)>> {
        let _guard = EvaluatingGuard::enter(self.address())?;
        #[cfg(test)]
        {
            let hook = self
                .test
                .evaluate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        Some((self.evaluate)())
    }

    /// Publishes `value` computed from `versions`, unless an input moved on while it was
    /// evaluated (inputs are read one at a time, so the value may mix versions) or a value
    /// computed from newer inputs is already published.
    fn publish(&self, versions: Versions, value: T) -> Result<Publish> {
        let published = {
            let mut state = lock(&self.state);
            if (self.read_versions)(false)? != versions {
                return Ok(Publish::InputsMoved);
            }
            if !supersedes(&versions, &state.versions) {
                return Ok(Publish::Superseded);
            }
            state.versions = versions;
            // Pushed while the core lock is held, so publications keep their order.
            match self.equal {
                Some(equal) => self.signal.set_deferred_by(value, equal)?,
                None => {
                    self.signal.replace_deferred(value)?;
                    true
                }
            }
        };
        if published {
            self.signal.drain_changes()?;
        }
        Ok(Publish::Published)
    }

    fn is_stale(&self) -> Result<bool> {
        let versions = (self.read_versions)(true)?;
        Ok(versions != lock(&self.state).versions)
    }

    /// Brings the published value up to date with the inputs once.
    fn recompute(&self) -> Step {
        if !matches!(self.is_stale(), Ok(true)) {
            return Step::Done;
        }
        match self.evaluate() {
            None => Step::Reentrant,
            Some(Ok((versions, value))) => match self.publish(versions, value) {
                Ok(Publish::InputsMoved) => Step::Retry,
                _ => Step::Done,
            },
            Some(Err(_)) => Step::Done,
        }
    }

    /// Input-change handler: one thread processes changes at a time; changes arriving
    /// meanwhile (from any thread, or re-entrantly) mark the core dirty and are picked up.
    fn on_input_change(&self) {
        {
            let mut state = lock(&self.state);
            state.dirty = true;
            if state.processing {
                return;
            }
            state.processing = true;
        }
        self.process();
    }

    /// Runs while this thread owns `processing`.
    fn process(&self) {
        let mut guard = ProcessingGuard {
            state: &self.state,
            armed: true,
        };
        loop {
            {
                let mut state = lock(&self.state);
                if !state.dirty {
                    state.processing = false;
                    guard.armed = false;
                    return;
                }
                state.dirty = false;
            }
            match self.recompute() {
                Step::Done => {}
                Step::Retry => lock(&self.state).dirty = true,
                Step::Reentrant => {
                    #[cfg(test)]
                    self.test
                        .handbacks
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    // This core is evaluating further up this thread's stack (a read
                    // refreshing it); hand the pending change back to that frame.
                    let mut state = lock(&self.state);
                    state.dirty = true;
                    state.processing = false;
                    guard.armed = false;
                    return;
                }
            }
        }
    }
}

enum Publish {
    Published,
    Superseded,
    InputsMoved,
}

enum Step {
    Done,
    Retry,
    Reentrant,
}

impl<T> SignalRefresh for ComputedCore<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn refresh(&self) {
        loop {
            match self.recompute() {
                Step::Done => break,
                Step::Retry => {}
                Step::Reentrant => return,
            }
        }
        // Pick up changes a nested processor handed back while this frame evaluated.
        {
            let mut state = lock(&self.state);
            if !state.dirty || state.processing {
                return;
            }
            state.processing = true;
        }
        self.process();
    }
}

/// Builds a computed value: evaluates once, subscribes to the inputs, and evaluates
/// again only when an input changed in between.
fn build<T>(
    evaluate: Evaluate<T>,
    read_versions: ReadVersions,
    equal: Option<fn(&T, &T) -> bool>,
    subscribe: impl FnOnce(Arc<dyn Fn() + Send + Sync>, &DisposeBag) -> Result<()>,
) -> Result<Computed<T>>
where
    T: Clone + Send + Sync + 'static,
{
    let (versions, value) = evaluate()?;
    let core = Arc::new(ComputedCore {
        signal: Signal::new(value),
        evaluate,
        read_versions,
        equal,
        state: Mutex::new(CoreState {
            versions,
            dirty: false,
            processing: false,
        }),
        #[cfg(test)]
        test: CoreTestHooks::default(),
    });
    let hook: Weak<dyn SignalRefresh> = Arc::downgrade(&core) as Weak<dyn SignalRefresh>;
    core.signal.set_refresh_hook(hook);

    let subscriptions = DisposeBag::new();
    let core_for_changes = Arc::clone(&core);
    subscribe(
        Arc::new(move || core_for_changes.on_input_change()),
        &subscriptions,
    )?;
    if (core.read_versions)(false)? != versions {
        core.on_input_change();
    }

    Ok(Computed {
        core,
        _subscriptions: subscriptions,
    })
}

fn track<S>(
    source: &ReadSignal<S>,
    on_change: &Arc<dyn Fn() + Send + Sync>,
    bag: &DisposeBag,
) -> Result<()>
where
    S: Clone + Send + Sync + 'static,
{
    let on_change = Arc::clone(on_change);
    source
        .changes()
        .subscribe(move |_| on_change())?
        .dispose_with(bag);
    Ok(())
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
        Self::derive_with(source, derive, None)
    }

    /// Like [`derive`](Self::derive), but publishes only when the derived value changes.
    pub fn derive_distinct<S>(
        source: ReadSignal<S>,
        derive: impl Fn(&S) -> T + Send + Sync + 'static,
    ) -> Result<Self>
    where
        S: Clone + Send + Sync + 'static,
        T: PartialEq,
    {
        Self::derive_with(source, derive, Some(T::eq))
    }

    fn derive_with<S>(
        source: ReadSignal<S>,
        derive: impl Fn(&S) -> T + Send + Sync + 'static,
        equal: Option<fn(&T, &T) -> bool>,
    ) -> Result<Self>
    where
        S: Clone + Send + Sync + 'static,
    {
        let source_for_evaluate = source.clone();
        let source_for_versions = source.clone();
        build(
            Box::new(move || {
                let (version, value) = source_for_evaluate.read_versioned(S::clone)?;
                Ok(([version, 0, 0], derive(&value)))
            }),
            Box::new(move |fresh| Ok([source_for_versions.input_version(fresh), 0, 0])),
            equal,
            move |on_change, bag| track(&source, &on_change, bag),
        )
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
        let (first_for_evaluate, second_for_evaluate) = (first.clone(), second.clone());
        let (first_for_versions, second_for_versions) = (first.clone(), second.clone());
        build(
            Box::new(move || {
                let (first_version, first) = first_for_evaluate.read_versioned(A::clone)?;
                let (second_version, second) = second_for_evaluate.read_versioned(B::clone)?;
                Ok(([first_version, second_version, 0], combine(&first, &second)))
            }),
            Box::new(move |fresh| {
                Ok([
                    first_for_versions.input_version(fresh),
                    second_for_versions.input_version(fresh),
                    0,
                ])
            }),
            None,
            move |on_change, bag| {
                track(&first, &on_change, bag)?;
                track(&second, &on_change, bag)
            },
        )
    }

    /// Combines three signals; glitch-free like [`combine2`](Self::combine2).
    pub fn combine3<A, B, C>(
        first: ReadSignal<A>,
        second: ReadSignal<B>,
        third: ReadSignal<C>,
        combine: impl Fn(&A, &B, &C) -> T + Send + Sync + 'static,
    ) -> Result<Self>
    where
        A: Clone + Send + Sync + 'static,
        B: Clone + Send + Sync + 'static,
        C: Clone + Send + Sync + 'static,
    {
        let inputs = (first.clone(), second.clone(), third.clone());
        let inputs_for_versions = (first.clone(), second.clone(), third.clone());
        build(
            Box::new(move || {
                let (first_version, first) = inputs.0.read_versioned(A::clone)?;
                let (second_version, second) = inputs.1.read_versioned(B::clone)?;
                let (third_version, third) = inputs.2.read_versioned(C::clone)?;
                Ok((
                    [first_version, second_version, third_version],
                    combine(&first, &second, &third),
                ))
            }),
            Box::new(move |fresh| {
                Ok([
                    inputs_for_versions.0.input_version(fresh),
                    inputs_for_versions.1.input_version(fresh),
                    inputs_for_versions.2.input_version(fresh),
                ])
            }),
            None,
            move |on_change, bag| {
                track(&first, &on_change, bag)?;
                track(&second, &on_change, bag)?;
                track(&third, &on_change, bag)
            },
        )
    }
}
