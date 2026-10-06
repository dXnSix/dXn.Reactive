use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use crate::primitives::{ReactiveError, Result, Subscription};

use crate::disposal::DisposeBag;

type ActivationCallback = Arc<dyn Fn(&DisposeBag) -> Result<()> + Send + Sync>;

#[derive(Clone, Default)]
pub struct ViewModelActivator {
    state: Arc<Mutex<ActivationState>>,
}

#[derive(Default)]
struct ActivationState {
    callbacks: Vec<ActivationCallback>,
    next_generation: u64,
    active_generation: Option<ActivationGeneration>,
}

struct ActivationGeneration {
    id: u64,
    active_count: usize,
    bag: Arc<DisposeBag>,
    phase: Arc<GenerationPhase>,
}

const ACTIVATING: u8 = 0;
const ACTIVE: u8 = 1;
const ENDED: u8 = 2;

/// Outcome of one activation generation, shared with its tokens.
struct GenerationPhase(AtomicU8);

impl GenerationPhase {
    fn activating() -> Arc<Self> {
        Arc::new(Self(AtomicU8::new(ACTIVATING)))
    }

    fn set(&self, phase: u8) {
        self.0.store(phase, Ordering::SeqCst);
    }

    fn is_active(&self) -> bool {
        self.0.load(Ordering::SeqCst) == ACTIVE
    }
}

pub trait ActivatableViewModel {
    fn activator(&self) -> &ViewModelActivator;
}

pub struct ActivationToken {
    activator: ViewModelActivator,
    generation: u64,
    phase: Arc<GenerationPhase>,
    active: bool,
}

impl ActivationToken {
    /// `true` while this token is held and its generation finished activating
    /// successfully. A token that joined a generation whose callbacks then failed reports
    /// `false`.
    pub fn is_active(&self) -> bool {
        self.active && self.phase.is_active()
    }
}

/// Runs `callback` against a child bag and attaches it to `bag` on success; on failure
/// the work the callback registered before failing is disposed.
fn run_registration(
    callback: &(dyn Fn(&DisposeBag) -> Result<()> + Send + Sync),
    bag: &DisposeBag,
) -> Result<()> {
    let child = DisposeBag::new();
    match callback(&child) {
        Ok(()) => {
            bag.add(Subscription::new(move || child.dispose()));
            Ok(())
        }
        Err(error) => {
            child.dispose();
            Err(error)
        }
    }
}

impl ViewModelActivator {
    /// `true` while a generation is active (its activation callbacks all succeeded and at
    /// least one token is held).
    pub fn is_active(&self) -> bool {
        self.state.lock().is_ok_and(|state| {
            state
                .active_generation
                .as_ref()
                .is_some_and(|generation| generation.phase.is_active())
        })
    }

    pub fn when_activated(
        &self,
        callback: impl Fn(&DisposeBag) -> Result<()> + Send + Sync + 'static,
    ) -> Result<()> {
        let callback: ActivationCallback = Arc::new(callback);
        let active_bag = {
            let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
            let active_bag = state
                .active_generation
                .as_ref()
                .map(|generation| Arc::clone(&generation.bag));
            state.callbacks.push(Arc::clone(&callback));
            active_bag
        };

        if let Some(active_bag) = active_bag
            && let Err(error) = run_registration(callback.as_ref(), &active_bag)
        {
            let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
            state
                .callbacks
                .retain(|registered| !Arc::ptr_eq(registered, &callback));
            return Err(error);
        }

        Ok(())
    }

    /// Runs `callback` once for the current activation generation only.
    ///
    /// Unlike [`when_activated`](Self::when_activated), nothing is kept for later
    /// activations: the work belongs to the generation that is active (or activating)
    /// now and is disposed when it ends, so registering from inside an activation
    /// callback runs once per activation instead of accumulating. Without an active
    /// generation the callback is not run. When the callback fails, the work it
    /// registered before failing is disposed and the error is returned.
    pub fn when_activated_scoped(
        &self,
        callback: impl Fn(&DisposeBag) -> Result<()> + Send + Sync,
    ) -> Result<()> {
        let active_bag = {
            let state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
            state
                .active_generation
                .as_ref()
                .map(|generation| Arc::clone(&generation.bag))
        };
        match active_bag {
            Some(active_bag) => run_registration(&callback, &active_bag),
            None => Ok(()),
        }
    }

    pub fn activate(&self) -> Result<ActivationToken> {
        let (generation, phase, callbacks) = {
            let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
            if let Some(generation) = state.active_generation.as_mut() {
                generation.active_count = generation.active_count.saturating_add(1);
                (generation.id, Arc::clone(&generation.phase), None)
            } else {
                state.next_generation = state.next_generation.wrapping_add(1).max(1);
                let generation = state.next_generation;
                let bag = Arc::new(DisposeBag::new());
                let phase = GenerationPhase::activating();
                state.active_generation = Some(ActivationGeneration {
                    id: generation,
                    active_count: 1,
                    bag: Arc::clone(&bag),
                    phase: Arc::clone(&phase),
                });
                (generation, phase, Some((bag, state.callbacks.clone())))
            }
        };

        if let Some((bag, callbacks)) = callbacks {
            // Ends, detaches and disposes the generation if a callback fails or panics.
            let mut abandon = AbandonGeneration {
                activator: self,
                generation,
                phase: &phase,
                bag: &bag,
                armed: true,
            };
            for callback in callbacks {
                callback(&bag)?;
            }
            abandon.armed = false;
            phase.set(ACTIVE);
        }

        Ok(ActivationToken {
            activator: self.clone(),
            generation,
            phase,
            active: true,
        })
    }

    fn detach_generation(&self, generation_id: u64) -> Result<Option<Arc<DisposeBag>>> {
        let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
        if state
            .active_generation
            .as_ref()
            .is_some_and(|generation| generation.id == generation_id)
        {
            Ok(state
                .active_generation
                .take()
                .map(|generation| generation.bag))
        } else {
            Ok(None)
        }
    }

    fn deactivate(&self, generation_id: u64) {
        let bag = self.state.lock().ok().and_then(|mut state| {
            let generation = state.active_generation.as_mut()?;
            if generation.id != generation_id {
                return None;
            }

            generation.active_count = generation.active_count.saturating_sub(1);
            if generation.active_count == 0 {
                state.active_generation.take().map(|generation| {
                    generation.phase.set(ENDED);
                    generation.bag
                })
            } else {
                None
            }
        });

        if let Some(bag) = bag {
            bag.dispose();
        }
    }
}

struct AbandonGeneration<'a> {
    activator: &'a ViewModelActivator,
    generation: u64,
    phase: &'a GenerationPhase,
    bag: &'a DisposeBag,
    armed: bool,
}

impl Drop for AbandonGeneration<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.phase.set(ENDED);
            let _ = self.activator.detach_generation(self.generation);
            self.bag.dispose();
        }
    }
}

impl Drop for ActivationToken {
    fn drop(&mut self) {
        if self.active {
            self.activator.deactivate(self.generation);
            self.active = false;
        }
    }
}
