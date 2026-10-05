use std::sync::{Arc, Mutex};

use crate::primitives::{ReactiveError, Result};

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
}

pub trait ActivatableViewModel {
    fn activator(&self) -> &ViewModelActivator;
}

pub struct ActivationToken {
    activator: ViewModelActivator,
    generation: u64,
    active: bool,
}

impl ViewModelActivator {
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
            && let Err(error) = callback(&active_bag)
        {
            let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
            state
                .callbacks
                .retain(|registered| !Arc::ptr_eq(registered, &callback));
            return Err(error);
        }

        Ok(())
    }

    pub fn activate(&self) -> Result<ActivationToken> {
        let (generation, callbacks) = {
            let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
            if let Some(generation) = state.active_generation.as_mut() {
                generation.active_count = generation.active_count.saturating_add(1);
                (generation.id, None)
            } else {
                state.next_generation = state.next_generation.wrapping_add(1).max(1);
                let generation = state.next_generation;
                let bag = Arc::new(DisposeBag::new());
                state.active_generation = Some(ActivationGeneration {
                    id: generation,
                    active_count: 1,
                    bag: Arc::clone(&bag),
                });
                (generation, Some((bag, state.callbacks.clone())))
            }
        };

        if let Some((bag, callbacks)) = callbacks {
            for callback in callbacks {
                if let Err(error) = callback(&bag) {
                    self.detach_generation(generation)?;
                    bag.dispose();
                    return Err(error);
                }
            }
        }

        Ok(ActivationToken {
            activator: self.clone(),
            generation,
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
                state
                    .active_generation
                    .take()
                    .map(|generation| generation.bag)
            } else {
                None
            }
        });

        if let Some(bag) = bag {
            bag.dispose();
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
