use std::sync::{Arc, Mutex};

use rayx_reactive_primitives::{ReactiveError, Result};

use crate::disposal::DisposeBag;

type ActivationCallback = Arc<dyn Fn(&DisposeBag) -> Result<()> + Send + Sync>;

#[derive(Clone, Default)]
pub struct ViewModelActivator {
    state: Arc<Mutex<ActivationState>>,
}

#[derive(Default)]
struct ActivationState {
    callbacks: Vec<ActivationCallback>,
    active_count: usize,
    active_bag: Option<DisposeBag>,
}

pub trait ActivatableViewModel {
    fn activator(&self) -> &ViewModelActivator;
}

pub struct ActivationToken {
    activator: ViewModelActivator,
    active: bool,
}

impl ViewModelActivator {
    pub fn when_activated(
        &self,
        callback: impl Fn(&DisposeBag) -> Result<()> + Send + Sync + 'static,
    ) -> Result<()> {
        let callback: ActivationCallback = Arc::new(callback);
        let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
        if let Some(active_bag) = state.active_bag.as_ref() {
            callback(active_bag)?;
        }
        state.callbacks.push(callback);
        Ok(())
    }

    pub fn activate(&self) -> Result<ActivationToken> {
        let callbacks = {
            let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
            state.active_count = state.active_count.saturating_add(1);
            if state.active_count == 1 {
                Some(state.callbacks.clone())
            } else {
                None
            }
        };

        if let Some(callbacks) = callbacks {
            let bag = DisposeBag::new();
            for callback in callbacks {
                if let Err(error) = callback(&bag) {
                    bag.dispose();
                    let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
                    state.active_count = state.active_count.saturating_sub(1);
                    return Err(error);
                }
            }

            let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
            state.active_bag = Some(bag);
        }

        Ok(ActivationToken {
            activator: self.clone(),
            active: true,
        })
    }

    fn deactivate(&self) {
        let bag = self
            .state
            .lock()
            .ok()
            .and_then(|mut state| {
                if state.active_count > 0 {
                    state.active_count -= 1;
                }

                if state.active_count == 0 {
                    state.active_bag.take()
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
            self.activator.deactivate();
            self.active = false;
        }
    }
}
