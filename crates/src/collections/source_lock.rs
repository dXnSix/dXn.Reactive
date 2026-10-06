//! The lock behind every source and live group.
//!
//! Same-thread re-entry (a source accessed from inside its own edit closure, key selector,
//! or `T::clone` while its state is locked) fails with [`CollectionError::NestedUpdate`]
//! instead of deadlocking. Cross-thread contention follows a compile-time policy: native
//! targets wait for the lock; `wasm32` returns [`CollectionError::Contended`] without
//! blocking, because a browser main thread must not block on a lock a worker holds.

use std::cell::RefCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, TryLockError};

use crate::collections::error::{CollectionError, Result};

thread_local! {
    /// Addresses of the source locks this thread currently holds.
    static HELD: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
thread_local! {
    static FORCE_NON_BLOCKING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Runs `action` with the non-blocking (wasm32) contention policy on this thread.
#[cfg(test)]
pub(crate) fn with_non_blocking_policy<R>(action: impl FnOnce() -> R) -> R {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            FORCE_NON_BLOCKING.with(|force| force.set(self.0));
        }
    }
    let _restore = Restore(FORCE_NON_BLOCKING.with(|force| force.replace(true)));
    action()
}

fn non_blocking() -> bool {
    #[cfg(test)]
    if FORCE_NON_BLOCKING.with(std::cell::Cell::get) {
        return true;
    }
    cfg!(target_arch = "wasm32")
}

type ReleaseHook = Box<dyn Fn() + Send + Sync>;

pub(crate) struct SourceLock<S> {
    state: Mutex<S>,
    /// Work deferred to whoever releases the lock next (see [`SourceLock::defer`]).
    release_requested: AtomicBool,
    release_hook: OnceLock<ReleaseHook>,
}

impl<S> SourceLock<S> {
    pub(crate) fn new(state: S) -> Self {
        Self {
            state: Mutex::new(state),
            release_requested: AtomicBool::new(false),
            release_hook: OnceLock::new(),
        }
    }

    /// Installs the work run by [`defer`](Self::defer). It must not block on this lock:
    /// use [`try_lock_now`](Self::try_lock_now).
    pub(crate) fn set_release_hook(&self, hook: ReleaseHook) {
        let _ = self.release_hook.set(hook);
    }

    /// Asks the current holder to run the release hook when it releases the lock.
    pub(crate) fn request_release(&self) {
        self.release_requested.store(true, Ordering::SeqCst);
        std::sync::atomic::fence(Ordering::SeqCst);
    }

    fn run_release_hook(&self) {
        if self.release_requested.load(Ordering::SeqCst)
            && self.release_requested.swap(false, Ordering::SeqCst)
            && let Some(hook) = self.release_hook.get()
        {
            hook();
        }
    }

    /// Takes the lock only if nobody (this thread included) holds it. The guard is tracked
    /// like [`lock`](Self::lock)'s, so re-entry while it is held reports `NestedUpdate`.
    pub(crate) fn try_lock_now(&self) -> Option<SourceGuard<'_, S>> {
        let guard = self.state.try_lock().ok()?;
        HELD.with(|held| held.borrow_mut().push(self.address()));
        Some(SourceGuard {
            guard: Some(guard),
            lock: self,
        })
    }

    fn address(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    /// Locks the state for this thread, or reports re-entry/contention.
    pub(crate) fn lock(&self) -> Result<SourceGuard<'_, S>> {
        let address = self.address();
        if HELD.with(|held| held.borrow().contains(&address)) {
            return Err(CollectionError::NestedUpdate);
        }
        let guard = if non_blocking() {
            match self.state.try_lock() {
                Ok(guard) => guard,
                Err(TryLockError::WouldBlock) => return Err(CollectionError::Contended),
                Err(TryLockError::Poisoned(_)) => return Err(CollectionError::Poisoned),
            }
        } else {
            self.state.lock().map_err(|_| CollectionError::Poisoned)?
        };
        HELD.with(|held| held.borrow_mut().push(address));
        Ok(SourceGuard {
            guard: Some(guard),
            lock: self,
        })
    }

    /// Waits for the lock regardless of the policy. Only for short internal sections that
    /// run no user code and cannot be re-entered (delivery routing, disposal).
    pub(crate) fn lock_blocking(&self) -> Result<MutexGuard<'_, S>> {
        self.state.lock().map_err(|_| CollectionError::Poisoned)
    }
}

pub(crate) struct SourceGuard<'a, S> {
    guard: Option<MutexGuard<'a, S>>,
    lock: &'a SourceLock<S>,
}

impl<S> Drop for SourceGuard<'_, S> {
    fn drop(&mut self) {
        drop(self.guard.take());
        std::sync::atomic::fence(Ordering::SeqCst);
        let address = self.lock.address();
        HELD.with(|held| {
            let mut held = held.borrow_mut();
            if let Some(position) = held.iter().rposition(|entry| *entry == address) {
                held.remove(position);
            }
        });
        // Released: run work deferred while the lock was held.
        self.lock.run_release_hook();
    }
}

impl<S> Deref for SourceGuard<'_, S> {
    type Target = S;

    #[allow(clippy::expect_used)]
    fn deref(&self) -> &S {
        self.guard
            .as_ref()
            .expect("source guard is held until dropped")
    }
}

impl<S> DerefMut for SourceGuard<'_, S> {
    #[allow(clippy::expect_used)]
    fn deref_mut(&mut self) -> &mut S {
        self.guard
            .as_mut()
            .expect("source guard is held until dropped")
    }
}
