/// The error type of collection sources, operators and materialized collections.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum CollectionError {
    /// A lock guarding collection state was poisoned by a panic.
    #[error("collection state is poisoned")]
    Poisoned,
    /// A key-based operation named a key that the cache does not contain.
    #[error("key was not found")]
    MissingKey,
    /// An index-based operation named an index outside the list.
    #[error("index was out of range")]
    IndexOutOfRange,
    /// A source was accessed from inside its own edit (or key selector, or `T::clone`
    /// while its state is locked) on the same thread.
    #[error("nested updates are not supported")]
    NestedUpdate,
    /// Another thread holds the source's state and the non-blocking contention policy
    /// (used on `wasm32`) does not wait for it. Retry later, for example on the next frame.
    #[error("collection state is in use by another thread")]
    Contended,
}

/// A `ReactiveError::External` that wraps a `CollectionError` converts back to it; any
/// other external error has no collection equivalent and maps to `Poisoned`.
impl From<crate::primitives::ReactiveError> for CollectionError {
    fn from(error: crate::primitives::ReactiveError) -> Self {
        match error {
            crate::primitives::ReactiveError::Poisoned => Self::Poisoned,
            crate::primitives::ReactiveError::External(error) => error
                .downcast_ref::<CollectionError>()
                .cloned()
                .unwrap_or(Self::Poisoned),
        }
    }
}

/// Lets `?` carry collection errors into scalar code (for example inside
/// `ViewModelActivator::when_activated` callbacks): `Poisoned` stays `Poisoned`, every
/// other variant becomes `ReactiveError::External` holding the original error.
impl From<CollectionError> for crate::primitives::ReactiveError {
    fn from(error: CollectionError) -> Self {
        match error {
            CollectionError::Poisoned => Self::Poisoned,
            other => Self::external(other),
        }
    }
}

/// A `Result` whose error is [`CollectionError`].
pub type Result<T> = std::result::Result<T, CollectionError>;
