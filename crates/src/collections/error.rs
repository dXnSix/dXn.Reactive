#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CollectionError {
    #[error("collection state is poisoned")]
    Poisoned,
    #[error("key was not found")]
    MissingKey,
    #[error("index was out of range")]
    IndexOutOfRange,
    #[error("nested updates are not supported")]
    NestedUpdate,
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

pub type Result<T> = std::result::Result<T, CollectionError>;
