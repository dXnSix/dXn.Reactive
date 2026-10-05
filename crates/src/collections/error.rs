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

impl From<crate::primitives::ReactiveError> for CollectionError {
    fn from(error: crate::primitives::ReactiveError) -> Self {
        match error {
            crate::primitives::ReactiveError::Poisoned => Self::Poisoned,
        }
    }
}

pub type Result<T> = std::result::Result<T, CollectionError>;
