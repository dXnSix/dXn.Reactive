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

pub type Result<T> = std::result::Result<T, CollectionError>;
