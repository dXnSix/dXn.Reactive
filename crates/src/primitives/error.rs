use std::error::Error;
use std::sync::Arc;

#[derive(Clone, Debug, thiserror::Error)]
pub enum ReactiveError {
    #[error("reactive state is poisoned")]
    Poisoned,
    /// An error from another layer (for example a collection operation) carried into scalar
    /// code, so `?` works across the boundary. The original error is preserved.
    #[error(transparent)]
    External(Arc<dyn Error + Send + Sync>),
}

impl ReactiveError {
    /// Wraps any error as [`ReactiveError::External`].
    pub fn external(error: impl Error + Send + Sync + 'static) -> Self {
        Self::External(Arc::new(error))
    }
}

/// `External` errors are equal when they are the same error or display the same message.
impl PartialEq for ReactiveError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Poisoned, Self::Poisoned) => true,
            (Self::External(left), Self::External(right)) => {
                Arc::ptr_eq(left, right) || left.to_string() == right.to_string()
            }
            _ => false,
        }
    }
}

impl Eq for ReactiveError {}

pub type Result<T> = std::result::Result<T, ReactiveError>;
