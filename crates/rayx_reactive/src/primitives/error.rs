#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReactiveError {
    #[error("reactive state is poisoned")]
    Poisoned,
}

pub type Result<T> = std::result::Result<T, ReactiveError>;
