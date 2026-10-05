#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransformError<K, T, E> {
    pub error: E,
    pub key: K,
    pub value: T,
}

impl<K, T, E> TransformError<K, T, E> {
    pub fn new(error: E, key: K, value: T) -> Self {
        Self { error, key, value }
    }
}
