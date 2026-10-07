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

/// A panic inside an async transform's future, as reported by the safe async transforms.
///
/// A safe async transform reports a panicking future through its `on_error` callback when
/// the panic can be expressed as its error type `E`: when `E` is `TransformPanic`, `String`
/// (the message), `Box<dyn Error + Send + Sync>` or `Arc<dyn Error + Send + Sync>` (holding a
/// `TransformPanic`), or when the panic payload is itself an `E`
/// ([`std::panic::panic_any`]). Otherwise the item is skipped, as in the other variants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransformPanic {
    message: String,
}

impl TransformPanic {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// The panic message (`"panicked"` when the payload was not a string).
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for TransformPanic {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "transform panicked: {}", self.message)
    }
}

impl std::error::Error for TransformPanic {}

type PanicPayload = Box<dyn std::any::Any + Send>;

/// The message of a panic payload.
pub(crate) fn panic_message(payload: &PanicPayload) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "panicked".to_string()
    }
}

/// Expresses a panic as the error type `E`, when `E` can carry it (see [`TransformPanic`]).
pub(crate) fn panic_as_error<E: 'static>(payload: PanicPayload) -> Option<E> {
    let message = panic_message(&payload);
    let payload = match payload.downcast::<E>() {
        Ok(error) => return Some(*error),
        Err(payload) => payload,
    };
    drop(payload);
    let panic = TransformPanic::new(message.clone());
    let candidates: [Box<dyn std::any::Any>; 4] = [
        Box::new(panic.clone()),
        Box::new(message),
        Box::new(Box::new(panic.clone()) as Box<dyn std::error::Error + Send + Sync>),
        Box::new(std::sync::Arc::new(panic) as std::sync::Arc<dyn std::error::Error + Send + Sync>),
    ];
    candidates
        .into_iter()
        .find_map(|candidate| candidate.downcast::<E>().ok().map(|error| *error))
}
