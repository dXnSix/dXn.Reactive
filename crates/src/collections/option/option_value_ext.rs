/// Helpers for `Option` values.
pub trait OptionValueExt<T> {
    /// Maps a `Some` value with `converter`; the same as `Option::map`.
    fn convert_some<U>(self, converter: impl FnOnce(T) -> U) -> Option<U>;

    /// Maps a `Some` value to another `Option` with `converter`; the same as `Option::and_then`.
    fn convert_some_optional<U>(self, converter: impl FnOnce(T) -> Option<U>) -> Option<U>;

    /// Maps a `Some` value with `converter`, or returns `fallback()` for `None`.
    fn convert_or<U>(self, converter: impl FnOnce(T) -> U, fallback: impl FnOnce() -> U) -> U;

    /// Returns `self` when it is `Some`, otherwise `fallback()`.
    fn or_else_option(self, fallback: impl FnOnce() -> Option<T>) -> Option<T>;

    /// Runs `action` with the value when it is `Some`, then returns `self`.
    fn on_some(self, action: impl FnOnce(&T)) -> Option<T>;

    /// Runs `action` when the value is `None`, then returns `self`.
    fn on_none(self, action: impl FnOnce()) -> Option<T>;

    /// Returns the `Some` value as `Ok`, or `Err(error())` for `None`.
    fn value_or_result<E>(self, error: impl FnOnce() -> E) -> Result<T, E>;
}

impl<T> OptionValueExt<T> for Option<T> {
    fn convert_some<U>(self, converter: impl FnOnce(T) -> U) -> Option<U> {
        self.map(converter)
    }

    fn convert_some_optional<U>(self, converter: impl FnOnce(T) -> Option<U>) -> Option<U> {
        self.and_then(converter)
    }

    fn convert_or<U>(self, converter: impl FnOnce(T) -> U, fallback: impl FnOnce() -> U) -> U {
        self.map_or_else(fallback, converter)
    }

    fn or_else_option(self, fallback: impl FnOnce() -> Option<T>) -> Option<T> {
        self.or_else(fallback)
    }

    fn on_some(self, action: impl FnOnce(&T)) -> Option<T> {
        if let Some(value) = &self {
            action(value);
        }
        self
    }

    fn on_none(self, action: impl FnOnce()) -> Option<T> {
        if self.is_none() {
            action();
        }
        self
    }

    fn value_or_result<E>(self, error: impl FnOnce() -> E) -> Result<T, E> {
        self.ok_or_else(error)
    }
}
