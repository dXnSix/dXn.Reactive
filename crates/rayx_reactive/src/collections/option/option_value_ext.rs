pub trait OptionValueExt<T> {
    fn convert_some<U>(self, converter: impl FnOnce(T) -> U) -> Option<U>;

    fn convert_some_optional<U>(self, converter: impl FnOnce(T) -> Option<U>) -> Option<U>;

    fn convert_or<U>(self, converter: impl FnOnce(T) -> U, fallback: impl FnOnce() -> U) -> U;

    fn or_else_option(self, fallback: impl FnOnce() -> Option<T>) -> Option<T>;

    fn on_some(self, action: impl FnOnce(&T)) -> Option<T>;

    fn on_none(self, action: impl FnOnce()) -> Option<T>;

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
