use std::sync::Arc;

use crate::collections::error::CollectionError;
use crate::primitives::ValueStream;

/// Operators on streams of `Option<T>`, for any stream error type.
pub trait OptionValueStreamExt<T, StreamError = CollectionError> {
    fn convert_some<U>(
        &self,
        converter: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ValueStream<Option<U>, StreamError>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static;

    fn convert_some_optional<U>(
        &self,
        converter: impl Fn(&T) -> Option<U> + Send + Sync + 'static,
    ) -> ValueStream<Option<U>, StreamError>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static;

    fn convert_or<U>(
        &self,
        converter: impl Fn(&T) -> U + Send + Sync + 'static,
        fallback: impl Fn() -> U + Send + Sync + 'static,
    ) -> ValueStream<U, StreamError>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static;

    fn or_else_option(&self, fallback: impl Fn() -> Option<T> + Send + Sync + 'static) -> Self
    where
        T: Clone + Send + Sync + 'static,
        Self: Sized;

    fn on_some(
        &self,
        action: impl Fn(&T) + Send + Sync + 'static,
        none_action: impl Fn() + Send + Sync + 'static,
    ) -> Self
    where
        T: Send + Sync + 'static,
        Self: Sized;

    fn on_none(
        &self,
        action: impl Fn() + Send + Sync + 'static,
        some_action: impl Fn(&T) + Send + Sync + 'static,
    ) -> Self
    where
        T: Send + Sync + 'static,
        Self: Sized;

    fn select_values(&self) -> ValueStream<T, StreamError>
    where
        T: Send + Sync + 'static;

    fn value_or(
        &self,
        fallback: impl Fn() -> T + Send + Sync + 'static,
    ) -> ValueStream<T, StreamError>
    where
        T: Clone + Send + Sync + 'static;

    fn value_or_default(&self) -> ValueStream<T, StreamError>
    where
        T: Clone + Default + Send + Sync + 'static;

    fn value_or_result<E>(
        &self,
        error: impl Fn() -> E + Send + Sync + 'static,
    ) -> ValueStream<Result<T, E>, StreamError>
    where
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static;
}

impl<T, StreamError> OptionValueStreamExt<T, StreamError> for ValueStream<Option<T>, StreamError>
where
    StreamError: Send + Sync + 'static,
{
    fn convert_some<U>(
        &self,
        converter: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ValueStream<Option<U>, StreamError>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let converter = Arc::new(converter);
        self.map(move |optional| optional.as_ref().map(|value| converter(value)))
    }

    fn convert_some_optional<U>(
        &self,
        converter: impl Fn(&T) -> Option<U> + Send + Sync + 'static,
    ) -> ValueStream<Option<U>, StreamError>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let converter = Arc::new(converter);
        self.map(move |optional| optional.as_ref().and_then(|value| converter(value)))
    }

    fn convert_or<U>(
        &self,
        converter: impl Fn(&T) -> U + Send + Sync + 'static,
        fallback: impl Fn() -> U + Send + Sync + 'static,
    ) -> ValueStream<U, StreamError>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let converter = Arc::new(converter);
        let fallback = Arc::new(fallback);
        self.map(move |optional| {
            optional
                .as_ref()
                .map(|value| converter(value))
                .unwrap_or_else(|| fallback())
        })
    }

    fn or_else_option(&self, fallback: impl Fn() -> Option<T> + Send + Sync + 'static) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        let fallback = Arc::new(fallback);
        self.map(move |optional| optional.clone().or_else(|| fallback()))
    }

    fn on_some(
        &self,
        action: impl Fn(&T) + Send + Sync + 'static,
        none_action: impl Fn() + Send + Sync + 'static,
    ) -> Self
    where
        T: Send + Sync + 'static,
    {
        let source = self.clone();
        let action = Arc::new(action);
        let none_action = Arc::new(none_action);
        ValueStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            let none_action = Arc::clone(&none_action);
            source.subscribe(move |optional| {
                if let Some(value) = optional {
                    action(value);
                } else {
                    none_action();
                }
                subscriber(optional);
            })
        })
    }

    fn on_none(
        &self,
        action: impl Fn() + Send + Sync + 'static,
        some_action: impl Fn(&T) + Send + Sync + 'static,
    ) -> Self
    where
        T: Send + Sync + 'static,
    {
        let source = self.clone();
        let action = Arc::new(action);
        let some_action = Arc::new(some_action);
        ValueStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            let some_action = Arc::clone(&some_action);
            source.subscribe(move |optional| {
                if let Some(value) = optional {
                    some_action(value);
                } else {
                    action();
                }
                subscriber(optional);
            })
        })
    }

    fn select_values(&self) -> ValueStream<T, StreamError>
    where
        T: Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            source.subscribe(move |optional| {
                if let Some(value) = optional {
                    subscriber(value);
                }
            })
        })
    }

    fn value_or(
        &self,
        fallback: impl Fn() -> T + Send + Sync + 'static,
    ) -> ValueStream<T, StreamError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let fallback = Arc::new(fallback);
        self.map(move |optional| optional.clone().unwrap_or_else(|| fallback()))
    }

    fn value_or_default(&self) -> ValueStream<T, StreamError>
    where
        T: Clone + Default + Send + Sync + 'static,
    {
        self.map(|optional| optional.clone().unwrap_or_default())
    }

    fn value_or_result<E>(
        &self,
        error: impl Fn() -> E + Send + Sync + 'static,
    ) -> ValueStream<Result<T, E>, StreamError>
    where
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let error = Arc::new(error);
        self.map(move |optional| optional.clone().ok_or_else(|| error()))
    }
}
