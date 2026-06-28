pub trait OptionIteratorExt<T>: IntoIterator<Item = Option<T>> + Sized {
    fn select_values(self) -> Vec<T>;
}

impl<I, T> OptionIteratorExt<T> for I
where
    I: IntoIterator<Item = Option<T>> + Sized,
{
    fn select_values(self) -> Vec<T> {
        self.into_iter().flatten().collect()
    }
}
