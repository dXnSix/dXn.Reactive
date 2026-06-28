#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ItemWithIndex<T> {
    pub item: T,
    pub index: usize,
}

impl<T> ItemWithIndex<T> {
    pub fn new(item: T, index: usize) -> Self {
        Self { item, index }
    }
}
