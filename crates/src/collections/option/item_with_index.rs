/// An item together with its position in a collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ItemWithIndex<T> {
    /// The item.
    pub item: T,
    /// The item's zero-based position.
    pub index: usize,
}

impl<T> ItemWithIndex<T> {
    /// Pairs `item` with `index`.
    pub fn new(item: T, index: usize) -> Self {
        Self { item, index }
    }
}
