use crate::objectstore::ports::ObjectStoreReader;

pub struct WALReader<T>
where
    T: ObjectStoreReader,
{
    store: T,
}

impl<T> WALReader<T>
where
    T: ObjectStoreReader,
{
    pub fn new(store: T) -> Self {
        WALReader { store: store }
    }
}
