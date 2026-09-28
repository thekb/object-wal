use crate::objectstore::ports::*;
use tokio::sync::{Mutex, mpsc};

/// WALWriter implements a WAL appender on top of object store using the
/// protocol described in https://nvartolomei.com/oswald/#appending.
pub struct WALWriter<'a, T>
where
    T: ObjectStoreWriter + ObjectStoreReader,
{
    store: &'a T,
}
