use redb::{DatabaseError, StorageError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RPQError {
    #[error("Disk error")]
    DiskError(DiskError),
    #[error("Item error")]
    ItemError(ItemError),

    #[error("Error sending to channel")]
    ChannelSendError,

    #[error("Error receiving from channel")]
    ChannelRecvError,
}

#[derive(Error, Debug)]
pub enum DiskError {
    #[error("Database error")]
    DatabaseError(#[from] DatabaseError),

    #[error("No disk uuid was set")]
    DiskUuidError,

    #[error("Error on the disk cache")]
    StorageError(#[from] StorageError),

    #[error("Error de/serializing item")]
    ItemSerdeError(#[from] ItemError),

    #[error("Disk cache not initialized")]
    DiskCacheNotInitialized,
}

#[derive(Error, Debug)]
pub enum ItemError {
    #[error("Error de/serializing item")]
    ItemSerdeError(#[from] bincode::Error),

    #[error("Empty byte array during deserialization")]
    EmptyByteArray,
}
