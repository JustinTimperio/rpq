use std::vec::Vec;

use redb::{Database, ReadableTableMetadata, TableDefinition};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::errors::DiskError;
use crate::schema;

pub struct DiskCache<T: Clone + Send> {
    db: Database,
    phantom: std::marker::PhantomData<T>,
}

const DB: TableDefinition<&str, &[u8]> = TableDefinition::new("rpq");

impl<T: Clone + Send> DiskCache<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn new(path: &str) -> Result<DiskCache<T>, DiskError> {
        let db = Database::create(path);

        match db {
            Ok(db) => {
                // Create the initial table
                let ctxn = db.begin_write().unwrap();
                ctxn.open_table(DB).unwrap();
                ctxn.commit().unwrap();

                Ok(DiskCache {
                    db,
                    phantom: std::marker::PhantomData,
                })
            }
            Err(e) => Err(DiskError::DatabaseError(e)),
        }
    }

    pub fn commit_batch(&self, write_cache: &mut Vec<schema::Item<T>>) -> Result<(), DiskError>
    where
        T: Serialize + DeserializeOwned,
    {
        let write_txn = self.db.begin_write().unwrap();
        for item in write_cache.iter() {
            let mut table = write_txn.open_table(DB).unwrap();
            let b = item.to_bytes();
            if b.is_err() {
                return Err(DiskError::ItemSerdeError(b.err().unwrap()));
            }

            let b = b.unwrap();
            let key = item.get_disk_uuid().unwrap();

            let was_written = table.insert(key.as_str(), &b[..]);
            if was_written.is_err() {
                return Err(DiskError::StorageError(was_written.err().unwrap()));
            }
        }

        write_txn.commit().unwrap();
        write_cache.clear();
        Ok(())
    }

    pub fn delete_batch(&self, delete_cache: &mut Vec<schema::Item<T>>) -> Result<(), DiskError>
    where
        T: Serialize + DeserializeOwned,
    {
        let write_txn = self.db.begin_write().unwrap();
        for item in delete_cache.iter() {
            let mut table = write_txn.open_table(DB).unwrap();
            let key = item.get_disk_uuid().unwrap();
            let was_deleted = table.remove(key.as_str());
            if was_deleted.is_err() {
                return Err(DiskError::StorageError(was_deleted.err().unwrap()));
            }
        }
        write_txn.commit().unwrap();

        delete_cache.clear();
        Ok(())
    }

    pub fn commit_single(&self, item: schema::Item<T>) -> Result<(), DiskError>
    where
        T: Serialize + DeserializeOwned,
    {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(DB).unwrap();
            let b = item.to_bytes();
            if b.is_err() {
                return Err(DiskError::ItemSerdeError(b.err().unwrap()));
            }

            let disk_uuid = item.get_disk_uuid();
            if disk_uuid.is_none() {
                return Err(DiskError::DiskUuidError);
            }

            let b = b.unwrap();
            let was_written = table.insert(disk_uuid.unwrap().as_str(), &b[..]);
            if was_written.is_err() {
                return Err(DiskError::StorageError(was_written.err().unwrap()));
            }
        }
        write_txn.commit().unwrap();

        Ok(())
    }

    pub fn delete_single(&self, key: &str) -> Result<(), DiskError>
    where
        T: Serialize + DeserializeOwned,
    {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(DB).unwrap();
            let was_removed = table.remove(key);
            if was_removed.is_err() {
                return Err(DiskError::StorageError(was_removed.err().unwrap()));
            }
        }
        write_txn.commit().unwrap();

        Ok(())
    }

    pub fn return_items_from_disk(&self) -> Result<Vec<schema::Item<T>>, DiskError>
    where
        T: Serialize + DeserializeOwned,
    {
        let mut items = Vec::new();
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(DB).unwrap();

        let cursor = match table.range::<&str>(..) {
            Ok(range) => range,
            Err(e) => return Err(DiskError::StorageError(e)),
        };

        // Restore the items from the disk cache
        for entry in cursor {
            match entry {
                Ok((_key, value)) => {
                    let item = schema::Item::from_bytes(value.value());
                    if item.is_err() {
                        return Err(DiskError::ItemSerdeError(item.err().unwrap()));
                    }

                    // Mark the item as restored
                    let mut i = item.unwrap();
                    i.set_restored();
                    items.push(i);
                }
                Err(e) => {
                    return Err(DiskError::StorageError(e));
                }
            }
        }
        _ = read_txn.close();

        Ok(items)
    }

    /// Returns the number of items in the database
    pub fn items_in_db(&self) -> usize {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(DB).unwrap();
        let count = table.len().unwrap();
        count as usize
    }
}
