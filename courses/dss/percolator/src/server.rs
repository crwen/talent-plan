use std::collections::BTreeMap;
use std::ptr::read;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{clone, thread};

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    timestamp: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        Ok(TimestampResponse {
            timestamp: self
                .timestamp
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.

        let start_ts = ts_start_inclusive.unwrap_or(0);
        let end_ts = ts_end_inclusive.unwrap_or(u64::MAX);

        let map = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };
        map.range((
            std::ops::Bound::Included((key.clone(), start_ts)),
            std::ops::Bound::Included((key, end_ts)),
        ))
        .last()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        let write_key = (key, ts);
        match column {
            Column::Write => self.write.insert(write_key, value),
            Column::Data => self.data.insert(write_key, value),
            Column::Lock => self.lock.insert(write_key, value),
        };
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        let delete_key = (key, commit_ts);
        match column {
            Column::Write => self.write.remove(&delete_key),
            Column::Data => self.data.remove(&delete_key),
            Column::Lock => self.lock.remove(&delete_key),
        };
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.

        loop {
            let mut table = self.data.lock().unwrap();
            // Check for locks that signal concurrent writes.
            if let Some(_) = table.read(req.key.clone(), Column::Lock, None, Some(req.start_ts)) {
                // There is a pending lock; try to clean it and wait
                drop(table);
                self.back_off_maybe_clean_up_lock(req.start_ts, req.key.clone());
                continue;
            }

            // Find the latest write below out start_ts
            if let Some((key, value)) =
                table.read(req.key.clone(), Column::Write, None, Some(req.start_ts))
            {
                if let Value::Timestamp(data_ts) = value {
                    if let Some((_, val)) = table.read(
                        req.key.clone(),
                        Column::Data,
                        Some(*data_ts),
                        Some(*data_ts),
                    ) {
                        if let Value::Vector(res) = val {
                            return Ok(GetResponse {
                                value: res.to_vec(),
                            });
                        } else {
                            // no data
                            panic!("Value type doesn't match! Expect Vector!")
                        }
                    } else {
                        return Ok(GetResponse { value: Vec::new() });
                        // panic!("Could not find the corresponding value!")
                    }
                } else {
                    panic!("Value type doesn't match! Expect Timestamp!")
                }
            } else {
                return Ok(GetResponse { value: Vec::new() });
            }
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.

        let mut table = self.data.lock().unwrap();
        // Abort on writes after out start timestamp
        if let Some(_) = table.read(req.key.clone(), Column::Write, Some(req.start_ts), None) {
            return Ok(PrewriteResponse { success: false });
        }
        // or locks at any timestamp
        if let Some(_) = table.read(req.key.clone(), Column::Lock, None, None) {
            return Ok(PrewriteResponse { success: false });
        }

        table.write(
            req.key.clone(),
            Column::Data,
            req.start_ts,
            Value::Vector(req.value),
        );
        table.write(
            req.key,
            Column::Lock,
            req.start_ts,
            Value::Vector(req.primary),
        );
        Ok(PrewriteResponse { success: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        let mut table = self.data.lock().unwrap();
        // if req.is_primary {
        if let None = table.read(
            req.key.clone(),
            Column::Lock,
            Some(req.start_ts),
            Some(req.start_ts),
        ) {
            return Ok(CommitResponse { success: false });
        }
        // }
        table.write(
            req.key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        info!("Erase Lock {:?}", req.key.clone());
        table.erase(req.key, Column::Lock, req.start_ts);
        Ok(CommitResponse { success: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        thread::sleep(Duration::from_nanos(TTL));
        let mut table = self.data.lock().unwrap();
        // get lock first, to find primary
        if let Some((lock_key, lock_value)) =
            table.read(key.clone(), Column::Lock, None, Some(start_ts))
        {
            let lock_start_ts = lock_key.1;
            // find primary lock
            let primary_key;
            if let Value::Vector(primary) = lock_value {
                primary_key = primary;
            } else {
                panic!("Unexpected lock column value");
            }
            // determine rollback or roll-forward
            // match table.read(primary_key.clone(), Column::Lock, Some(lock_start_ts), Some(lock_start_ts)) {
            //     Some(_) => {
            //         // has primary lock, means txn hasn't been committed, rollback
            //         table.erase(key, Column::Lock, lock_start_ts);
            //     },
            //     None => {
            //         // no primary lock, means txn has been committed, so roll-forward
            //         // find primary commit write, to get commit ts
            //         match table.read(primary_key.to_vec(), Column::Write, Some(lock_start_ts), Some(u64::MAX)) {
            //             Some((primary_write_key, primary_write_value)) => {
            //                 if let Value::Timestamp(ts) = primary_write_value {
            //                     if *ts == lock_start_ts {
            //                         let commit_ts = primary_write_key.1;
            //                         table.write(key.clone(), Column::Write, commit_ts, Value::Timestamp(lock_start_ts));
            //                         table.erase(key, Column::Lock, lock_start_ts);
            //                     }
            //                 }
            //             },
            //             None => {},
            //         }
            //     }
            // };

            match table.read(
                primary_key.to_vec(),
                Column::Write,
                Some(lock_start_ts),
                Some(u64::MAX),
            ) {
                Some((primary_write_key, primary_write_value)) => {
                    if let Value::Timestamp(ts) = primary_write_value {
                        if *ts == lock_start_ts {
                            // roll-forward
                            let commit_ts = primary_write_key.1;
                            table.write(
                                key.clone(),
                                Column::Write,
                                commit_ts,
                                Value::Timestamp(lock_start_ts),
                            );
                            table.erase(key, Column::Lock, lock_start_ts);
                        }
                    } else {
                        panic!("Unexpected write column value");
                    }
                }
                None => {
                    // rollback
                    table.erase(key, Column::Lock, lock_start_ts);
                }
            }
        }
    }
}
