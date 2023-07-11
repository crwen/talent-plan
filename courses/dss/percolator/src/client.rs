use std::{collections::HashMap, thread, time};

use futures::executor::block_on;
use labrpc::*;

use crate::msg::{CommitRequest, GetRequest, PrewriteRequest, TimestampRequest};
use crate::service::{TSOClient, TransactionClient};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    start_ts: u64,
    mem_buffer: HashMap<Vec<u8>, Vec<u8>>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            start_ts: 0,
            mem_buffer: HashMap::new(),
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        for i in 0..RETRY_TIMES {
            match block_on(async { self.tso_client.get_timestamp(&TimestampRequest {}).await }) {
                Ok(response) => return Ok(response.timestamp),
                Err(err) => match err {
                    Error::Timeout => thread::sleep(time::Duration::from_millis(
                        (1 << i) * BACKOFF_TIME_MS as u64,
                    )),
                    other => return Err(other),
                },
            };
        }
        Err(Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        self.start_ts = self.get_timestamp().unwrap();
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        let get_request = GetRequest {
            key,
            start_ts: self.start_ts,
        };
        for i in 0..RETRY_TIMES {
            match block_on(async { self.txn_client.get(&get_request).await }) {
                Ok(response) => return Ok(response.value),
                Err(err) => match err {
                    Error::Timeout => thread::sleep(time::Duration::from_millis(
                        (1 << i) * BACKOFF_TIME_MS as u64,
                    )),
                    other => return Err(other),
                },
            }
        }
        Err(Error::Timeout)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        self.mem_buffer.insert(key, value);
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        if self.mem_buffer.is_empty() {
            return Ok(true);
        }

        let start_ts = self.start_ts;
        let mutation: Vec<(&Vec<u8>, &Vec<u8>)> = self.mem_buffer.iter().collect();

        let primary = &mutation[0];
        let secondaries = &mutation[1..];

        // Write primary first
        if !self.prewrite_wrap(
            primary.0.to_vec(),
            primary.1.to_vec(),
            primary.0.to_vec(),
            start_ts,
        )? {
            println!("Prewrite primary fail");
            return Ok(false);
        }

        for secondary in secondaries {
            if !self.prewrite_wrap(
                secondary.0.to_vec(),
                secondary.1.to_vec(),
                primary.0.to_vec(),
                start_ts,
            )? {
                println!("Prewrite secondary fail");
                return Ok(false);
            }
        }

        let commit_ts = self.get_timestamp()?;

        // Commit primary first
        if !self.commit_wrap(primary.0.to_vec(), start_ts, commit_ts, true)? {
            return Ok(false);
        }

        // commit secondary
        for secondary in secondaries {
            let _ = self.commit_wrap(secondary.0.to_vec(), start_ts, commit_ts, false);
        }

        Ok(true)
    }

    fn prewrite_wrap(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        primary: Vec<u8>,
        start_ts: u64,
    ) -> Result<bool> {
        let prewrite_request = PrewriteRequest {
            start_ts,
            primary,
            key,
            value,
        };
        for i in 0..RETRY_TIMES {
            match block_on(async { self.txn_client.prewrite(&prewrite_request).await }) {
                Ok(response) => return Ok(response.success),
                Err(err) => match err {
                    Error::Timeout => thread::sleep(time::Duration::from_millis(
                        (1 << i) * BACKOFF_TIME_MS as u64,
                    )),
                    // Error::Other(o) if o == "reqhook" => return Ok(false),
                    // Error::Recv(r) => return Ok(false),
                    other => return Err(other),
                },
            }
        }
        return Err(Error::Timeout);
    }

    fn commit_wrap(
        &self,
        key: Vec<u8>,
        start_ts: u64,
        commit_ts: u64,
        is_primary: bool,
    ) -> Result<bool> {
        let commit_request = CommitRequest {
            is_primary,
            start_ts,
            commit_ts,
            key,
        };
        for i in 0..RETRY_TIMES {
            match block_on(async { self.txn_client.commit(&commit_request).await }) {
                Ok(response) => return Ok(response.success),
                Err(err) => match err {
                    Error::Timeout => thread::sleep(time::Duration::from_millis(
                        (1 << i) * BACKOFF_TIME_MS as u64,
                    )),
                    Error::Other(o) if o == "reqhook" => return Ok(false),
                    // Error::Recv(r) => return Ok(false),
                    other => return Err(other),
                },
            }
        }
        return Err(Error::Timeout);
    }
}
