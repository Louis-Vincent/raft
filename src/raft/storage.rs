
use std::{iter::{self, zip}, marker::PhantomData, ops::RangeFrom, os::linux::raw, path::Path};

use rust_rocksdb::{ColumnFamilyRef, DBAccess, DBRawIteratorWithThreadMode, IteratorMode, Options, WriteBatch, WriteBatchWithTransaction, DB};

use crate::raft::common::{LogIndex, Term};

const LOG: &str = "LOG";

const COMMIT: &[u8] = b"COMMIT";

#[derive(Debug, PartialEq)]
pub enum CommitError {
    LogIndexNotFound,
    // Raised when attempted to commit index i, but index "i" - last commit index > 1
    CommitHole,
}

#[derive(Debug, PartialEq)]
pub enum WriteError {
    // Raised when attempted to update a commit log index.
    AttemptToOverwriteCommited
}

#[derive(Debug, PartialEq)]
pub enum DeleteError {
    // Raised when attempted to delete a commit log index.
    AttemptDeleteCommited
}


struct Nothing;

impl From<&[u8]> for Nothing {
    fn from(value: &[u8]) -> Self {
        Nothing
    }
}

pub trait LogStorage {

    fn append(&mut self, bytes: &[u8]) -> LogIndex {
        let new_log_idx = self.get_last_log_index().unwrap_or(0) + 1;
        let batch = [bytes];
        self.insert_slice_at(new_log_idx, &batch).unwrap();
        return new_log_idx
    }

    fn get(&self, i: LogIndex) -> Option<Vec<u8>>;

    fn delete_from(&mut self, log_index_range: RangeFrom<LogIndex>) -> Result<(), DeleteError>;

    fn insert_slice_at<S: AsRef<[u8]>>(&mut self, start_idx: LogIndex, batch: &[S]) -> Result<(), WriteError>;

    fn get_last_commited_log_index(&self) -> Option<LogIndex>;

    fn commit_log(&mut self, i: LogIndex) -> Result<(), CommitError>;

    fn get_last<T: for<'a> From<&'a [u8]>>(&self) -> Option<(LogIndex, T)>;

    fn get_last_log_index(&self) -> Option<LogIndex> {
        self.get_last::<Nothing>().map(|tuple| tuple.0)
    }

    fn iter<V: for<'a> From<&'a [u8]>>(&self) -> impl Iterator<Item=(LogIndex, V)>;
}


pub struct PersistentStorage {
    inner: DB
}

impl PersistentStorage {

    pub fn open_default<P: AsRef<Path>>(path: P) -> Self {
        let mut db = PersistentStorage { inner: DB::open_default(path).unwrap() };
        let opts = Options::default();
        db.inner.create_cf(LOG, &opts).unwrap();
        return db;
    }

    fn get_log_handle(&self) -> ColumnFamilyRef {
        self.inner.cf_handle(LOG).expect("Could not find RocksDB log column family")
    }
}

pub struct LogIterator<'a, T: for<'b> From<&'b [u8]>, D: DBAccess> {
    inner: DBRawIteratorWithThreadMode<'a, D>,
    t: PhantomData<T>
}

impl <'a, T: for<'b> From<&'b [u8]>, D: DBAccess> LogIterator<'a, T, D> {
    fn new(mut raw_iter: DBRawIteratorWithThreadMode<'a, D>) -> Self {
        raw_iter.seek_to_first();
        LogIterator {
            inner: raw_iter,
            t: PhantomData::default()
        }
    }
}

impl <T: for<'b> From<&'b [u8]>, D: DBAccess> Iterator for LogIterator<'_, T, D> {
    type Item = (LogIndex, T);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.inner.valid() {
            self.inner.status().unwrap();
            None
        } else {
            let ret = self.inner.item().map(|(k, v)| 
                (
                    LogIndex::from_be_bytes(k.try_into().expect("Invalid slice for LogIndex")),
                    T::from(v)
                ) 
            );
            self.inner.next();
            return ret;
        }
    }
}

impl LogStorage for PersistentStorage {


    fn get(&self, i: LogIndex) -> Option<Vec<u8>> {
        self.inner
            .get_cf(self.get_log_handle(), i.to_be_bytes())
            .unwrap()
    }

    fn delete_from(&mut self, log_index_range: RangeFrom<LogIndex>) -> Result<(), DeleteError> {
        if let Some(i) = self.get_last_commited_log_index() {
            if log_index_range.contains(&i) {
                return Err(DeleteError::AttemptDeleteCommited)
            }
        }
        let from_key = log_index_range.start.to_be_bytes();
        let mut batch: WriteBatchWithTransaction<false> = WriteBatch::default();
        batch.delete_range_cf(self.get_log_handle(), from_key, LogIndex::MAX.to_be_bytes());
        self.inner.write(batch).unwrap();
        return Ok(());
    }

    fn insert_slice_at<S: AsRef<[u8]>>(&mut self, start_idx: LogIndex, batch: &[S]) -> Result<(), WriteError> {
        if batch.len() == 0 {
            return Ok(());
        }

        let end_idx = start_idx + (batch.len() as LogIndex) - 1;

        let last_commit_log_index = self.get_last_commited_log_index().unwrap_or(0);
        if last_commit_log_index >= start_idx && last_commit_log_index <= end_idx {
            return Err(WriteError::AttemptToOverwriteCommited);
        }

        let mut wb = WriteBatch::default();
        let zip_it = zip(start_idx.., batch);
        for (i, entry) in zip_it {

            wb.put_cf(self.get_log_handle(), i.to_be_bytes(), entry.as_ref());
        }
        self.inner.write(wb).unwrap();
        return Ok(());
    }
 
    fn get_last_commited_log_index(&self) -> Option<LogIndex> {
        self.inner
            .get(COMMIT)
            .unwrap()
            .map(|bytestring| 
                LogIndex::from_be_bytes(
                    bytestring[..8]
                        .try_into()
                        .expect("Invalid bytestring for commit log value")
                )
            )
    }
    
    fn get_last<T: for<'a> From<&'a [u8]>>(&self) -> Option<(LogIndex, T)> {
        let mut raw_iter = self.inner.raw_iterator_cf(self.get_log_handle());
        raw_iter.seek_to_last();
        if raw_iter.valid() {
            raw_iter.item().map(|(k, v)| 
                (
                    LogIndex::from_be_bytes(k.try_into().expect("Invalid slice for LogIndex")),
                    T::from(v)
                ) 
            )
        } else {
            raw_iter.status().expect("Unable to reach the end of the log column family");
            None
        }
    }

    
    fn commit_log(&mut self, i: LogIndex) -> Result<(), CommitError> {

        let diff = i - self.get_last_commited_log_index().unwrap_or(0);
        
        if diff > 1 {
            return Err(CommitError::CommitHole)
        }
        if diff <= 0 {
            // The index "i" has already been committed.
            return Ok(())
        }

        if self.get(i).is_none() {
            return Err(CommitError::LogIndexNotFound);
        }

        let _ = self.inner.put(COMMIT, i.to_be_bytes()).expect("Failed to write to commit log");

        return Ok(())   
    }
    
    fn iter<V: for<'a> From<&'a [u8]>>(&self) -> impl Iterator<Item=(LogIndex, V)> {
        let raw_iter: DBRawIteratorWithThreadMode<_> = self.inner.raw_iterator_cf(self.get_log_handle());
        LogIterator::new(raw_iter)
    }
}

#[cfg(test)]
mod tests {
    use rust_rocksdb::{Options, DB};

    use crate::raft::{common::LogIndex, storage::CommitError};

    use super::{LogStorage, PersistentStorage};

    fn get_db() -> PersistentStorage {
        let opts = Options::default();
        DB::destroy(&opts, "./test_db").unwrap();
        PersistentStorage::open_default("./test_db")
    }


    #[test]
    fn it_should_return_none_if_empty_log() {
        let db = get_db();

        assert!(db.get(1).is_none());
        assert!(db.get_last_log_index().is_none());
        assert!(db.get_last_commited_log_index().is_none());
    }

    #[test]
    fn it_should_append_key() {
        let mut db = get_db();
        let log_index = db.append(b"test");
        let actual = db.get(log_index).unwrap();
        assert_eq!(actual, b"test");
        assert_eq!(db.get_last_log_index(), Some(1));
        assert_eq!(db.get_last_commited_log_index(), None);
    }

    #[test]
    fn it_should_commit() {
        let mut db = get_db();
        let log_index = db.append( b"test");
        db.commit_log(log_index).unwrap();
        assert_eq!(db.get_last_log_index(), Some(1));
        assert_eq!(db.get_last_commited_log_index(), Some(1));
    }

    #[test]
    fn commit_should_be_idempotent() {
        let mut db = get_db();

        let log_index = db.append(b"test");
        
        let _once = db.commit_log(log_index).unwrap();
        let _twice = db.commit_log(log_index).unwrap();
        
        assert_eq!(db.get_last_log_index(), Some(1));
        assert_eq!(db.get_last_commited_log_index(), Some(1));
    }

    #[test]
    fn it_should_return_log_index_not_found_when_committing() {
        let mut db = get_db();
        let actual = db.commit_log(1);

        assert_eq!(actual, Err(CommitError::LogIndexNotFound));
    }

    #[test]
    fn it_should_not_allow_commit_with_gap() {
        let mut db = get_db();

        let log_index1 = db.append(b"test1");
        let log_index2 = db.append(b"test2");

        // Commit log_index2 before committing log_index1
        let actual = db.commit_log(log_index2);

        assert!(log_index2 > log_index1);
        assert_eq!(db.get_last_log_index(), Some(log_index2));
        assert_eq!(db.get_last_commited_log_index(), None);

        assert_eq!(actual, Err(CommitError::CommitHole));
    }
        
    #[test]
    fn it_should_insert_a_slice_between_indices_1_and_5() {
        let mut db = get_db();

        let _log_index1 = db.append(b"test1");
        let _log_index2 = db.append(b"test2");
        let _log_index3 = db.append(b"test3");
        let _log_index4 = db.append(b"test4");
        let _log_index5 = db.append(b"test5");

        let my_slice = vec![
            b"overwrite2",
            b"overwrite3",
            b"overwrite4",
        ];
        db.insert_slice_at(2, &my_slice).unwrap();

        let all: Vec<(LogIndex, Vec<u8>)> = db.iter::<Vec<u8>>().collect();
        let expected = vec![
            (1,  b"test1".to_vec()),
            (2,  b"overwrite2".to_vec()),
            (3,  b"overwrite3".to_vec()),
            (4,  b"overwrite4".to_vec()),
            (5,  b"test5".to_vec()),
        ];
        assert_eq!(all, expected);
        assert_eq!(db.get_last_commited_log_index(), None);
    }

}