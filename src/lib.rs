//! # ssb-db
//!
//! The most basic db that you need to do (legacy) replication on ssb.
//!
//! ## Legacy Replication
//!
//! "Legacy Replication" is how ssb used to do replication before
//! [ebt](https://github.com/dominictarr/epidemic-broadcast-trees)
//!
//! It's simpler than ebt, but uses more bandwidth.
//!
//! To do legacy replication a client calls
//! [createHistoryStream](https://scuttlebot.io/apis/scuttlebot/ssb.html#createhistorystream-source)
//! for each feed it wants to replicate, passing the largest sequence number it knows about.
//!
//! ## SsbDb
//!
//! `ssb-db` defines a trait [SsbDb] that provides all the functionality you should need
//! to make and handle legacy replication requests.
//!
//! ## Architecture
//!
//! [SqliteSsbDb] implements the [SsbDb] trait.
//!
//! The underlying architecture is based on [flume-db](https://github.com/sunrise-choir/flumedb-rs).
//!
//! `ssb-db` stores data in an append only log. It maintains indexes for querying the log in sqlite.
//! The append only log is the source of truth and the indexes are derived from the log. If the
//! indexes break or need to be migrated, the sqlite db can be deleted and rebuilt from the log.
//!
//! ## Validation
//!
//! `ssb-db` **does not validate any messages before appending them.** The caller must validate them first.
//! See [ssb-validate](https://github.com/sunrise-choir/ssb-validate) and [ssb-verify-signatures](https://github.com/sunrise-choir/ssb-verify-signatures).
//!
#[macro_use]
extern crate diesel_migrations;
#[macro_use]
extern crate diesel;

pub use flumedb::flume_view::Sequence as FlumeSequence;

mod db;
pub mod error;
pub mod sqlite_ssb_db;
mod ssb_message;

pub use error::Error;
pub use sqlite_ssb_db::SqliteSsbDb;

use error::Result;
use ssb_multiformats::multihash::Multihash;
use ssb_multiformats::multikey::Multikey;

pub trait SsbDb {
    /// Append a batch of valid ssb messages authored by the `feed_id`.
    fn append_batch<T: 'static + AsRef<[u8]>>(&self, feed_id: &Multikey, messages: &[T]) -> Result<()>;
    /// Get an entry by its ssb message key.
    fn get_entry_by_key(&self, message_key: &Multihash) -> Result<Vec<u8>>;
    /// Get an entry by its sequence key + author.
    fn get_entry_by_seq(&self, feed_id: &Multikey, sequence: i32) -> Result<Option<Vec<u8>>>;
    /// Get the latest sequence number for the given feed.
    fn get_feed_latest_sequence(&self, feed_id: &Multikey) -> Result<Option<i32>>;
    /// Get all the entries for the given `feed_id`, with a sequence larger than `sequence`.
    ///
    /// You may `limit` the maximum number of entries to get.
    ///
    /// You can control whether to `include_keys`, `include_values`, or both.
    ///
    /// If `include_values` and `include_values` are both `false`
    /// `get_entries_newer_than_sequence` will return an `Error`.
    fn get_entries_newer_than_sequence(
        &self,
        feed_id: &Multikey,
        sequence: i32,
        limit: Option<i64>,
        include_keys: bool,
        include_values: bool,
    ) -> Result<Vec<Vec<u8>>>;
    /// You can rebuild the indexes in sqlite db (but not the offset file) if they become
    /// corrupted.
    fn rebuild_indexes(&self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use crate::ssb_message::{SsbMessage, SsbValue};
    use crate::{SqliteSsbDb, SsbDb};
    use flumedb::offset_log::OffsetLog;
    use ssb_multiformats::multihash::Multihash;
    use ssb_multiformats::multikey::Multikey;

    #[test]
    fn get_entry_by_key_works() {
        let key_str = "%/v5mCnV/kmnVtnF3zXtD4tbzoEQo4kRq/0d/bgxP1WI=.sha256";
        let key = Multihash::from_legacy(key_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_get_entry_by_key.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db.get_entry_by_key(&key);
        let entry = res.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&entry).unwrap();

        let actual_key_str: &str = value["key"].as_str().unwrap();

        assert_eq!(actual_key_str, key_str);
        std::fs::remove_file(&db_path).unwrap();
    }

    #[test]
    fn get_feed_latest_sequence_works() {
        let expected_seq = 6006;
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_get_latest_seq.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db.get_feed_latest_sequence(&author);
        let seq = res.unwrap();

        assert_eq!(seq.unwrap(), expected_seq);
        std::fs::remove_file(&db_path).unwrap();
    }
    #[test]
    fn get_entries_kv_newer_than_sequence_works() {
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_get_entries_kv.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db
            .get_entries_newer_than_sequence(&author, 6000, None, true, true)
            .unwrap()
            .iter()
            .flat_map(|entry| serde_json::from_slice::<SsbMessage>(&entry))
            .collect::<Vec<_>>();

        assert_eq!(res.len(), 6);

        std::fs::remove_file(&db_path).unwrap();
    }
    #[test]
    fn get_entries_newer_than_sequence_works_with_limit() {
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_get_entries_kv_limit.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db
            .get_entries_newer_than_sequence(&author, 6000, Some(2), true, true)
            .unwrap()
            .iter()
            .flat_map(|entry| serde_json::from_slice::<SsbMessage>(&entry))
            .collect::<Vec<_>>();

        assert_eq!(res.len(), 2);

        std::fs::remove_file(&db_path).unwrap();
    }
    #[test]
    fn get_entries_k_newer_than_sequence_works() {
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_get_entries_k.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db
            .get_entries_newer_than_sequence(&author, 6000, None, true, false)
            .unwrap()
            .iter()
            .flat_map(|entry| Multihash::from_legacy(entry))
            .map(|(key, _)| key)
            .collect::<Vec<_>>();

        assert_eq!(res.len(), 6);

        std::fs::remove_file(&db_path).unwrap();
    }
    #[test]
    fn get_entries_v_newer_than_sequence_works() {
        //check message is valid
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_get_entries_v.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db
            .get_entries_newer_than_sequence(&author, 6000, None, false, true)
            .unwrap()
            .iter()
            .flat_map(|entry| serde_json::from_slice::<SsbValue>(entry))
            .collect::<Vec<_>>();

        assert_eq!(res.len(), 6);

        std::fs::remove_file(&db_path).unwrap();
    }
    #[test]
    fn get_entries_no_kv_newer_than_sequence_errors() {
        //check message is valid
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_get_entries_no_kv.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db.get_entries_newer_than_sequence(&author, 6000, None, false, false);

        assert!(res.is_err());

        std::fs::remove_file(&db_path).unwrap();
    }
    #[test]
    fn append_batch_works() {
        let expected_seq = 6006;
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;
        let offset_log_path = "./test_vecs/piet.offset";
        let log = OffsetLog::<u32>::new(&offset_log_path).unwrap();

        let entries = log.iter().map(|entry| entry.data).collect::<Vec<_>>();

        let db_path = "/tmp/test_append_batch.sqlite3";
        let offset_path = "/tmp/test_append_batch.offset";
        let db = SqliteSsbDb::new(db_path, offset_path);

        let res = db.append_batch(&author, &entries.as_slice());
        assert!(res.is_ok());

        let res = db.get_feed_latest_sequence(&author);
        let seq = res.unwrap();
        assert_eq!(seq.unwrap(), expected_seq);

        std::fs::remove_file(&db_path).unwrap();
        std::fs::remove_file(&offset_path).unwrap();
    }
    #[test]
    fn rebuild_indexes_works() {
        let expected_seq = 6006;

        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_rebuild_indexes.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db.rebuild_indexes();

        assert!(res.is_ok());

        let res = db.get_feed_latest_sequence(&author);
        let seq = res.unwrap();

        assert_eq!(seq.unwrap(), expected_seq);

        std::fs::remove_file(&db_path).unwrap();
    }
}
