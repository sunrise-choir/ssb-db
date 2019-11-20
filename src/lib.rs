#[macro_use]
extern crate diesel_migrations;
#[macro_use]
extern crate diesel;

pub use flumedb::flume_view::Sequence as FlumeSequence;

use flumedb::offset_log::OffsetLog;
use flumedb::FlumeLog;

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel_migrations::any_pending_migrations;
use itertools::Itertools;
use snafu::{OptionExt, ResultExt, Snafu};
use ssb_legacy_msg_data;
use ssb_legacy_msg_data::value::Value;
use ssb_multiformats::multihash::Multihash;
use ssb_multiformats::multikey::Multikey;

mod db;
mod ssb_message;

use ssb_message::SsbMessage;

use db::{
    append_item, find_feed_flume_seqs_newer_than, find_feed_latest_seq,
    find_message_flume_seq_by_key, get_latest,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("`include_keys` and `include_values` were both false. Pick one or both."))]
    IncludeKeysIncludeValuesBothFalse{},
    #[snafu(display("Could not encode legacy value as vec"))]
    EncodingValueAsVecError {},
    #[snafu(display("Error, tried to parse contents of db as legacy Value. This should never fail. The db may be corrupt. Rebuild the indexes"))]
    ErrorParsingAsLegacyValue {},
    #[snafu(display("Error, could not find message in db. {}", source))]
    MessageNotFound { source: db::Error },
    #[snafu(display("Error, could not find feed in db. {}", source))]
    FeedNotFound { source: db::Error },
    #[snafu(display("Error, could not batch append to offset file."))]
    OffsetAppendError {},
    #[snafu(display("Error, could not batch append to sqlite db."))]
    SqliteAppendError {},
    #[snafu(display("Error, could not find entry at expected offset."))]
    OffsetGetError {},
    #[snafu(display(
        "Error, could not get the latest sequence number from the db. {}",
        source
    ))]
    UnableToGetLatestSequence { source: db::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait SsbDb 
{
    fn append_batch<T: AsRef<[u8]>>(&mut self, feed_id: &Multikey, messages: &[T]) -> Result<()>;
    fn get_entry_by_key(&self, message_key: &Multihash) -> Result<Vec<u8>>;
    fn get_feed_latest_sequence(&self, feed_id: &Multikey) -> Result<Option<i32>>;
    fn get_entries_newer_than_sequence(
        &self,
        feed_id: &Multikey,
        sequence: i32,
        limit: Option<i64>,
        include_keys: bool,
        include_values: bool,
    ) -> Result<Vec<Vec<u8>>>;
    fn rebuild_indexes(&mut self) -> Result<()>;
}

pub struct SqliteSsbDb {
    connection: SqliteConnection,
    offset_log: OffsetLog<u32>,
    db_path: String,
}

embed_migrations!();

impl SqliteSsbDb {
    pub fn new<S: AsRef<str>>(database_path: S, offset_log_path: S) -> SqliteSsbDb {
        let connection = setup_connection(database_path.as_ref());

        let offset_log = match OffsetLog::new(&offset_log_path.as_ref()) {
            Ok(log) => log,
            Err(_) => {
                panic!("failed to open offset log at {}", offset_log_path.as_ref());
            }
        };
        SqliteSsbDb {
            connection,
            offset_log,
            db_path: database_path.as_ref().to_owned(),
        }
    }

    pub fn update_indexes_from_offset_file(&self) -> Result<()> {
        //We're using Max of flume_seq.
        //When the db is empty, we'll get None.
        //When there is one item in the db, we'll get 0 (it's the first seq number you get)
        //When there's more than one you'll get some >0 number
        let max_seq = get_latest(&self.connection)
            .context(UnableToGetLatestSequence)?
            .map(|val| val as u64);

        let num_to_skip: usize = match max_seq {
            None => 0,
            _ => 1,
        };

        let starting_offset = max_seq.unwrap_or(0);

        self.offset_log
            .iter_at_offset(starting_offset)
            .skip(num_to_skip)
            .chunks(10000)
            .into_iter()
            .map(|chunk| {
                self.connection
                    .transaction::<_, db::Error, _>(|| {
                        chunk
                            .map(|log_entry| {
                                append_item(&self.connection, log_entry.offset, &log_entry.data)?;

                                Ok(())
                            })
                            .collect::<std::result::Result<(), db::Error>>()
                    })
                    .map_err(|_| Error::SqliteAppendError {})
                    .and_then(|_| Ok(()))
            })
            .collect()
    }
}

impl SsbDb for SqliteSsbDb {
    fn append_batch<T: AsRef<[u8]>>(&mut self, _: &Multikey, messages: &[T]) -> Result<()> {
        // First, append the messages to flume
        self.offset_log
            .append_batch(messages)
            .map_err(|_| Error::OffsetAppendError {})?;

        self.update_indexes_from_offset_file()
    }
    fn get_entry_by_key<'a>(&'a self, message_key: &Multihash) -> Result<Vec<u8>> {
        let flume_seq =
            find_message_flume_seq_by_key(&self.connection, &message_key.to_legacy_string())
                .context(MessageNotFound)?;
        self.offset_log
            .get(flume_seq)
            .map_err(|_| Error::OffsetGetError {})
    }
    fn get_feed_latest_sequence(&self, feed_id: &Multikey) -> Result<Option<i32>> {
        find_feed_latest_seq(&self.connection, &feed_id.to_legacy_string()).context(FeedNotFound)
    }
    fn get_entries_newer_than_sequence<'a>(
        &'a self,
        feed_id: &Multikey,
        sequence: i32,
        limit: Option<i64>,
        include_keys: bool,
        include_values: bool,
    ) -> Result<Vec<Vec<u8>>> {
        let seqs = find_feed_flume_seqs_newer_than(
            &self.connection,
            &feed_id.to_legacy_string(),
            sequence,
            limit,
        )
        .context(FeedNotFound)?;

        match (include_keys, include_values) {
            (false, false) => Err(Error::IncludeKeysIncludeValuesBothFalse{}),
            (true, false) => seqs
                .iter()
                .flat_map(|seq| {
                    self.offset_log
                        .get(*seq)
                        .map_err(|_| Error::OffsetGetError {})
                })
                .flat_map(|msg| serde_json::from_slice::<SsbMessage>(&msg))
                .map(|msg| Ok(msg.key.into_bytes()))
                .collect(),
            (false, true) => {
                seqs.iter()
                    .flat_map(|seq| {
                        self.offset_log
                            .get(*seq)
                            .map_err(|_| Error::OffsetGetError {})
                    })
                    .flat_map(|msg| {
                        //If we're going to use Serde to pluck out the value we have to use
                        //ssb-legacy-data Value so that when we convert it back to a string, the
                        //ordering is still intact.
                        //If we don't do that then we would return a message that would fail
                        //verification
                        ssb_legacy_msg_data::json::from_slice(&msg)
                    })
                    .map(|legacy_value| {
                        if let Value::Object(legacy_val) = legacy_value {
                            let val = legacy_val.get("value").context(ErrorParsingAsLegacyValue)?;
                            ssb_legacy_msg_data::json::to_vec(&val, false)
                                .map_err(|_| Error::EncodingValueAsVecError {})
                        } else {
                            Err(Error::ErrorParsingAsLegacyValue {})
                        }
                    })
                    .collect()
            }
            (true, true) => seqs
                .iter()
                .map(|seq| {
                    self.offset_log
                        .get(*seq)
                        .map_err(|_| Error::OffsetGetError {})
                })
                .collect(),
        }
    }
    fn rebuild_indexes(&mut self) -> Result<()> {
        std::fs::remove_file(&self.db_path).unwrap();
        self.connection = setup_connection(&self.db_path);
        self.update_indexes_from_offset_file()
    }
}
fn setup_connection(database_path: &str) -> SqliteConnection {
    let database_url = to_sqlite_uri(database_path, "rwc");
    let connection = SqliteConnection::establish(&database_url)
        .expect(&format!("Error connecting to {}", database_url));

    if let Err(_) = any_pending_migrations(&connection) {
        embedded_migrations::run(&connection).unwrap();
    }

    if let Ok(true) = any_pending_migrations(&connection) {
        std::fs::remove_file(&database_path).unwrap();
        embedded_migrations::run(&connection).unwrap();
    }

    connection
}
fn to_sqlite_uri(path: &str, rw_mode: &str) -> String {
    format!("file:{}?mode={}", path, rw_mode)
}
#[cfg(test)]
mod tests {
    use crate::{SqliteSsbDb, SsbDb, SsbMessage};
    use crate::ssb_message::SsbValue;
    use ssb_multiformats::multihash::Multihash;
    use ssb_multiformats::multikey::Multikey;
    use flumedb::offset_log::OffsetLog;

    #[test]
    fn get_entry_by_key_works(){
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
    fn get_feed_latest_sequence_works(){
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
    fn get_entries_kv_newer_than_sequence_works(){
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_get_entries_kv.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db.get_entries_newer_than_sequence(&author, 6000, None, true, true).unwrap()
            .iter()
            .flat_map(|entry |serde_json::from_slice::<SsbMessage>(&entry))
            .collect::<Vec<_>>();

        assert_eq!(res.len(), 6);

        std::fs::remove_file(&db_path).unwrap();
    }
    #[test]
    fn get_entries_newer_than_sequence_works_with_limit(){
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_get_entries_kv_limit.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db.get_entries_newer_than_sequence(&author, 6000, Some(2), true, true).unwrap()
            .iter()
            .flat_map(|entry |serde_json::from_slice::<SsbMessage>(&entry))
            .collect::<Vec<_>>();

        assert_eq!(res.len(), 2);

        std::fs::remove_file(&db_path).unwrap();
    }
    #[test]
    fn get_entries_k_newer_than_sequence_works(){
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_get_entries_k.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db.get_entries_newer_than_sequence(&author, 6000, None, true, false).unwrap()
            .iter()
            .flat_map(|entry | Multihash::from_legacy(entry))
            .map(|(key,_)| key)
            .collect::<Vec<_>>();

        assert_eq!(res.len(), 6);

        std::fs::remove_file(&db_path).unwrap();
    }
    #[test]
    fn get_entries_v_newer_than_sequence_works(){
        //check message is valid
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_get_entries_v.sqlite3";
        let db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db.get_entries_newer_than_sequence(&author, 6000, None, false, true).unwrap()
            .iter()
            .flat_map(|entry | serde_json::from_slice::<SsbValue>(entry))
            .collect::<Vec<_>>();

        assert_eq!(res.len(), 6);

        std::fs::remove_file(&db_path).unwrap();
    }
    #[test]
    fn get_entries_no_kv_newer_than_sequence_errors(){
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
    fn append_batch_works(){
        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;
        let offset_log_path = "./test_vecs/piet.offset";
        let log = OffsetLog::<u32>::new(&offset_log_path).unwrap();

        let entries = log
            .iter()
            .map(|entry| entry.data)
            .collect::<Vec<_>>();

        let db_path = "/tmp/test_append_batch.sqlite3";
        let offset_path = "/tmp/test_append_batch.offset";
        let mut db = SqliteSsbDb::new(db_path, offset_path);

        let res = db.append_batch(&author, &entries.as_slice());
        assert!(res.is_ok());
        std::fs::remove_file(&db_path).unwrap();
        std::fs::remove_file(&offset_path).unwrap();
    }
    #[test]
    fn rebuild_indexes_works(){
        let expected_seq = 6006;

        let author_str = "@U5GvOKP/YUza9k53DSXxT0mk3PIrnyAmessvNfZl5E0=.ed25519";
        let author = Multikey::from_legacy(author_str.as_bytes()).unwrap().0;

        let db_path = "/tmp/test_rebuild_indexes.sqlite3";
        let mut db = SqliteSsbDb::new(db_path, "./test_vecs/piet.offset");
        db.update_indexes_from_offset_file().unwrap();

        let res = db.rebuild_indexes();

        assert!(res.is_ok());

        let res = db.get_feed_latest_sequence(&author);
        let seq = res.unwrap();

        assert_eq!(seq.unwrap(), expected_seq);

        std::fs::remove_file(&db_path).unwrap();
    }
}
