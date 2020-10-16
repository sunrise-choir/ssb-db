use flumedb::offset_log::OffsetLog;
use flumedb::{FlumeLog, IterAtOffset};

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel_migrations::any_pending_migrations;
use itertools::Itertools;
use snafu::{OptionExt, ResultExt};
use ssb_legacy_msg_data;
use ssb_legacy_msg_data::value::Value;
use ssb_multiformats::multihash::Multihash;
use ssb_multiformats::multikey::Multikey;
use std::cell::RefCell;

use crate::db;
use crate::error::*;
use crate::ssb_message::SsbMessage;
use crate::SsbDb;

use db::{
    append_item, find_feed_flume_seqs_newer_than, find_feed_latest_seq,
    find_message_flume_seq_by_author_and_sequence, find_message_flume_seq_by_key, get_latest,
};

pub struct SqliteSsbDb {
    connection: RefCell<SqliteConnection>,
    offset_log: RefCell<OffsetLog<u32>>,
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
            connection: RefCell::new(connection),
            offset_log: RefCell::new(offset_log),
            db_path: database_path.as_ref().to_owned(),
        }
    }

    pub fn update_indexes_from_offset_file(&self) -> Result<()> {
        //We're using Max of flume_seq.
        //When the db is empty, we'll get None.
        //When there is one item in the db, we'll get 0 (it's the first seq number you get)
        //When there's more than one you'll get some >0 number

        let connection = self.connection.borrow_mut();
        let offset_log = self.offset_log.borrow();

        let max_seq = get_latest(&connection)
            .context(UnableToGetLatestSequence)?
            .map(|val| val as u64);

        let num_to_skip: usize = match max_seq {
            None => 0,
            _ => 1,
        };

        let starting_offset = max_seq.unwrap_or(0);

        offset_log
            .iter_at_offset(starting_offset)
            .skip(num_to_skip)
            .chunks(10000)
            .into_iter()
            .map(|chunk| {
                connection
                    .transaction::<_, db::Error, _>(|| {
                        chunk
                            .map(|log_entry| {
                                append_item(&connection, log_entry.offset, &log_entry.data)?;

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
    fn append_batch<T: AsRef<[u8]>>(&self, _: &Multikey, messages: &[T]) -> Result<()> {
        // First, append the messages to flume
        self.offset_log
            .borrow_mut()
            .append_batch(messages)
            .map_err(|_| Error::OffsetAppendError {})?;

        self.update_indexes_from_offset_file()
    }
    fn get_entry_by_key<'a>(&'a self, message_key: &Multihash) -> Result<Vec<u8>> {
        let flume_seq = find_message_flume_seq_by_key(
            &self.connection.borrow(),
            &message_key.to_legacy_string(),
        )
        .context(MessageNotFound)?;
        self.offset_log
            .borrow()
            .get(flume_seq)
            .map_err(|_| Error::OffsetGetError {})
    }

    fn get_entry_by_seq(&self, feed_id: &Multikey, sequence: i32) -> Result<Option<Vec<u8>>> {
        let flume_seq = find_message_flume_seq_by_author_and_sequence(
            &self.connection.borrow(),
            &feed_id.to_legacy_string(),
            sequence,
        )
        .context(MessageNotFound)?;

        flume_seq
            .map(|flume_seq| {
                self.offset_log
                    .borrow()
                    .get(flume_seq as u64)
                    .map_err(|_| Error::OffsetGetError {})
            })
            .transpose()
    }
    fn get_feed_latest_sequence(&self, feed_id: &Multikey) -> Result<Option<i32>> {
        find_feed_latest_seq(&self.connection.borrow(), &feed_id.to_legacy_string())
            .context(FeedNotFound)
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
            &self.connection.borrow(),
            &feed_id.to_legacy_string(),
            sequence,
            limit,
        )
        .context(FeedNotFound)?;

        match (include_keys, include_values) {
            (false, false) => Err(Error::IncludeKeysIncludeValuesBothFalse {}),
            (true, false) => seqs
                .iter()
                .flat_map(|seq| {
                    self.offset_log
                        .borrow()
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
                            .borrow()
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
                        .borrow()
                        .get(*seq)
                        .map_err(|_| Error::OffsetGetError {})
                })
                .collect(),
        }
    }
    fn rebuild_indexes(&self) -> Result<()> {
        std::fs::remove_file(&self.db_path).unwrap();
        self.connection.replace(setup_connection(&self.db_path));
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
