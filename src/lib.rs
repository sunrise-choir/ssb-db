#[macro_use]
extern crate diesel_migrations;
#[macro_use]
extern crate diesel;

pub use flumedb::flume_view::Sequence as FlumeSequence;
use flumedb::offset_log::OffsetLog;

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel_migrations::any_pending_migrations;
use itertools::Itertools;
use snafu::{ResultExt, Snafu};
use ssb_multiformats::multihash::Multihash;
use ssb_multiformats::multikey::Multikey;

mod db;
mod ssb_message;

use db::{
    find_feed_flume_seqs_newer_than, find_feed_latest_seq, find_message_flume_seq_by_key,
    get_latest, 
    append_item
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error, could not find message in db. {}", source))]
    MessageNotFound { source: db::Error },
    #[snafu(display("Error, could not find feed in db. {}", source))]
    FeedNotFound { source: db::Error },
    #[snafu(display("Error, could not batch append to offset file."))]
    OffsetAppendError {},
    #[snafu(display(
        "Error, could not get the latest sequence number from the db. {}",
        source
    ))]
    UnableToGetLatestSequence { source: db::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

trait SsbDb {
    fn append_batch(&mut self, feed_id: &Multikey, messages: &[&[u8]]) -> Result<()>;
    fn get_entry_by_key<'a>(&self, message_key: &Multihash) -> Result<&'a [u8]>;
    fn get_feed_latest_sequence(&self, feed_id: &Multikey) -> Result<FlumeSequence>;
    fn get_entries_newer_than_sequence<'a>(
        &'a self,
        feed_id: &Multikey,
        sequence: i32,
        limit: Option<i64>,
        include_keys: bool,
        include_values: bool,
    ) -> Result<&'a [&'a [u8]]>;
    fn rebuild_indexes(&mut self) -> Result<()>;
}

pub struct SqliteSsbDb {
    connection: SqliteConnection,
    offset_log: OffsetLog<u32>,
}

embed_migrations!();

impl SqliteSsbDb {
    pub fn new<S: AsRef<str>>(database_path: S, offset_log_path: S) -> SqliteSsbDb {
        let database_url = to_sqlite_uri(database_path.as_ref(), "rwc");
        let connection = SqliteConnection::establish(&database_url)
            .expect(&format!("Error connecting to {}", database_url));

        if let Err(_) = any_pending_migrations(&connection) {
            embedded_migrations::run(&connection).unwrap();
        }

        if let Ok(true) = any_pending_migrations(&connection) {
            std::fs::remove_file(&database_url).unwrap();
            embedded_migrations::run(&connection).unwrap();
        }

        let offset_log = match OffsetLog::new(&offset_log_path.as_ref()) {
            Ok(log) => log,
            Err(_) => {
                panic!("failed to open offset log at {}", offset_log_path.as_ref());
            }
        };
        SqliteSsbDb {
            connection,
            offset_log,
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
            .for_each(|chunk| {
                self.connection
                    .transaction::<_, db::Error, _>(|| {
                        chunk.map(|log_entry| {
                            append_item(&self.connection, log_entry.offset, &log_entry.data)?;
                            
                            Ok(())
                        })
                        .collect::<std::result::Result<(), db::Error>>()
                    })
                    .unwrap();
            });
        Ok(())
    }
}

impl SsbDb for SqliteSsbDb {
    fn append_batch(&mut self, _: &Multikey, messages: &[&[u8]]) -> Result<()> {
        // First, append the messages to flume
        self.offset_log
            .append_batch(messages)
            .map_err(|_| snafu::NoneError)
            .context(OffsetAppendError)?;

        self.update_indexes_from_offset_file()
    }
    fn get_entry_by_key<'a>(&self, message_key: &Multihash) -> Result<&'a [u8]> {
        let flume_seq =
            find_message_flume_seq_by_key(&self.connection, &message_key.to_legacy_string())
                .context(MessageNotFound)?;
        unimplemented!();
    }
    fn get_feed_latest_sequence(&self, feed_id: &Multikey) -> Result<FlumeSequence> {
        find_feed_latest_seq(&self.connection, &feed_id.to_legacy_string()).context(FeedNotFound)
    }
    fn get_entries_newer_than_sequence<'a>(
        &'a self,
        feed_id: &Multikey,
        sequence: i32,
        limit: Option<i64>,
        include_keys: bool,
        include_values: bool,
    ) -> Result<&'a [&'a [u8]]> {
        find_feed_flume_seqs_newer_than(
            &self.connection,
            &feed_id.to_legacy_string(),
            sequence,
            limit,
        )
        .context(FeedNotFound)?;
        unimplemented!();
    }
    fn rebuild_indexes(&mut self) -> Result<()> {
        unimplemented!();
    }
}

fn to_sqlite_uri(path: &str, rw_mode: &str) -> String {
    format!("file:{}?mode={}", path, rw_mode)
}
#[cfg(test)]
mod tests {
    use crate::{SsbDb, SqliteSsbDb};
    #[test]
    fn it_opens_a_connection_ok() {
        SqliteSsbDb::new("./test.sqlite3", "/home/piet/.ssb/flume/log.offset");
    }
    #[test]
    fn it_process_eeerything() {
        let db = SqliteSsbDb::new("./test2.sqlite3", "/home/piet/.ssb/flume/log.offset");
        let res = db.update_indexes_from_offset_file();
        assert!(res.is_ok());
    }
}
