#[macro_use]
extern crate diesel_migrations;

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel_migrations::any_pending_migrations;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use ssb_multiformats::multihash::Multihash;
use ssb_multiformats::multikey::Multikey;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(""))]
    InvalidPreviousMessage {},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

trait SsbDb {
    fn append_batch(&mut self, feed_id: &Multikey, messages: &[&[u8]]) -> Result<()>;
    fn get_entry_by_key<'a>(&self, message_key: &Multihash) -> Result<&'a [u8]>;
    fn get_feed_latest_sequence(&self, feed_id: &Multikey) -> Result<i32>;
    fn get_entries_newer_than_sequence<'a>(
        &'a self,
        feed_id: &Multikey,
        sequence: i32,
        limit: Option<i32>,
        include_keys: bool,
        include_values: bool,
    ) -> Result<&'a [&'a [u8]]>;
    fn rebuild_indexes(&mut self) -> Result<()>;
}

pub struct SqliteSsbDb {
    connection: SqliteConnection,
}

embed_migrations!();

impl SqliteSsbDb {
    pub fn new(database_path: String) -> SqliteSsbDb {
        let database_url = to_sqlite_uri(&database_path, "rwc");
        let connection = SqliteConnection::establish(&database_url)
            .expect(&format!("Error connecting to {}", database_url));

        if let Err(_) = any_pending_migrations(&connection) {
            embedded_migrations::run(&connection).unwrap();
        }

        if let Ok(true) = any_pending_migrations(&connection) {
            std::fs::remove_file(&database_url).unwrap();
            embedded_migrations::run(&connection).unwrap();
        }

        SqliteSsbDb { connection }
    }
}

fn to_sqlite_uri(path: &str, rw_mode: &str) -> String {
    format!("file:{}?mode={}", path, rw_mode)
}

impl SsbDb for SqliteSsbDb {
    fn append_batch(&mut self, feed_id: &Multikey, messages: &[&[u8]]) -> Result<()> {
        unimplemented!();
    }
    fn get_entry_by_key<'a>(&self, message_key: &Multihash) -> Result<&'a [u8]> {
        unimplemented!();
    }
    fn get_feed_latest_sequence(&self, feed_id: &Multikey) -> Result<i32> {
        unimplemented!();
    }
    fn get_entries_newer_than_sequence<'a>(
        &'a self,
        feed_id: &Multikey,
        sequence: i32,
        limit: Option<i32>,
        include_keys: bool,
        include_values: bool,
    ) -> Result<&'a [&'a [u8]]> {
        unimplemented!();
    }
    fn rebuild_indexes(&mut self) -> Result<()> {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
