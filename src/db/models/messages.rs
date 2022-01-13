use super::keys::*;
use crate::db::{Error, SqliteConnection};

use crate::db::schema::authors::dsl::{
    author as authors_author, authors as authors_table, id as authors_id,
};
use crate::db::schema::keys::dsl::{id as keys_id, key as keys_key, keys as keys_table};
use crate::db::schema::messages;
use crate::db::schema::messages::dsl::{
    author_id as messages_author_id, flume_seq as messages_flume_seq, key_id as messages_key_id,
    messages as messages_table, seq as messages_seq,
};
use diesel::dsl::max;
use diesel::insert_into;
use diesel::prelude::*;
use flumedb::flume_view::Sequence as FlumeSequence;

#[derive(Queryable, Insertable, Associations, Identifiable, Debug, Default)]
#[table_name = "messages"]
#[primary_key(flume_seq)]
#[belongs_to(Key)]
pub struct Message {
    pub flume_seq: Option<i64>,
    pub seq: i32,
    pub key_id: i32,
    pub author_id: i32,
}

pub fn get_latest(connection: &SqliteConnection) -> Result<Option<f64>, Error> {
    messages_table
        .select(max(messages_flume_seq))
        .first(connection)
        .map(|res: Option<i64>| res.map(|val| val as f64))
}

pub fn insert_message(
    connection: &SqliteConnection,
    seq: i32,
    flume_seq: i64,
    message_key_id: i32,
    author_id: i32,
) -> Result<usize, Error> {
    let message = Message {
        flume_seq: Some(flume_seq),
        key_id: message_key_id,
        seq,
        author_id,
    };

    insert_into(messages_table)
        .values(message)
        .execute(connection)
}

pub fn find_message_flume_seq_by_key(
    connection: &SqliteConnection,
    key: &str,
) -> Result<FlumeSequence, Error> {
    let flume_seq = keys_table
        .inner_join(messages_table.on(messages_key_id.nullable().eq(keys_id)))
        .select(messages_flume_seq)
        .filter(keys_key.eq(key.clone()))
        .first::<i64>(connection)? as u64;

    Ok(flume_seq)
}

pub fn find_message_flume_seq_by_author_and_sequence(
    connection: &SqliteConnection,
    author: &str,
    sequence: i32,
) -> Result<Option<i64>, Error> {
    authors_table
        .inner_join(messages_table.on(messages_author_id.nullable().eq(authors_id)))
        .select(messages_flume_seq)
        .filter(messages_seq.eq(sequence))
        .filter(authors_author.eq(author))
        .first(connection)
        .optional()
}
pub fn find_feed_latest_seq(
    connection: &SqliteConnection,
    author: &str,
) -> Result<Option<i32>, Error> {
    authors_table
        .inner_join(messages_table.on(messages_author_id.nullable().eq(authors_id)))
        .select(max(messages_seq))
        .filter(authors_author.eq(author.clone()))
        .first(connection)
}
pub fn find_feed_flume_seqs_newer_than(
    connection: &SqliteConnection,
    author: &str,
    sequence: i32,
    limit: Option<i64>,
) -> Result<Vec<FlumeSequence>, Error> {
    let flume_seqs = authors_table
        .inner_join(messages_table.on(messages_author_id.nullable().eq(authors_id)))
        .select(messages_flume_seq)
        .filter(messages_seq.gt(sequence))
        .filter(authors_author.eq(author.clone()))
        .limit(limit.unwrap_or(std::i64::MAX))
        .load(connection)?
        .iter()
        .map(|s: &i64| *s as FlumeSequence)
        .collect();

    Ok(flume_seqs)
}
