use diesel::prelude::*;
pub use diesel::result::Error;
use flumedb::flume_view::Sequence as FlumeSequence;

pub mod models;
pub mod schema;

pub use models::{authors, keys, messages};

pub use authors::find_or_create_author;
pub use keys::find_or_create_key;
pub use messages::{
    find_feed_flume_seqs_newer_than, find_feed_latest_seq,
    find_message_flume_seq_by_author_and_sequence, find_message_flume_seq_by_key, get_latest,
    insert_message,
};

use crate::ssb_message::SsbMessage;

pub fn append_item(
    connection: &SqliteConnection,
    seq: FlumeSequence,
    item: &[u8],
) -> Result<(), Error> {
    let result = serde_json::from_slice::<SsbMessage>(item);

    // If there are deleted records with all bytes zerod then we should just skip this message.
    if let Err(_) = result {
        return Ok(());
    }

    let message = result.unwrap();

    let message_key_id = find_or_create_key(&connection, &message.key)?;
    let author_id = find_or_create_author(&connection, &message.value.author)?;

    insert_message(
        connection,
        message.value.sequence as i32,
        seq as i64,
        message_key_id,
        author_id,
    )?;

    Ok(())
}
