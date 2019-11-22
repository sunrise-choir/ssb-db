use crate::db;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("`include_keys` and `include_values` were both false. Pick one or both."))]
    IncludeKeysIncludeValuesBothFalse {},
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
