table! {
    authors (id) {
        id -> Nullable<Integer>,
        author -> Text,
    }
}

table! {
    keys (id) {
        id -> Nullable<Integer>,
        key -> Text,
    }
}

table! {
    messages (id) {
        id -> Nullable<Integer>,
        flume_seq -> BigInt,
        seq -> Integer,
        key_id -> Integer,
        author_id -> Integer,
    }
}

allow_tables_to_appear_in_same_query!(
    authors,
    keys,
    messages,
);
