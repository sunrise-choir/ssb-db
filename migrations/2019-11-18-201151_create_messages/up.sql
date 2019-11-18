CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY,
  flume_seq BIGINT UNIQUE NOT NULL,
  seq INTEGER NOT NULL,
  key_id INTEGER UNIQUE NOT NULL,
  author_id INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_author_id_index ON messages(author_id);
CREATE INDEX IF NOT EXISTS messages_author_id_seq_index ON messages(author_id, seq);
