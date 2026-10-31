-- Discord archive tables, applied to the existing message keyspace
-- (MESSAGE_DB_KEYSPACE, default discord_message_cache) on both Cassandra and
-- ScyllaDB by `hainm schema`. Statements are separated by ';' at line end.
--
-- Columns follow the Discord API v10 Message object. Scalars a query is likely
-- to filter or group on get real columns; nested objects that Discord changes
-- often (embeds, components, polls, ...) are kept as JSON text. raw_json always
-- holds the complete message exactly as Discord returned it.

CREATE TYPE IF NOT EXISTS attachment (
    id bigint,
    filename text,
    title text,
    description text,
    content_type text,
    size bigint,
    url text,
    proxy_url text,
    width int,
    height int,
    duration_secs double,
    flags int
);

-- One partition per channel per 10-day window (bucket = snowflake time in ms
-- since the Unix epoch / 864000000), newest message first. Bounded partitions
-- keep a busy channel from becoming one huge partition.
CREATE TABLE IF NOT EXISTS messages (
    channel_id bigint,
    bucket int,
    message_id bigint,
    guild_id bigint,
    author_id bigint,
    author_username text,
    author_global_name text,
    author_bot boolean,
    webhook_id bigint,
    application_id bigint,
    content text,
    created_at timestamp,
    edited_at timestamp,
    type int,
    flags int,
    tts boolean,
    pinned boolean,
    mention_everyone boolean,
    mention_user_ids set<bigint>,
    mention_role_ids set<bigint>,
    reply_to_message_id bigint,
    reply_to_channel_id bigint,
    thread_id bigint,
    attachments list<frozen<attachment>>,
    embeds_json text,
    reactions_json text,
    components_json text,
    sticker_items_json text,
    poll_json text,
    raw_json text,
    deleted boolean,
    deleted_at timestamp,
    archived_at timestamp,
    PRIMARY KEY ((channel_id, bucket), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

-- Earlier versions of a message, saved just before an edit overwrites it.
-- version_at is when that version was written (its edited_at, else created_at).
CREATE TABLE IF NOT EXISTS message_revisions (
    channel_id bigint,
    message_id bigint,
    version_at timestamp,
    content text,
    edited_at timestamp,
    raw_json text,
    PRIMARY KEY ((channel_id, message_id), version_at)
) WITH CLUSTERING ORDER BY (version_at DESC);

CREATE TABLE IF NOT EXISTS users (
    user_id bigint PRIMARY KEY,
    username text,
    global_name text,
    bot boolean,
    avatar text,
    last_seen_at timestamp,
    raw_json text
);

CREATE TABLE IF NOT EXISTS guilds (
    guild_id bigint PRIMARY KEY,
    name text,
    raw_json text,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS channels (
    guild_id bigint,
    channel_id bigint,
    name text,
    type int,
    topic text,
    parent_id bigint,
    raw_json text,
    updated_at timestamp,
    PRIMARY KEY (guild_id, channel_id)
);

-- Collector progress per channel: the newest and oldest message ids stored,
-- and whether the backfill has reached the start of the channel.
CREATE TABLE IF NOT EXISTS archive_state (
    channel_id bigint PRIMARY KEY,
    newest_message_id bigint,
    oldest_message_id bigint,
    backfill_complete boolean,
    updated_at timestamp
);
