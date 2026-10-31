"""Cassandra/ScyllaDB storage for archived Discord messages (see schema/discord.cql)."""

import json
from collections import namedtuple
from datetime import UTC, datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, ShardAwareOptions
from cassandra.policies import RoundRobinPolicy

from hainm.config import ROOT, Config

DISCORD_EPOCH_MS = 1420070400000
BUCKET_MS = 10 * 24 * 60 * 60 * 1000

Attachment = namedtuple(
    "Attachment",
    "id filename title description content_type size url proxy_url width height duration_secs flags",
)


def snowflake_ms(snowflake: int) -> int:
    return (snowflake >> 22) + DISCORD_EPOCH_MS


def bucket_of(snowflake: int) -> int:
    return snowflake_ms(snowflake) // BUCKET_MS


def snowflake_time(snowflake: int) -> datetime:
    return datetime.fromtimestamp(snowflake_ms(snowflake) / 1000, UTC)


def parse_time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def as_int(value) -> int | None:
    return int(value) if value is not None else None


def as_json(value) -> str | None:
    return json.dumps(value, separators=(",", ":")) if value else None


def now() -> datetime:
    return datetime.now(UTC)


class Store:
    def __init__(self, config: Config):
        # Single local node: protocol v4 works on both Cassandra 5 and ScyllaDB,
        # and ScyllaDB's shard-aware port (19042) is not published by Docker.
        self.cluster = Cluster(
            [config.db_host],
            port=config.db_port,
            auth_provider=PlainTextAuthProvider(config.db_username, config.db_password),
            protocol_version=4,
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=RoundRobinPolicy())},
            shard_aware_options=ShardAwareOptions(disable=True),
        )
        self.keyspace = config.keyspace
        self.session = self.cluster.connect(config.keyspace)
        self._prepared = {}

    def close(self) -> None:
        self.cluster.shutdown()

    def apply_schema(self) -> list[str]:
        text = (ROOT / "schema" / "discord.cql").read_text()
        lines = [line for line in text.splitlines() if not line.lstrip().startswith("--")]
        statements = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
        for statement in statements:
            self.session.execute(statement)
        return [" ".join(s.split()[:5]) for s in statements]

    def _run(self, cql: str, params: tuple):
        if cql not in self._prepared:
            self._prepared[cql] = self.session.prepare(cql)
        return self.session.execute(self._prepared[cql], params)

    # Messages ---------------------------------------------------------------

    def upsert_message(self, msg: dict, guild_id: int) -> None:
        """Insert or overwrite a message from its raw Discord JSON.

        If a stored copy has different content, it is saved to message_revisions
        first so edits never lose the earlier text.
        """
        channel_id, message_id = int(msg["channel_id"]), int(msg["id"])
        previous = self.get_message(channel_id, message_id)
        if previous and (previous.content != msg.get("content") or previous.edited_at != parse_time(msg.get("edited_timestamp"))):
            self._run(
                "INSERT INTO message_revisions (channel_id, message_id, version_at, content, edited_at, raw_json) VALUES (?, ?, ?, ?, ?, ?)",
                (channel_id, message_id, previous.edited_at or previous.created_at, previous.content, previous.edited_at, previous.raw_json),
            )

        author = msg.get("author") or {}
        reference = msg.get("message_reference") or {}
        attachments = [
            Attachment(
                int(a["id"]), a.get("filename"), a.get("title"), a.get("description"), a.get("content_type"),
                a.get("size"), a.get("url"), a.get("proxy_url"), a.get("width"), a.get("height"),
                a.get("duration_secs"), a.get("flags"),
            )
            for a in msg.get("attachments") or []
        ]
        self._run(
            """INSERT INTO messages (
                channel_id, bucket, message_id, guild_id,
                author_id, author_username, author_global_name, author_bot, webhook_id, application_id,
                content, created_at, edited_at, type, flags, tts, pinned, mention_everyone,
                mention_user_ids, mention_role_ids, reply_to_message_id, reply_to_channel_id, thread_id,
                attachments, embeds_json, reactions_json, components_json, sticker_items_json, poll_json,
                raw_json, archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                channel_id, bucket_of(message_id), message_id, guild_id,
                as_int(author.get("id")), author.get("username"), author.get("global_name"), author.get("bot", False),
                as_int(msg.get("webhook_id")), as_int(msg.get("application_id")),
                msg.get("content"), parse_time(msg.get("timestamp")), parse_time(msg.get("edited_timestamp")),
                msg.get("type"), msg.get("flags"), msg.get("tts"), msg.get("pinned"), msg.get("mention_everyone"),
                {int(u["id"]) for u in msg.get("mentions") or []}, {int(r) for r in msg.get("mention_roles") or []},
                as_int(reference.get("message_id")), as_int(reference.get("channel_id")),
                as_int((msg.get("thread") or {}).get("id")),
                attachments, as_json(msg.get("embeds")), as_json(msg.get("reactions")),
                as_json(msg.get("components")), as_json(msg.get("sticker_items")), as_json(msg.get("poll")),
                json.dumps(msg, separators=(",", ":")), now(),
            ),
        )
        if author.get("id") and not msg.get("webhook_id"):
            self.upsert_user(author)

    def get_message(self, channel_id: int, message_id: int):
        return self._run(
            "SELECT content, created_at, edited_at, raw_json FROM messages WHERE channel_id = ? AND bucket = ? AND message_id = ?",
            (channel_id, bucket_of(message_id), message_id),
        ).one()

    def mark_deleted(self, channel_id: int, message_id: int) -> None:
        # Only flag rows we have; an UPDATE alone would create a stub row.
        if self.get_message(channel_id, message_id):
            self._run(
                "UPDATE messages SET deleted = true, deleted_at = ? WHERE channel_id = ? AND bucket = ? AND message_id = ?",
                (now(), channel_id, bucket_of(message_id), message_id),
            )

    def count_messages(self, channel_id: int) -> int:
        first, last = bucket_of(channel_id), int(now().timestamp() * 1000) // BUCKET_MS
        return sum(
            self._run("SELECT COUNT(*) FROM messages WHERE channel_id = ? AND bucket = ?", (channel_id, b)).one()[0]
            for b in range(first, last + 1)
        )

    # Users, guilds, channels ------------------------------------------------

    def upsert_user(self, user: dict) -> None:
        self._run(
            "INSERT INTO users (user_id, username, global_name, bot, avatar, last_seen_at, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (int(user["id"]), user.get("username"), user.get("global_name"), user.get("bot", False),
             user.get("avatar"), now(), json.dumps(user, separators=(",", ":"))),
        )

    def upsert_guild(self, guild: dict) -> None:
        self._run(
            "INSERT INTO guilds (guild_id, name, raw_json, updated_at) VALUES (?, ?, ?, ?)",
            (int(guild["id"]), guild.get("name"), json.dumps(guild, separators=(",", ":")), now()),
        )

    def upsert_channel(self, channel: dict) -> None:
        self._run(
            "INSERT INTO channels (guild_id, channel_id, name, type, topic, parent_id, raw_json, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (int(channel["guild_id"]), int(channel["id"]), channel.get("name"), channel.get("type"),
             channel.get("topic"), as_int(channel.get("parent_id")), json.dumps(channel, separators=(",", ":")), now()),
        )

    # Collector progress -----------------------------------------------------

    def get_state(self, channel_id: int):
        return self._run(
            "SELECT newest_message_id, oldest_message_id, backfill_complete FROM archive_state WHERE channel_id = ?",
            (channel_id,),
        ).one()

    def set_state(self, channel_id: int, newest: int | None, oldest: int | None, complete: bool) -> None:
        self._run(
            "INSERT INTO archive_state (channel_id, newest_message_id, oldest_message_id, backfill_complete, updated_at) VALUES (?, ?, ?, ?, ?)",
            (channel_id, newest, oldest, complete, now()),
        )
