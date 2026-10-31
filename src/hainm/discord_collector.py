"""Archive Discord channel messages into Cassandra/ScyllaDB.

On every (re)connect, each configured channel is synced over REST: first any
messages newer than the last one stored, then older history back to the start
of the channel. While connected, raw gateway events keep the archive current:
creates, edits, deletes and reaction changes. Everything is an idempotent
upsert keyed by message id, so overlap between sync and live events is safe.
"""

import asyncio
import json
import logging

import discord

from hainm.config import Channel, Config
from hainm.store import Store

log = logging.getLogger("hainm.discord")

PAGE_SIZE = 100
MESSAGE_EVENTS = {"MESSAGE_CREATE", "MESSAGE_UPDATE", "MESSAGE_DELETE", "MESSAGE_DELETE_BULK"}
REACTION_EVENTS = {
    "MESSAGE_REACTION_ADD", "MESSAGE_REACTION_REMOVE",
    "MESSAGE_REACTION_REMOVE_ALL", "MESSAGE_REACTION_REMOVE_EMOJI",
}


class Collector(discord.Client):
    def __init__(self, config: Config, store: Store, once: bool = False):
        intents = discord.Intents.none()
        intents.guilds = True
        intents.guild_messages = True
        intents.guild_reactions = True
        intents.message_content = True
        super().__init__(intents=intents, enable_debug_events=True)
        self.store = store
        self.once = once
        self.channels = {c.channel_id: c for c in config.channels}
        self.state: dict[int, dict] = {}
        self.sync_lock = asyncio.Lock()

    async def db(self, fn, *args):
        return await asyncio.to_thread(fn, *args)

    # Sync over REST ---------------------------------------------------------

    async def on_ready(self):
        log.info("Connected as %s; archiving %d channel(s).", self.user, len(self.channels))
        async with self.sync_lock:
            for channel in self.channels.values():
                try:
                    await self.sync_channel(channel)
                except discord.HTTPException as e:
                    log.error("Channel %s: %s (is the bot in the server with View Channel and Read Message History?)",
                              channel.channel_id, e)
        if self.once:
            await self.close()

    async def sync_channel(self, channel: Channel):
        cid = channel.channel_id
        await self.db(self.store.upsert_guild, await self.http.get_guild(channel.guild_id, with_counts=False))
        info = await self.http.get_channel(cid)
        await self.db(self.store.upsert_channel, info)
        log.info("Syncing #%s (%s).", info.get("name"), cid)

        row = await self.db(self.store.get_state, cid)
        self.state[cid] = {
            "newest": row.newest_message_id if row else None,
            "oldest": row.oldest_message_id if row else None,
            "complete": bool(row and row.backfill_complete),
        }
        state = self.state[cid]

        # Forward: everything posted since the newest stored message.
        if state["newest"]:
            after, fetched = state["newest"], 0
            while page := await self.http.logs_from(cid, PAGE_SIZE, after=after):
                page.sort(key=lambda m: int(m["id"]))
                await self.store_page(channel, page)
                after = int(page[-1]["id"])
                fetched += len(page)
            log.info("#%s: caught up %d new message(s).", info.get("name"), fetched)

        # Backward: history older than the oldest stored message.
        if not state["complete"]:
            before, fetched = state["oldest"], 0
            while page := await self.http.logs_from(cid, PAGE_SIZE, before=before):
                await self.store_page(channel, page)
                before = min(int(m["id"]) for m in page)
                fetched += len(page)
                if fetched % 1000 < PAGE_SIZE:
                    log.info("#%s: backfilled %d message(s) so far.", info.get("name"), fetched)
            state["complete"] = True
            await self.save_state(cid)
            log.info("#%s: backfill complete (%d message(s) this run).", info.get("name"), fetched)

    async def store_page(self, channel: Channel, page: list[dict]):
        for msg in page:
            await self.db(self.store.upsert_message, msg, channel.guild_id)
        self.track(channel.channel_id, [int(m["id"]) for m in page])
        await self.save_state(channel.channel_id)

    def track(self, cid: int, ids: list[int]):
        state = self.state.setdefault(cid, {"newest": None, "oldest": None, "complete": False})
        state["newest"] = max([state["newest"] or 0, *ids])
        state["oldest"] = min(i for i in [state["oldest"], *ids] if i)

    async def save_state(self, cid: int):
        s = self.state[cid]
        await self.db(self.store.set_state, cid, s["newest"], s["oldest"], s["complete"])

    # Live gateway events ----------------------------------------------------

    async def on_socket_raw_receive(self, raw: str):
        event = json.loads(raw)
        kind = event.get("t")
        if event.get("op") != 0 or kind not in MESSAGE_EVENTS | REACTION_EVENTS:
            return
        data = event["d"]
        channel = self.channels.get(int(data.get("channel_id", 0)))
        if not channel:
            return
        try:
            await self.handle_event(channel, kind, data)
        except Exception:
            log.exception("Failed to store %s in channel %s.", kind, channel.channel_id)

    async def handle_event(self, channel: Channel, kind: str, data: dict):
        cid = channel.channel_id
        if kind == "MESSAGE_CREATE":
            await self.db(self.store.upsert_message, data, channel.guild_id)
            if cid in self.state:
                self.track(cid, [int(data["id"])])
                await self.save_state(cid)
        elif kind == "MESSAGE_DELETE":
            await self.db(self.store.mark_deleted, cid, int(data["id"]))
        elif kind == "MESSAGE_DELETE_BULK":
            for mid in data["ids"]:
                await self.db(self.store.mark_deleted, cid, int(mid))
        else:
            # Edits may arrive as partial objects and reaction events carry no
            # message body, so store a fresh full copy from REST.
            mid = data["message_id"] if kind in REACTION_EVENTS else data["id"]
            if kind == "MESSAGE_UPDATE" and "author" in data and "content" in data:
                msg = data
            else:
                try:
                    msg = await self.http.get_message(cid, mid)
                except discord.NotFound:
                    return
            await self.db(self.store.upsert_message, msg, channel.guild_id)


async def list_channels(config: Config) -> None:
    """Print every server and text channel the bot can see, for config.toml."""
    client = discord.Client(intents=discord.Intents.none())
    await client.login(config.discord_token)
    try:
        guilds = await client.http.get_guilds(200)
        if not guilds:
            print("The bot is not in any server yet. Invite it first (see README).")
        for guild in guilds:
            print(f"{guild['name']}  guild_id = {guild['id']}")
            for ch in sorted(await client.http.get_all_guild_channels(guild["id"]), key=lambda c: c.get("position", 0)):
                if ch["type"] in (0, 5, 15):  # text, announcement, forum
                    print(f"    #{ch['name']}  channel_id = {ch['id']}")
    finally:
        await client.close()


def run(config: Config, store: Store, once: bool) -> None:
    if not config.channels:
        raise SystemExit("No channels in config.toml under [[discord.channels]].")
    try:
        Collector(config, store, once=once).run(config.discord_token, log_handler=None)
    except discord.PrivilegedIntentsRequired:
        raise SystemExit(
            "Discord refused the Message Content intent. Turn on 'Message Content Intent' under Bot > "
            "Privileged Gateway Intents at https://discord.com/developers/applications for this bot, then retry."
        ) from None
