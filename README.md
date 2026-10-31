# How Am I Not Myself?

Collect messages from various services, evaluate and categorize them, build personas from them, and use them to train AI.

## Discord archive

`hainm collect` archives Discord channels into the message-cache databases on the
AI Workstation (Cassandra on `127.0.0.1:9042`, ScyllaDB on `127.0.0.1:9043`, both
started by `~/develai/scripts/start-message-dbs.sh`).

- On each connect it catches up on messages posted since the last run, then
  backfills older history until it reaches the start of the channel. Progress is
  saved per page, so an interrupted backfill resumes where it stopped.
- While running it stores new messages, edits (earlier text goes to
  `message_revisions`), deletes (flagged, never removed) and reaction changes.
- Every message keeps the complete Discord JSON in `raw_json`; common fields also
  get their own columns. Tables are defined in [schema/discord.cql](schema/discord.cql).

### Setup

1. In the [Developer Portal](https://discord.com/developers/applications), open
   **How Am I Not Myself** → **Bot** and turn on **Message Content Intent**.
2. Invite the bot to the server with View Channel and Read Message History:
   <https://discord.com/oauth2/authorize?client_id=1563754064406384712&scope=bot&permissions=66560&guild_id=612725793990246445&disable_guild_select=true>
   For a private channel, also add the bot (or one of its roles) to that channel.
3. `cp .env.example .env` and fill in the bot token and database password.
4. List the channels to archive in [config.toml](config.toml).

### Commands

```bash
uv run hainm schema                     # create/update tables (Cassandra)
uv run hainm --backend scylladb schema  # same for ScyllaDB
uv run hainm channels                   # servers/channels the bot can see, with IDs
uv run hainm collect --once             # sync history, then exit
uv run hainm collect                    # sync, then keep listening
uv run hainm status                     # messages stored and backfill progress
```

The collector writes to one backend at a time (`[database] backend` in
`config.toml`, or `--backend`).

Not yet handled: threads and forum posts under an archived channel, and
downloading attachment files (Discord's attachment URLs expire, so only the
metadata is durable today).
