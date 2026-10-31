"""How Am I Not Myself? command line."""

import argparse
import asyncio
import logging

from hainm import config as config_mod


def main() -> None:
    parser = argparse.ArgumentParser(prog="hainm")
    parser.add_argument("--backend", choices=sorted(config_mod.DB_PORTS),
                        help="database to use (default: config.toml [database] backend)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("schema", help="create or update the archive tables")
    sub.add_parser("channels", help="list the servers and channels the bot can see")
    sub.add_parser("status", help="show archive progress per configured channel")
    collect = sub.add_parser("collect", help="archive configured channels and keep listening")
    collect.add_argument("--once", action="store_true", help="sync history, then exit instead of listening")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("cassandra").setLevel(logging.WARNING)
    # Cassandra 5 warns on every prepare when the session has a keyspace set;
    # all of our tables live in that one keyspace, so the ambiguity can't arise.
    logging.getLogger("cassandra.protocol").setLevel(logging.ERROR)
    config = config_mod.load(args.backend)

    if args.command == "channels":
        from hainm.discord_collector import list_channels
        asyncio.run(list_channels(config))
        return

    from hainm.store import Store
    store = Store(config)
    try:
        if args.command == "schema":
            for statement in store.apply_schema():
                print(f"{config.backend}: {statement}")
        elif args.command == "status":
            for channel in config.channels:
                state = store.get_state(channel.channel_id)
                count = store.count_messages(channel.channel_id)
                done = "complete" if state and state.backfill_complete else "in progress" if state else "not started"
                print(f"{config.backend}: {channel.label or channel.channel_id}: {count} message(s), backfill {done}")
        elif args.command == "collect":
            from hainm.discord_collector import run
            run(config, store, once=args.once)
    finally:
        store.close()
