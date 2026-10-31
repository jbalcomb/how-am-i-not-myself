"""Settings: non-secret IDs from config.toml, secrets from .env / the environment."""

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

DB_PORTS = {"cassandra": 9042, "scylladb": 9043}


@dataclass(frozen=True)
class Channel:
    guild_id: int
    channel_id: int
    label: str = ""


@dataclass(frozen=True)
class Config:
    discord_token: str
    backend: str
    db_host: str
    db_port: int
    db_username: str
    db_password: str
    keyspace: str
    channels: tuple[Channel, ...]


def load_dotenv(path: Path) -> None:
    """Load KEY=value lines into os.environ without overriding what is already set."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def load(backend: str | None = None, config_path: Path | None = None) -> Config:
    load_dotenv(ROOT / ".env")
    raw = tomllib.loads((config_path or ROOT / "config.toml").read_text())

    backend = backend or os.environ.get("MESSAGE_DB_BACKEND") or raw.get("database", {}).get("backend", "cassandra")
    if backend not in DB_PORTS:
        raise SystemExit(f"Unknown database backend {backend!r}; use one of {sorted(DB_PORTS)}.")
    prefix = backend.upper()

    channels = tuple(
        Channel(int(c["guild_id"]), int(c["channel_id"]), c.get("label", ""))
        for c in raw.get("discord", {}).get("channels", [])
    )
    return Config(
        discord_token=os.environ.get("DISCORD_TOKEN", ""),
        backend=backend,
        db_host=os.environ.get(f"{prefix}_HOST", "127.0.0.1"),
        db_port=int(os.environ.get(f"{prefix}_PORT", DB_PORTS[backend])),
        db_username=os.environ.get("MESSAGE_DB_USERNAME", "discord_bot"),
        db_password=os.environ.get("MESSAGE_DB_PASSWORD", ""),
        keyspace=os.environ.get("MESSAGE_DB_KEYSPACE", "discord_message_cache"),
        channels=channels,
    )
