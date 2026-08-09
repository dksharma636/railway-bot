"""
Gmap Agent Referral Bot
========================

NOTES FOR THE HUMAN OPERATOR (read before running)
----------------------------------------------------
1. This bot MUST be added as an ADMINISTRATOR in every force-join channel you
   configure via /admin -> Manage Channels. Without admin rights in a channel,
   get_chat_member() calls against it will fail and the gate will never pass.
   This is required even for public channels, and essential for private ones.
2. Fill in BOT_TOKEN and ADMIN_IDS below before running.
3. Install dependencies:  pip install -r requirements.txt
4. Run the bot:           python bot.py
5. The SQLite file (bot.db) is created automatically on first run in the same
   directory as this script.

Design notes
------------
- FSM text-input steps in the admin panel (Set Reward Content, Set Required
  Referrals, Manage Channels -> Add, Broadcast, Find User) can each be
  cancelled with the inline "Cancel" button shown on that screen. That button
  is the intended way to back out of a step; typing /start or /admin mid-step
  will otherwise be swallowed as literal input for that step, since the FSM
  handlers match on state, not on command text.
- get_chat_member() is treated as "joined" for any status other than LEFT or
  KICKED (so "restricted" still counts as joined -- restricted members are
  still members of the chat, just limited in what they can do there).
- A referrer's reward is only marked as sent (reward_sent = 1) after delivery
  actually succeeds. If delivery fails (e.g. the referrer has blocked the
  bot), reward_sent stays 0 so a future code path could retry; there is no
  scheduled retry job built in, since the spec didn't call for one.

Pre-join detection (anti-fake-referral)
----------------------------------------
- The moment a referred user hits /start, BEFORE they see the gate, the bot
  checks their membership status in every configured force-join channel
  right then. If they're already a member of ANY of them, that's stamped
  onto their row as pre_joined = 1, permanently, at registration time.
- maybe_credit_referral() refuses to credit any user with pre_joined = 1.
  The referrer's count never moves for that invite -- no matter how many
  times the referred user leaves and rejoins, taps "I've Joined", or
  triggers a join-request event afterward. The disqualification is decided
  once, at first contact, and nothing downstream can undo it.
- This closes the loop where someone already sitting in the force channel
  clicks a friend's link, passes the gate instantly because they were
  never actually gated, and hands out a free referral credit for zero
  actual growth.
- Admin Panel -> Stats shows a "Disqualified (pre-joined)" count so admins
  can see how many signups were caught by this filter.
- Admin Panel -> Find User shows "Pre-joined: Yes/No" on the user lookup
  screen for the same reason.

Device fingerprint gate (multi-account referral farming)
-----------------------------------------------------------
- HONEST LIMIT, READ THIS FIRST: Telegram's Bot API gives a bot zero access
  to physical hardware. There is no device ID this bot can read. Everything
  below is a browser/behavioral fingerprint, not a device ID -- it changes
  when someone clears app data, switches browsers, or uses a different
  phone. This raises the cost of farming; it does not make farming
  impossible. Set that expectation before promising a stakeholder "one
  device, one account."
- Layer 1 (free, decided at /start before the gate, same moment as
  pre_joined): Telegram Premium status (message.from_user.is_premium) and
  approximate account age inferred from the numeric user_id (Telegram IDs
  are roughly sequential over time, so a very high ID relative to
  FINGERPRINT_ID_FLOOR_DATE / FINGERPRINT_ID_FLOOR_VALUE below is treated
  as "likely brand-new"). Neither is proof of anything alone -- both are
  stamped onto the row as signals, not as a pass/fail verdict by
  themselves.
- Layer 2 (free, structural): referral velocity. If more than
  MAX_REFERRALS_PER_HOUR distinct referred users get credited to the same
  referrer inside a rolling 60-minute window, further credits for that
  referrer are held (not lost -- queued as pending_review = 1) until an
  admin clears them via Manage Referrals. This is the loudest signal
  available without asking the user to do anything.
- Layer 3 (opt-in, requires a browser fingerprint): a Telegram WebApp
  (Mini App) button appears on the gate screen after Layer 1+2 checks
  pass. Tapping it opens fingerprint.html inside Telegram's in-app
  browser, which reads navigator.userAgent, screen resolution, timezone
  offset, and a canvas-rendering hash, combines them into a single SHA-256
  string client-side, and posts that hash back to the bot via
  Telegram.WebApp.sendData(). If that exact hash has already been used by
  a DIFFERENT user_id who was credited as a referral in the last
  FINGERPRINT_REUSE_WINDOW_DAYS days, this signup is flagged
  fingerprint_flagged = 1 and is NOT auto-credited -- it goes to admin
  review instead of being silently blocked, because canvas fingerprints
  do occasionally collide innocently (same phone model, same browser
  version, locked-down corporate images) and a false positive should cost
  an admin a lookup, not cost a genuine user their referral.
- The WebApp step is presented as OPTIONAL on the gate screen ("Verify
  device to unlock instantly" vs. a slower manual-review path for anyone
  who skips it) rather than mandatory, because a hard requirement to run
  JS inside Telegram's browser will lose real users who are on low-end
  devices or have JS restricted, and a referral gate that blocks
  legitimate signups is worse than one that lets a slice of farming
  through.
- fingerprint.html must be hosted somewhere with a real HTTPS
  certificate -- Telegram WebApps refuse to load over plain HTTP.
  FINGERPRINT_WEBAPP_URL below must point at wherever you host it. It is
  a fully static file; no backend of its own required, since it posts
  straight back to this bot via Telegram.WebApp.sendData().
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    ChatJoinRequest,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    MessageOriginChannel,
    WebAppInfo,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
#
# Your hosting platform only uploads this .py file + requirements.txt, with no
# way to set environment variables -- so the token/admin IDs are set directly
# below. (If your platform later supports env vars, os.environ.get(...) still
# takes priority automatically, so nothing needs to change.)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

_admin_ids_raw = os.environ.get("ADMIN_IDS", "5888777479")
if _admin_ids_raw:
    try:
        ADMIN_IDS = [int(x.strip()) for x in _admin_ids_raw.split(",") if x.strip()]
    except ValueError:
        ADMIN_IDS = []
else:
    ADMIN_IDS = [5888777479]  # Replace with real Telegram user IDs (integers)

# FIXED: Railway filesystem is ephemeral — default to /data/bot.db so it
# lands on the mounted persistent Volume. Set DB_PATH env var in Railway
# dashboard to override. Mount a Volume at /data in Railway settings.
DB_PATH = os.environ.get("DB_PATH", "/data/bot.db")

# ---------------------------------------------------------------------------
# Device fingerprint gate config -- see module docstring "Device fingerprint
# gate" section for what each layer actually does and its honest limits.
# ---------------------------------------------------------------------------

# Layer 1: account-age heuristic. Telegram numeric user_id growth is roughly
# monotonic with account creation time -- these two points anchor a rough
# linear estimate. Update FINGERPRINT_ID_FLOOR_VALUE/DATE periodically (e.g.
# every few months) by looking up a freshly-created test account's user_id,
# since Telegram's growth rate isn't perfectly linear and drifts over time.
FINGERPRINT_ID_FLOOR_VALUE = int(os.environ.get("FINGERPRINT_ID_FLOOR_VALUE", "7000000000"))
FINGERPRINT_ID_FLOOR_DATE = os.environ.get("FINGERPRINT_ID_FLOOR_DATE", "2024-06-01")
FINGERPRINT_NEW_ACCOUNT_DAYS = int(os.environ.get("FINGERPRINT_NEW_ACCOUNT_DAYS", "3"))

# Layer 2: referral velocity cap. More than this many DISTINCT referred
# users credited to one referrer inside a rolling 60-minute window pauses
# further auto-credits for that referrer -- held as pending_review, not
# discarded, until an admin clears them via Manage Referrals.
MAX_REFERRALS_PER_HOUR = int(os.environ.get("MAX_REFERRALS_PER_HOUR", "5"))

# Layer 3: WebApp fingerprint reuse window. If the same fingerprint hash
# shows up on a second credited-referral signup within this many days, that
# second signup is held for admin review instead of auto-credited.
FINGERPRINT_REUSE_WINDOW_DAYS = int(os.environ.get("FINGERPRINT_REUSE_WINDOW_DAYS", "30"))

# Must be a real HTTPS URL hosting fingerprint.html -- Telegram WebApps
# refuse plain HTTP. Empty string disables the Layer 3 button entirely and
# the gate falls back to Layer 1+2 only, which still function standalone.
FINGERPRINT_WEBAPP_URL = os.environ.get("FINGERPRINT_WEBAPP_URL", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger("gmap_agent_bot")


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def friend_word(n: int) -> str:
    return "friend" if n == 1 else "friends"


# ---------------------------------------------------------------------------
# Database layer
# ---------------------------------------------------------------------------

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id            INTEGER PRIMARY KEY,
                username           TEXT,
                first_name         TEXT,
                referred_by        INTEGER,
                referral_count     INTEGER NOT NULL DEFAULT 0,
                joined_gate        INTEGER NOT NULL DEFAULT 0,
                referral_credited  INTEGER NOT NULL DEFAULT 0,
                reward_sent        INTEGER NOT NULL DEFAULT 0,
                pre_joined         INTEGER NOT NULL DEFAULT 0,
                is_premium         INTEGER NOT NULL DEFAULT 0,
                likely_new_account INTEGER NOT NULL DEFAULT 0,
                fingerprint_hash   TEXT,
                pending_review     INTEGER NOT NULL DEFAULT 0,
                created_at         TEXT NOT NULL
            )
            """
        )
        # Additive migrations for DBs created before these columns existed.
        # On a brand-new DB every column already exists from CREATE TABLE
        # above, so every ALTER below fails with "duplicate column name"
        # every time -- that failure is expected and is what each bare
        # except is for. On an existing DB missing a given column, that
        # column's ALTER succeeds exactly once.
        for migration_sql in (
            "ALTER TABLE users ADD COLUMN pre_joined INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE users ADD COLUMN is_premium INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE users ADD COLUMN likely_new_account INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE users ADD COLUMN fingerprint_hash TEXT",
            "ALTER TABLE users ADD COLUMN pending_review INTEGER NOT NULL DEFAULT 0",
        ):
            try:
                await db.execute(migration_sql)
            except Exception:
                pass
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS channels (
                channel_id  INTEGER PRIMARY KEY,
                title       TEXT,
                invite_link TEXT
            )
            """
        )
        # Tracks join requests as soon as Telegram tells us about them, so the
        # gate can treat a *pending* request the same as an accepted one --
        # we don't wait for an admin to click Approve in the channel.
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_join (
                user_id     INTEGER NOT NULL,
                channel_id  INTEGER NOT NULL,
                requested_at TEXT NOT NULL,
                PRIMARY KEY (user_id, channel_id)
            )
            """
        )

        defaults = {
            "required_referrals": "1",
            "reward_type": "text",
            "reward_text": (
                "Your Gmap Agent reward hasn't been configured yet -- "
                "set it via /admin -> Set Reward Content."
            ),
            "reward_file_id": "",
        }
        for key, value in defaults.items():
            await db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (key, value),
            )
        await db.commit()


async def get_user(user_id: int) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return await cursor.fetchone()


async def find_user_by_username(username: str) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM users WHERE LOWER(username) = LOWER(?)", (username,)
        )
        return await cursor.fetchone()


async def create_user(
    user_id: int,
    username: Optional[str],
    first_name: Optional[str],
    referred_by: Optional[int],
    pre_joined: bool = False,
    is_premium: bool = False,
    likely_new_account: bool = False,
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO users (user_id, username, first_name, referred_by, "
            "pre_joined, is_premium, likely_new_account, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                user_id,
                username,
                first_name,
                referred_by,
                1 if pre_joined else 0,
                1 if is_premium else 0,
                1 if likely_new_account else 0,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        await db.commit()


async def update_user_profile(user_id: int, username: Optional[str], first_name: Optional[str]) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET username = ?, first_name = ? WHERE user_id = ?",
            (username, first_name, user_id),
        )
        await db.commit()


async def mark_joined_gate(user_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET joined_gate = 1 WHERE user_id = ?", (user_id,))
        await db.commit()


async def mark_reward_sent(user_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET reward_sent = 1 WHERE user_id = ?", (user_id,))
        await db.commit()


async def credit_referral_and_mark(referred_user_id: int, referrer_id: int) -> None:
    """Idempotency for this pair is enforced by the caller checking
    referral_credited == 0 before calling this."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET referral_count = referral_count + 1 WHERE user_id = ?",
            (referrer_id,),
        )
        await db.execute(
            "UPDATE users SET referral_credited = 1 WHERE user_id = ?",
            (referred_user_id,),
        )
        await db.commit()


async def admin_reset_referral_count(user_id: int) -> None:
    """Zeroes a user's referral_count and reward_sent so a re-earned reward
    fires again on the next credited referral. Does NOT touch
    referral_credited on rows where this user is the *referred* party --
    this only resets what they've earned as a *referrer*."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET referral_count = 0, reward_sent = 0 WHERE user_id = ?",
            (user_id,),
        )
        await db.commit()


async def admin_remove_referrer_link(referred_user_id: int) -> Optional[int]:
    """Strips one bad referral relationship: clears referred_by and
    referral_credited on the referred user, and decrements the referrer's
    referral_count by 1 (floored at 0) if a credit had actually been given.
    Returns the referrer_id that was detached, or None if this user had no
    referrer on file."""
    user = await get_user(referred_user_id)
    if user is None or user["referred_by"] is None:
        return None

    referrer_id = user["referred_by"]
    was_credited = bool(user["referral_credited"])

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET referred_by = NULL, referral_credited = 0 WHERE user_id = ?",
            (referred_user_id,),
        )
        if was_credited:
            await db.execute(
                "UPDATE users SET referral_count = MAX(0, referral_count - 1) "
                "WHERE user_id = ?",
                (referrer_id,),
            )
        await db.commit()

    return referrer_id


async def record_join_request(user_id: int, channel_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO pending_join (user_id, channel_id, requested_at) "
            "VALUES (?, ?, ?)",
            (user_id, channel_id, datetime.now(timezone.utc).isoformat()),
        )
        await db.commit()


async def has_pending_join(user_id: int, channel_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM pending_join WHERE user_id = ? AND channel_id = ?",
            (user_id, channel_id),
        )
        return await cursor.fetchone() is not None


async def get_setting(key: str, default: str = "") -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row[0] if row and row[0] is not None else default


async def set_setting(key: str, value: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        await db.commit()


async def get_required_referrals() -> int:
    value = await get_setting("required_referrals", "1")
    try:
        return max(1, int(value))
    except ValueError:
        return 1


async def get_reward_content() -> tuple[str, str, str]:
    reward_type = await get_setting("reward_type", "text")
    reward_text = await get_setting("reward_text", "")
    reward_file_id = await get_setting("reward_file_id", "")
    return reward_type, reward_text, reward_file_id


async def set_reward_content(reward_type: str, reward_text: str, reward_file_id: str) -> None:
    await set_setting("reward_type", reward_type)
    await set_setting("reward_text", reward_text)
    await set_setting("reward_file_id", reward_file_id)


async def get_channels() -> list[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM channels")
        return list(await cursor.fetchall())


async def add_channel(channel_id: int, title: str, invite_link: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO channels (channel_id, title, invite_link) VALUES (?, ?, ?) "
            "ON CONFLICT(channel_id) DO UPDATE SET title = excluded.title, "
            "invite_link = excluded.invite_link",
            (channel_id, title, invite_link),
        )
        await db.commit()


async def remove_channel(channel_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))
        await db.commit()


async def get_all_user_ids() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users")
        rows = await cursor.fetchall()
        return [r[0] for r in rows]


async def get_leaderboard(limit: int = 10) -> list[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT user_id, username, first_name, referral_count FROM users "
            "WHERE referral_count > 0 ORDER BY referral_count DESC LIMIT ?",
            (limit,),
        )
        return list(await cursor.fetchall())


async def get_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cursor = await db.execute("SELECT COUNT(*) AS c FROM users")
        total_users = (await cursor.fetchone())["c"]

        cursor = await db.execute("SELECT COUNT(*) AS c FROM users WHERE joined_gate = 1")
        gate_verified = (await cursor.fetchone())["c"]

        cursor = await db.execute(
            "SELECT COUNT(*) AS c FROM users WHERE referral_credited = 1"
        )
        completed_referrals = (await cursor.fetchone())["c"]

        cursor = await db.execute(
            "SELECT COUNT(*) AS c FROM users WHERE pre_joined = 1"
        )
        pre_joined_blocked = (await cursor.fetchone())["c"]

        cursor = await db.execute(
            "SELECT user_id, username, first_name, referral_count FROM users "
            "ORDER BY referral_count DESC LIMIT 10"
        )
        top10 = list(await cursor.fetchall())

    return {
        "total_users": total_users,
        "gate_verified": gate_verified,
        "completed_referrals": completed_referrals,
        "pre_joined_blocked": pre_joined_blocked,
        "top10": top10,
    }


def display_name(row: aiosqlite.Row) -> str:
    if row["first_name"]:
        return row["first_name"]
    if row["username"]:
        return f"@{row['username']}"
    return f"User {row['user_id']}"


# ---------------------------------------------------------------------------
# Reward delivery
# ---------------------------------------------------------------------------

async def send_reward(bot: Bot, user_id: int, referral_count: int, required: int) -> None:
    reward_type, reward_text, reward_file_id = await get_reward_content()
    header = f"🎉 Referral complete — {referral_count}/{required}\n\n🗺️ Here's your Gmap Agent:"

    try:
        if reward_type == "photo" and reward_file_id:
            await bot.send_message(user_id, header)
            await bot.send_photo(user_id, reward_file_id, caption=reward_text or None)
        elif reward_type == "document" and reward_file_id:
            await bot.send_message(user_id, header)
            await bot.send_document(user_id, reward_file_id, caption=reward_text or None)
        else:
            await bot.send_message(user_id, f"{header}\n\n{reward_text}")
        await mark_reward_sent(user_id)
    except TelegramForbiddenError:
        logger.warning("Reward not delivered to %s: bot is blocked by user.", user_id)
    except Exception:
        logger.exception("Reward delivery failed for user %s", user_id)


async def check_pre_joined(bot: Bot, user_id: int) -> bool:
    """Called once, at /start, before the referred user has seen the gate.
    Returns True if the user is ALREADY a member of any configured
    force-join channel right now -- meaning any subsequent gate-pass proves
    nothing about whether the referral link actually brought them in.
    Errors (bot not admin in the channel, channel unreachable, etc.) are
    treated as "not a member" for that channel -- this function only ever
    flags a CONFIRMED existing membership, never a guess."""
    channels = await get_channels()
    for ch in channels:
        try:
            member = await bot.get_chat_member(chat_id=ch["channel_id"], user_id=user_id)
            if member.status not in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
                return True
        except (TelegramBadRequest, TelegramForbiddenError):
            continue
        except Exception:
            logger.exception(
                "check_pre_joined: get_chat_member failed for channel %s", ch["channel_id"]
            )
            continue
    return False


def estimate_likely_new_account(user_id: int) -> bool:
    """Layer 1 signal, not a verdict. Telegram numeric user_id growth is
    roughly monotonic with account creation date. This does a rough linear
    estimate from FINGERPRINT_ID_FLOOR_VALUE/DATE (one anchor point, today's
    date as the second anchor) and flags accounts estimated to be younger
    than FINGERPRINT_NEW_ACCOUNT_DAYS. This drifts as Telegram's actual
    signup rate changes over time -- re-anchor the floor constants
    periodically by checking a freshly-created test account's real user_id.
    A user_id below the floor value returns False immediately (older than
    the anchor point, not a new-account signal)."""
    if user_id < FINGERPRINT_ID_FLOOR_VALUE:
        return False

    try:
        floor_date = datetime.strptime(FINGERPRINT_ID_FLOOR_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        logger.error("FINGERPRINT_ID_FLOOR_DATE is malformed: %s", FINGERPRINT_ID_FLOOR_DATE)
        return False

    days_since_floor = (datetime.now(timezone.utc) - floor_date).days
    if days_since_floor <= 0:
        return False

    # IDs-per-day since the anchor point, applied backwards from user_id to
    # estimate how many days ago THIS id was likely issued.
    ids_per_day = FINGERPRINT_ID_FLOOR_VALUE / days_since_floor if FINGERPRINT_ID_FLOOR_VALUE else 0
    if ids_per_day <= 0:
        return False

    ids_since_floor = user_id - FINGERPRINT_ID_FLOOR_VALUE
    estimated_days_old = days_since_floor - (ids_since_floor / ids_per_day)

    return estimated_days_old <= FINGERPRINT_NEW_ACCOUNT_DAYS


async def check_referral_velocity(referrer_id: int) -> bool:
    """Layer 2. Returns True if referrer_id has already had
    MAX_REFERRALS_PER_HOUR or more DISTINCT users credited to them within
    the last rolling 60 minutes -- meaning the NEXT credit should be held
    for admin review instead of auto-applied. This only counts rows that
    were actually credited (referral_credited = 1), so it measures real
    throughput, not signups that never passed the gate."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM users WHERE referred_by = ? AND referral_credited = 1 "
            "AND created_at >= datetime('now', '-60 minutes')",
            (referrer_id,),
        )
        row = await cursor.fetchone()
        recent_count = row[0] if row else 0
    return recent_count >= MAX_REFERRALS_PER_HOUR


async def check_fingerprint_reuse(user_id: int, fingerprint_hash: str) -> Optional[int]:
    """Layer 3. Returns the OTHER user_id that this exact fingerprint hash
    was already seen on, if that other user was credited as a referral
    within FINGERPRINT_REUSE_WINDOW_DAYS -- or None if this hash is either
    new or its prior owner was never credited (so there's nothing to flag).
    Excludes user_id's own row, since a hash always matches itself. A
    collision here does NOT prove the two rows are the same physical
    device -- see module docstring -- it means the two rows share a
    behavioral fingerprint closely enough to warrant a human look."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT user_id FROM users WHERE fingerprint_hash = ? AND user_id != ? "
            "AND referral_credited = 1 "
            "AND created_at >= datetime('now', ?) "
            "ORDER BY created_at DESC LIMIT 1",
            (fingerprint_hash, user_id, f"-{FINGERPRINT_REUSE_WINDOW_DAYS} days"),
        )
        row = await cursor.fetchone()
    return row["user_id"] if row else None


async def store_fingerprint(user_id: int, fingerprint_hash: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET fingerprint_hash = ? WHERE user_id = ?",
            (fingerprint_hash, user_id),
        )
        await db.commit()


async def maybe_credit_referral(user_id: int, bot: Bot) -> None:
    user = await get_user(user_id)
    if user is None or user["referred_by"] is None or user["referral_credited"]:
        return

    if user["pre_joined"]:
        # This user was already inside the force channel before they ever
        # touched the referral link -- the gate they just passed proves
        # nothing about the link's effectiveness. No credit, permanently.
        # referral_credited is intentionally left at 0 rather than set to a
        # "credited but zero-value" state, so an admin reading the DB
        # directly sees an honest, uncredited row -- not a fake credit.
        logger.info(
            "Referral NOT credited for user %s -> referrer %s: pre_joined=1",
            user_id, user["referred_by"],
        )
        return

    referrer_id = user["referred_by"]

    if await check_referral_velocity(referrer_id):
        # Referrer already hit MAX_REFERRALS_PER_HOUR credited referrals in
        # the last rolling hour. HELD, not dropped -- referral_credited
        # stays 0 and pending_review goes to 1, so the count doesn't move
        # yet but nothing is lost. An admin clears it via Manage Referrals,
        # at which point it's credited normally like any other referral.
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET pending_review = 1 WHERE user_id = ?", (user_id,)
            )
            await db.commit()
        logger.info(
            "Referral HELD for review: user %s -> referrer %s (velocity cap hit)",
            user_id, referrer_id,
        )
        return

    await credit_referral_and_mark(user_id, referrer_id)

    # FIXED: re-fetch referrer AFTER credit_referral_and_mark() so
    # referral_count reflects the incremented value, not the stale snapshot.
    referrer = await get_user(referrer_id)
    if referrer is None:
        return

    required = await get_required_referrals()
    if referrer["referral_count"] >= required and not referrer["reward_sent"]:
        await send_reward(bot, referrer_id, referrer["referral_count"], required)


# ---------------------------------------------------------------------------
# Keyboards
# ---------------------------------------------------------------------------

def gate_keyboard(channels: list[aiosqlite.Row], show_fingerprint_button: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"📢 Join {ch['title']}", url=ch["invite_link"])]
        for ch in channels
    ]
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="gate_check")])
    if show_fingerprint_button and FINGERPRINT_WEBAPP_URL:
        rows.append([InlineKeyboardButton(
            text="⚡ Verify Device",
            web_app=WebAppInfo(url=FINGERPRINT_WEBAPP_URL),
        )])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def main_menu_keyboard(unlocked: bool) -> InlineKeyboardMarkup:
    link_label = "🔗 My Referral Link" if unlocked else "🔗 Refer & Unlock"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=link_label, callback_data="menu_link")],
            [InlineKeyboardButton(text="🏆 Leaderboard", callback_data="menu_leaderboard")],
        ]
    )


def back_keyboard(callback_data: str = "menu_back") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data=callback_data)]]
    )


def admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Stats", callback_data="adm_stats")],
            [InlineKeyboardButton(text="🎁 Set Reward Content", callback_data="adm_reward")],
            [InlineKeyboardButton(text="🔢 Set Required Referrals", callback_data="adm_required")],
            [InlineKeyboardButton(text="📢 Manage Channels", callback_data="adm_channels")],
            [InlineKeyboardButton(text="🔗 Manage Referrals", callback_data="adm_referrals")],
            [InlineKeyboardButton(text="📣 Broadcast", callback_data="adm_broadcast")],
            [InlineKeyboardButton(text="👤 Find User", callback_data="adm_finduser")],
        ]
    )


def cancel_keyboard(callback_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✖️ Cancel", callback_data=callback_data)]]
    )


def build_channels_list(channels: list[aiosqlite.Row]) -> tuple[str, InlineKeyboardMarkup]:
    rows = [
        [
            InlineKeyboardButton(
                text=f"❌ Remove {ch['title']}", callback_data=f"ch_remove:{ch['channel_id']}"
            )
        ]
        for ch in channels
    ]
    rows.append([InlineKeyboardButton(text="➕ Add Channel", callback_data="ch_add")])
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="adm_back")])

    body = "\n".join(f"• {ch['title']}" for ch in channels) if channels else "No channels configured yet."
    text = f"📢 Manage Channels\n\n{body}"
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


# ---------------------------------------------------------------------------
# Screen renderers (shared between fresh sends and in-place edits)
# ---------------------------------------------------------------------------

async def render_gate(
    bot: Bot, chat_id: int, user_id: Optional[int] = None, edit_message: Optional[Message] = None
) -> None:
    channels = await get_channels()

    # The fingerprint button is offered, not required, and only shown once
    # Layer 1 signals are on file for this user AND their fingerprint isn't
    # already stored -- re-showing it to someone who already verified would
    # just prompt them to verify twice for no reason.
    show_fingerprint = False
    if user_id is not None and FINGERPRINT_WEBAPP_URL:
        user = await get_user(user_id)
        if user is not None and not user["fingerprint_hash"]:
            show_fingerprint = True

    text = "🔒 One Quick Step\n\nJoin the channel(s) below, then tap ✅ I've Joined."
    if show_fingerprint:
        text += (
            "\n\nOr tap ⚡ Verify Device below -- most devices verify "
            "instantly. If your device fingerprint looks similar to a "
            "recent signup, it goes to a quick manual check instead."
        )
    kb = gate_keyboard(channels, show_fingerprint_button=show_fingerprint)
    if edit_message:
        await edit_message.edit_text(text, reply_markup=kb)
    else:
        await bot.send_message(chat_id, text, reply_markup=kb)


async def render_main_menu(
    bot: Bot, chat_id: int, user_id: int, edit_message: Optional[Message] = None
) -> None:
    user = await get_user(user_id)
    required = await get_required_referrals()
    referral_count = user["referral_count"] if user else 0
    unlocked = is_admin(user_id) or (user is not None and bool(user["reward_sent"]))

    if unlocked:
        text = (
            "✅ Gmap Agent Unlocked!\n\n"
            f"👥 Total referrals: {referral_count}\n"
            "Thanks for spreading the word 🙌"
        )
    else:
        text = (
            "┌───────────────────────┐\n"
            "      🗺️ GMAP AGENT\n"
            "└───────────────────────┘\n\n"
            f"🎁 Refer {required} {friend_word(required)} to unlock your free Gmap Agent — instantly.\n\n"
            f"👥 Progress: {referral_count}/{required}\n\n"
            "Tap below to get your invite link 👇"
        )

    kb = main_menu_keyboard(unlocked)
    if edit_message:
        await edit_message.edit_text(text, reply_markup=kb)
    else:
        await bot.send_message(chat_id, text, reply_markup=kb)


async def show_channels_list(message: Message) -> None:
    text, kb = build_channels_list(await get_channels())
    await message.edit_text(text, reply_markup=kb)


async def send_channels_list(message: Message) -> None:
    text, kb = build_channels_list(await get_channels())
    await message.answer(text, reply_markup=kb)


# ---------------------------------------------------------------------------
# FSM states (admin text-input steps)
# ---------------------------------------------------------------------------

class AdminStates(StatesGroup):
    waiting_reward_content = State()
    waiting_required_referrals = State()
    waiting_channel_forward = State()
    waiting_channel_link = State()
    waiting_broadcast = State()
    waiting_find_user = State()
    waiting_referral_lookup = State()


# ---------------------------------------------------------------------------
# User-facing router
# ---------------------------------------------------------------------------

user_router = Router(name="user")


@user_router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject, bot: Bot) -> None:
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name or ""

    user = await get_user(user_id)

    if user is None:
        referred_by = None
        payload = command.args

        if payload:
            candidate_id: Optional[int]
            try:
                candidate_id = int(payload)
            except ValueError:
                candidate_id = None

            if candidate_id is not None and candidate_id != user_id:
                candidate = await get_user(candidate_id)
                if candidate is not None:
                    referred_by = candidate_id
            # Any other case (self-referral, unknown referrer, malformed
            # payload) silently falls through to a normal registration.

        # Snapshot membership status right now, before this user has seen
        # the gate or had any chance to leave/rejoin -- this is the only
        # point where "already a member" and "joined because of the link"
        # can be told apart. Only worth checking for referred signups;
        # organic (non-referred) users have no referrer to protect.
        pre_joined = False
        is_premium = bool(message.from_user.is_premium)
        likely_new_account = False
        if referred_by is not None:
            pre_joined = await check_pre_joined(bot, user_id)
            likely_new_account = estimate_likely_new_account(user_id)

        await create_user(
            user_id, username, first_name, referred_by,
            pre_joined=pre_joined, is_premium=is_premium,
            likely_new_account=likely_new_account,
        )
        user = await get_user(user_id)
    else:
        await update_user_profile(user_id, username, first_name)

    if is_admin(user_id):
        await message.answer(
            "👑 Admin access — gate and referral requirements are bypassed for you.\n"
            "Use /admin to open the control panel."
        )
        await render_main_menu(bot, message.chat.id, user_id)
        return

    if not user["joined_gate"]:
        await render_gate(bot, message.chat.id, user_id=user_id)
    else:
        await render_main_menu(bot, message.chat.id, user_id)


@user_router.callback_query(F.data == "gate_check")
async def cb_gate_check(callback: CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id

    if is_admin(user_id):
        await callback.answer()
        await render_main_menu(bot, callback.message.chat.id, user_id, edit_message=callback.message)
        return

    channels = await get_channels()
    for ch in channels:
        # A pending (not-yet-approved) join request counts as joined -- the
        # gate doesn't wait on an admin clicking Approve in the channel.
        if await has_pending_join(user_id, ch["channel_id"]):
            continue
        try:
            member = await bot.get_chat_member(chat_id=ch["channel_id"], user_id=user_id)
            if member.status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
                await callback.answer("❌ You haven't joined all channels yet", show_alert=True)
                return
        except (TelegramBadRequest, TelegramForbiddenError):
            # Bot lacks admin rights in the channel, channel not found, user
            # never interacted with the channel, etc. -- treat as not-joined.
            await callback.answer("❌ You haven't joined all channels yet", show_alert=True)
            return
        except Exception:
            logger.exception("get_chat_member failed for channel %s", ch["channel_id"])
            await callback.answer("❌ You haven't joined all channels yet", show_alert=True)
            return

    await mark_joined_gate(user_id)
    await maybe_credit_referral(user_id, bot)
    await callback.answer()
    await render_main_menu(bot, callback.message.chat.id, user_id, edit_message=callback.message)


@user_router.message(F.web_app_data)
async def on_webapp_fingerprint(message: Message, bot: Bot) -> None:
    """Fires when fingerprint.html calls Telegram.WebApp.sendData(). Layer 3
    of the multi-account gate -- see module docstring. This still requires
    the channel gate to pass on its own merits; the fingerprint step never
    substitutes for actually joining the force channels, it only decides
    whether a credit that's already gate-eligible gets auto-applied or held
    for review."""
    user_id = message.from_user.id
    raw = message.web_app_data.data if message.web_app_data else None

    if not raw:
        await message.answer("❌ Verification data was empty. Please try again from the gate screen.")
        return

    try:
        payload = json.loads(raw)
        fingerprint_hash = payload.get("hash", "")
    except (json.JSONDecodeError, AttributeError):
        logger.warning("on_webapp_fingerprint: malformed payload from user %s", user_id)
        await message.answer("❌ Verification data was malformed. Please try again from the gate screen.")
        return

    if not fingerprint_hash or not isinstance(fingerprint_hash, str) or len(fingerprint_hash) > 128:
        await message.answer("❌ Verification data was invalid. Please try again from the gate screen.")
        return

    await store_fingerprint(user_id, fingerprint_hash)

    if is_admin(user_id):
        await render_main_menu(bot, message.chat.id, user_id)
        return

    # The fingerprint step doesn't bypass the actual channel gate -- confirm
    # membership the same way cb_gate_check does before crediting anything.
    channels = await get_channels()
    for ch in channels:
        if await has_pending_join(user_id, ch["channel_id"]):
            continue
        try:
            member = await bot.get_chat_member(chat_id=ch["channel_id"], user_id=user_id)
            if member.status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
                await message.answer(
                    "🔒 Verified, but you haven't joined all channels yet. "
                    "Join them, then tap ✅ I've Joined on the gate screen."
                )
                return
        except (TelegramBadRequest, TelegramForbiddenError):
            await message.answer(
                "🔒 Verified, but you haven't joined all channels yet. "
                "Join them, then tap ✅ I've Joined on the gate screen."
            )
            return
        except Exception:
            logger.exception("on_webapp_fingerprint: get_chat_member failed for channel %s", ch["channel_id"])
            await message.answer(
                "🔒 Verified, but you haven't joined all channels yet. "
                "Join them, then tap ✅ I've Joined on the gate screen."
            )
            return

    await mark_joined_gate(user_id)

    reused_by = await check_fingerprint_reuse(user_id, fingerprint_hash)
    if reused_by is not None:
        # Collision with a recently-credited referral. Held for admin
        # review, not blocked -- see module docstring on false positives.
        # referral_credited stays 0; the admin clears it via Manage
        # Referrals the same way a velocity-cap hold gets cleared.
        user = await get_user(user_id)
        if user is not None and user["referred_by"] is not None and not user["referral_credited"]:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE users SET pending_review = 1 WHERE user_id = ?", (user_id,)
                )
                await db.commit()
            logger.info(
                "Fingerprint collision: user %s matches recently-credited user %s -- held for review",
                user_id, reused_by,
            )
        await message.answer(
            "✅ Device verified.\n\n"
            "Your referral is in manual review -- an admin will confirm it "
            "shortly. This happens when a device fingerprint looks similar "
            "to a recent signup and doesn't mean anything is wrong."
        )
        await render_main_menu(bot, message.chat.id, user_id)
        return

    await maybe_credit_referral(user_id, bot)
    await message.answer("✅ Device verified.")
    await render_main_menu(bot, message.chat.id, user_id)


@user_router.chat_join_request()
async def on_chat_join_request(update: ChatJoinRequest, bot: Bot) -> None:
    """Fires the instant a user taps 'Request to Join' on a private channel --
    we do NOT wait for an admin to hit Approve. This is what lets a
    request-based (private) channel behave the same as a public one for the
    referral gate."""
    user_id = update.from_user.id
    channel_id = update.chat.id

    await record_join_request(user_id, channel_id)

    user = await get_user(user_id)
    if user is None:
        # User hit "Request to Join" on the channel directly without ever
        # starting the bot -- nothing to credit yet. They'll be picked up
        # once they /start the bot and the referral row exists.
        return

    if is_admin(user_id):
        return

    channels = await get_channels()
    channel_ids = {ch["channel_id"] for ch in channels}
    if channel_id not in channel_ids:
        return

    # If this was the last gated channel the user hadn't requested/joined
    # yet, they've now cleared the whole gate -- credit immediately.
    for ch in channels:
        joined = await has_pending_join(user_id, ch["channel_id"])
        if not joined:
            try:
                member = await bot.get_chat_member(chat_id=ch["channel_id"], user_id=user_id)
                joined = member.status not in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED)
            except (TelegramBadRequest, TelegramForbiddenError):
                joined = False
            except Exception:
                logger.exception("get_chat_member failed for channel %s", ch["channel_id"])
                joined = False
        if not joined:
            return

    await mark_joined_gate(user_id)
    await maybe_credit_referral(user_id, bot)


@user_router.callback_query(F.data == "menu_link")
async def cb_referral_link(callback: CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start={user_id}"
    caption = "Join me and unlock the free Gmap Agent 🗺️"
    share_url = f"https://t.me/share/url?url={quote(link, safe='')}&text={quote(caption, safe='')}"

    text = (
        "Your personal invite link 👇\n\n"
        f"<code>{link}</code>\n\n"
        "Share it — the moment your friend joins and verifies, you unlock the Gmap Agent automatically 🎉"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📤 Share with a Friend", url=share_url)],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="menu_back")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()


@user_router.callback_query(F.data == "menu_leaderboard")
async def cb_leaderboard(callback: CallbackQuery) -> None:
    top = await get_leaderboard(10)
    if not top:
        body = "No referrals yet — be the first! 🚀"
    else:
        body = "\n".join(
            f"{i}. {display_name(row)} — {row['referral_count']}"
            for i, row in enumerate(top, start=1)
        )
    text = f"🏆 Leaderboard\n\n{body}"
    await callback.message.edit_text(text, reply_markup=back_keyboard("menu_back"))
    await callback.answer()


@user_router.callback_query(F.data == "menu_back")
async def cb_menu_back(callback: CallbackQuery, bot: Bot) -> None:
    await render_main_menu(
        bot, callback.message.chat.id, callback.from_user.id, edit_message=callback.message
    )
    await callback.answer()


# ---------------------------------------------------------------------------
# Admin router
# ---------------------------------------------------------------------------

admin_router = Router(name="admin")
admin_router.message.filter(F.from_user.id.in_(ADMIN_IDS))
admin_router.callback_query.filter(F.from_user.id.in_(ADMIN_IDS))


@admin_router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("⚙️ Admin Panel", reply_markup=admin_panel_keyboard())


@admin_router.callback_query(F.data == "adm_back")
async def cb_admin_back(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text("⚙️ Admin Panel", reply_markup=admin_panel_keyboard())
    await callback.answer()


# --- Stats -----------------------------------------------------------------

@admin_router.callback_query(F.data == "adm_stats")
async def cb_admin_stats(callback: CallbackQuery) -> None:
    stats = await get_stats()
    lines = [
        "📊 Bot Stats",
        "",
        f"👥 Total users: {stats['total_users']}",
        f"✅ Gate verified: {stats['gate_verified']}",
        f"🎉 Completed referrals: {stats['completed_referrals']}",
        f"🚫 Disqualified (pre-joined): {stats['pre_joined_blocked']}",
        "",
        "🏆 Top Referrers",
    ]
    if stats["top10"]:
        lines.extend(
            f"{i}. {display_name(row)} — {row['referral_count']}"
            for i, row in enumerate(stats["top10"], start=1)
        )
    else:
        lines.append("No referrals yet.")

    await callback.message.edit_text("\n".join(lines), reply_markup=back_keyboard("adm_back"))
    await callback.answer()


# --- Set reward content ------------------------------------------------------

@admin_router.callback_query(F.data == "adm_reward")
async def cb_admin_set_reward(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(AdminStates.waiting_reward_content)
    await callback.message.edit_text(
        "🎁 Send the new reward content now.\n\n"
        "Text, photo, or document (with an optional caption) — this will be "
        "used for every future auto-delivery.",
        reply_markup=cancel_keyboard("adm_back"),
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_reward_content)
async def process_reward_content(message: Message, state: FSMContext) -> None:
    if message.photo:
        await set_reward_content("photo", message.caption or "", message.photo[-1].file_id)
    elif message.document:
        await set_reward_content("document", message.caption or "", message.document.file_id)
    elif message.text:
        await set_reward_content("text", message.text, "")
    else:
        await message.answer("Please send text, a photo, or a document.")
        return

    await state.clear()
    await message.answer("✅ Reward content updated.", reply_markup=admin_panel_keyboard())


# --- Set required referrals -------------------------------------------------

@admin_router.callback_query(F.data == "adm_required")
async def cb_admin_set_required(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(AdminStates.waiting_required_referrals)
    await callback.message.edit_text(
        "🔢 Send the new required referral count (a whole number, e.g. 3).",
        reply_markup=cancel_keyboard("adm_back"),
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_required_referrals)
async def process_required_referrals(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text.isdigit() or int(text) <= 0:
        await message.answer(
            "❌ That's not a valid number. Send a whole number greater than 0 "
            "(e.g. 3), or tap Cancel above to back out.",
            reply_markup=cancel_keyboard("adm_back"),
        )
        return

    new_value = int(text)
    if new_value > 1000:
        await message.answer(
            f"❌ {new_value} referrals required is almost certainly a typo -- "
            "send a smaller number, or tap Cancel above if you meant to back out.",
            reply_markup=cancel_keyboard("adm_back"),
        )
        return

    old_value = await get_required_referrals()
    await set_setting("required_referrals", text)
    await state.clear()
    await message.answer(
        f"✅ Required referrals changed: {old_value} → {new_value}.",
        reply_markup=admin_panel_keyboard(),
    )


# --- Manage channels ---------------------------------------------------------

@admin_router.callback_query(F.data == "adm_channels")
async def cb_admin_channels(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await show_channels_list(callback.message)
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ch_remove:"))
async def cb_channel_remove(callback: CallbackQuery) -> None:
    channel_id = int(callback.data.split(":", 1)[1])
    await remove_channel(channel_id)
    await show_channels_list(callback.message)
    await callback.answer("Channel removed.")


@admin_router.callback_query(F.data == "ch_add")
async def cb_channel_add(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(AdminStates.waiting_channel_forward)
    await callback.message.edit_text(
        "➕ Forward any message from the channel you want to add.\n\n"
        "The bot must already be an admin in that channel — this works for "
        "private channels too, and correctly captures the numeric chat ID.",
        reply_markup=cancel_keyboard("adm_channels"),
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_channel_forward)
async def process_channel_forward(message: Message, state: FSMContext, bot: Bot) -> None:
    origin = message.forward_origin
    if not isinstance(origin, MessageOriginChannel):
        await message.answer(
            "❌ That doesn't look like a message forwarded from a channel. "
            "Please forward a post directly from the channel, or tap Cancel above.",
            reply_markup=cancel_keyboard("adm_channels"),
        )
        return

    # The whole gate depends on this bot being an admin in the channel --
    # per the module docstring, get_chat_member() throws otherwise and the
    # gate silently treats every user as not-joined, forever. Checking it
    # NOW, before the channel is ever staged, means the failure surfaces
    # here instead of days later as a support ticket about a broken gate.
    try:
        bot_member = await bot.get_chat_member(chat_id=origin.chat.id, user_id=(await bot.get_me()).id)
        is_bot_admin = bot_member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)
    except (TelegramBadRequest, TelegramForbiddenError):
        is_bot_admin = False
    except Exception:
        logger.exception("process_channel_forward: get_chat_member self-check failed for %s", origin.chat.id)
        is_bot_admin = False

    if not is_bot_admin:
        await message.answer(
            f"❌ This bot isn't an admin in <b>{origin.chat.title}</b> yet.\n\n"
            "Add it as an administrator in that channel first, then forward "
            "the message again -- otherwise the gate check will fail for "
            "every user, silently.",
            reply_markup=cancel_keyboard("adm_channels"),
        )
        return

    await state.update_data(pending_channel_id=origin.chat.id, pending_channel_title=origin.chat.title)
    await state.set_state(AdminStates.waiting_channel_link)
    await message.answer(
        f"✅ Verified — <b>{origin.chat.title}</b>, bot is admin.\n\n"
        "Now send the invite link for this channel.",
        reply_markup=cancel_keyboard("adm_channels"),
    )


@admin_router.message(AdminStates.waiting_channel_link)
async def process_channel_link(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    channel_id = data.get("pending_channel_id")
    title = data.get("pending_channel_title")
    invite_link = (message.text or "").strip()

    if channel_id is None:
        await message.answer("Something went wrong — send /admin and start over.")
        return

    if not invite_link.startswith(("https://t.me/", "http://t.me/")):
        await message.answer(
            "❌ That doesn't look like a Telegram invite link -- it should "
            "start with https://t.me/. Send the link again, or tap Cancel above.",
            reply_markup=cancel_keyboard("adm_channels"),
        )
        return

    await add_channel(channel_id, title, invite_link)
    await state.clear()
    await message.answer(f"✅ Channel added: {title}")
    await send_channels_list(message)


# --- Broadcast ---------------------------------------------------------------

@admin_router.callback_query(F.data == "adm_broadcast")
async def cb_admin_broadcast(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(AdminStates.waiting_broadcast)
    await callback.message.edit_text(
        "📣 Send the message to broadcast — text, photo, or any content. "
        "It will be copied to every user in the database.",
        reply_markup=cancel_keyboard("adm_back"),
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_broadcast)
async def process_broadcast(message: Message, state: FSMContext, bot: Bot) -> None:
    if message.text and message.text.startswith("/"):
        await message.answer(
            "That looks like a command, not broadcast content — send the message you "
            "want to broadcast, or tap Cancel above to back out."
        )
        return

    await state.clear()
    user_ids = await get_all_user_ids()
    total = len(user_ids)
    progress_msg = await message.answer(f"📣 Broadcasting… 0/{total} processed")

    sent = 0
    blocked = 0
    failed = 0

    for i, user_id in enumerate(user_ids, start=1):
        try:
            await bot.copy_message(
                chat_id=user_id, from_chat_id=message.chat.id, message_id=message.message_id
            )
            sent += 1
        except TelegramForbiddenError:
            blocked += 1
        except Exception:
            logger.exception("Broadcast failed for user %s", user_id)
            failed += 1

        if i % 20 == 0 or i == total:
            try:
                await progress_msg.edit_text(
                    f"📣 Broadcasting… {i}/{total} processed\n"
                    f"✅ Sent: {sent}  🚫 Blocked: {blocked}  ⚠️ Failed: {failed}"
                )
            except TelegramBadRequest:
                pass

        await asyncio.sleep(0.07)  # FIXED: 0.05 (~20/s) risks 429 in bulk; 0.07 (~14/s) is safe

    await progress_msg.edit_text(
        f"✅ Broadcast complete\n\nSent: {sent}   Blocked: {blocked}   Failed: {failed}",
        reply_markup=admin_panel_keyboard(),
    )


# --- Find user -----------------------------------------------------------------

@admin_router.callback_query(F.data == "adm_finduser")
async def cb_admin_find_user(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(AdminStates.waiting_find_user)
    await callback.message.edit_text(
        "👤 Send a user ID or @username to look up.",
        reply_markup=cancel_keyboard("adm_back"),
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_find_user)
async def process_find_user(message: Message, state: FSMContext) -> None:
    await state.clear()
    query = (message.text or "").strip()

    user: Optional[aiosqlite.Row]
    if query.startswith("@"):
        user = await find_user_by_username(query[1:])
    else:
        try:
            user = await get_user(int(query))
        except ValueError:
            user = None

    if user is None:
        await message.answer(
            "❌ No user found with that ID or username.",
            reply_markup=back_keyboard("adm_back"),
        )
        return

    required = await get_required_referrals()
    username_line = f"@{user['username']}" if user["username"] else "—"
    text = (
        "👤 User Lookup\n\n"
        f"ID: {user['user_id']}\n"
        f"Username: {username_line}\n"
        f"Name: {user['first_name'] or '—'}\n"
        f"Referrals: {user['referral_count']}/{required}\n"
        f"Gate passed: {'Yes' if user['joined_gate'] else 'No'}\n"
        f"Reward sent: {'Yes' if user['reward_sent'] else 'No'}\n"
        f"Pre-joined (referral blocked): {'Yes' if user['pre_joined'] else 'No'}\n"
        f"Joined: {user['created_at']}"
    )
    await message.answer(text, reply_markup=back_keyboard("adm_back"))


# --- Manage referrals --------------------------------------------------------

def referral_action_keyboard(user_id: int, has_referrer: bool, is_pending_review: bool) -> InlineKeyboardMarkup:
    rows = []
    if is_pending_review:
        rows.append([InlineKeyboardButton(
            text="✅ Approve Held Referral",
            callback_data=f"ref_approve:{user_id}",
        )])
    rows.append([InlineKeyboardButton(
        text="♻️ Reset Referral Count",
        callback_data=f"ref_reset:{user_id}",
    )])
    if has_referrer:
        rows.append([InlineKeyboardButton(
            text="✂️ Remove Referrer Link",
            callback_data=f"ref_unlink:{user_id}",
        )])
    rows.append([InlineKeyboardButton(text="👤 Look Up Another User", callback_data="adm_referrals")])
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="adm_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def render_referral_lookup_result(message: Message, user: aiosqlite.Row) -> None:
    required = await get_required_referrals()
    username_line = f"@{user['username']}" if user["username"] else "—"
    referrer_line = str(user["referred_by"]) if user["referred_by"] is not None else "None (organic signup)"

    # Signal summary -- explains WHY a row might be pending, not just THAT
    # it is. An admin clearing a hold should see the same evidence the
    # system used to hold it, not just a bare "pending_review: 1" flag.
    signal_lines = []
    if user["pre_joined"]:
        signal_lines.append("🚫 Pre-joined force channel before using this link")
    if user["is_premium"]:
        signal_lines.append("⭐ Telegram Premium")
    if user["likely_new_account"]:
        signal_lines.append("🆕 Account estimated < %s days old" % FINGERPRINT_NEW_ACCOUNT_DAYS)
    if user["fingerprint_hash"]:
        signal_lines.append("📱 Device fingerprint on file")
    else:
        signal_lines.append("📱 No device fingerprint submitted")
    signals_block = "\n".join(signal_lines) if signal_lines else "None on file"

    status_line = "🟡 PENDING REVIEW" if user["pending_review"] else "🟢 Normal"

    text = (
        "🔗 Manage Referrals\n\n"
        f"ID: {user['user_id']}\n"
        f"Username: {username_line}\n"
        f"Name: {user['first_name'] or '—'}\n"
        f"Status: {status_line}\n"
        f"Referral count (as referrer): {user['referral_count']}/{required}\n"
        f"Referred by: {referrer_line}\n"
        f"Credited: {'Yes' if user['referral_credited'] else 'No'}\n\n"
        f"Signals on file:\n{signals_block}\n\n"
        "Pick an action:"
    )
    kb = referral_action_keyboard(
        user["user_id"],
        has_referrer=user["referred_by"] is not None,
        is_pending_review=bool(user["pending_review"]),
    )
    await message.answer(text, reply_markup=kb)


@admin_router.callback_query(F.data == "adm_referrals")
async def cb_admin_referrals(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(AdminStates.waiting_referral_lookup)
    await callback.message.edit_text(
        "🔗 Manage Referrals\n\n"
        "Send a user ID or @username to reset their referral count, or "
        "remove a bad referrer link.",
        reply_markup=cancel_keyboard("adm_back"),
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_referral_lookup)
async def process_referral_lookup(message: Message, state: FSMContext) -> None:
    await state.clear()
    query = (message.text or "").strip()

    user: Optional[aiosqlite.Row]
    if query.startswith("@"):
        user = await find_user_by_username(query[1:])
    else:
        try:
            user = await get_user(int(query))
        except ValueError:
            user = None

    if user is None:
        await message.answer(
            "❌ No user found with that ID or username. Send /admin to start over.",
            reply_markup=back_keyboard("adm_back"),
        )
        return

    await render_referral_lookup_result(message, user)


@admin_router.callback_query(F.data.startswith("ref_reset:"))
async def cb_referral_reset(callback: CallbackQuery) -> None:
    user_id = int(callback.data.split(":", 1)[1])
    user = await get_user(user_id)
    if user is None:
        await callback.answer("User no longer exists.", show_alert=True)
        return

    await admin_reset_referral_count(user_id)
    logger.info("Admin %s reset referral_count for user %s", callback.from_user.id, user_id)

    refreshed = await get_user(user_id)
    await callback.message.edit_text(
        f"✅ Referral count reset to 0 for {display_name(refreshed)}."
    )
    await render_referral_lookup_result(callback.message, refreshed)
    await callback.answer("Referral count reset.")


@admin_router.callback_query(F.data.startswith("ref_unlink:"))
async def cb_referral_unlink(callback: CallbackQuery) -> None:
    user_id = int(callback.data.split(":", 1)[1])
    detached_referrer_id = await admin_remove_referrer_link(user_id)

    if detached_referrer_id is None:
        await callback.answer("This user has no referrer link to remove.", show_alert=True)
        return

    logger.info(
        "Admin %s removed referrer link: user %s was referred by %s",
        callback.from_user.id, user_id, detached_referrer_id,
    )

    refreshed = await get_user(user_id)
    await callback.message.edit_text(
        f"✅ Referrer link removed. {display_name(refreshed)} is no longer "
        f"credited to referrer {detached_referrer_id}."
    )
    await render_referral_lookup_result(callback.message, refreshed)
    await callback.answer("Referrer link removed.")


@admin_router.callback_query(F.data.startswith("ref_approve:"))
async def cb_referral_approve(callback: CallbackQuery, bot: Bot) -> None:
    """Clears a Layer 2 (velocity) or Layer 3 (fingerprint collision) hold
    and runs the credit for real -- this is the human-in-the-loop step
    both docstring sections promised: a hold pauses the count, it doesn't
    discard it, and this button is what actually moves it forward."""
    user_id = int(callback.data.split(":", 1)[1])
    user = await get_user(user_id)
    if user is None:
        await callback.answer("User no longer exists.", show_alert=True)
        return

    if not user["pending_review"]:
        await callback.answer("This user isn't pending review.", show_alert=True)
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET pending_review = 0 WHERE user_id = ?", (user_id,)
        )
        await db.commit()

    logger.info("Admin %s approved held referral for user %s", callback.from_user.id, user_id)

    # maybe_credit_referral re-checks pre_joined and velocity on its own --
    # approving a hold doesn't bypass those, it only clears the review flag
    # that was blocking this specific credit attempt from running.
    await maybe_credit_referral(user_id, bot)

    refreshed = await get_user(user_id)
    credited_line = "credited" if refreshed["referral_credited"] else "still not credited (see signals below)"
    await callback.message.edit_text(f"✅ Review cleared for {display_name(refreshed)} -- {credited_line}.")
    await render_referral_lookup_result(callback.message, refreshed)
    await callback.answer("Review cleared.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def main() -> None:
    if not BOT_TOKEN:
        logger.error(
            "BOT_TOKEN is not set. Export it as an environment variable before "
            "starting the bot, e.g.: export BOT_TOKEN=\"123456:ABC-your-token\""
        )
        sys.exit(1)

    if not ADMIN_IDS:
        logger.error(
            "ADMIN_IDS is not set (or contains no valid integers). Export it as "
            "a comma-separated list, e.g.: export ADMIN_IDS=\"5888777479,987654321\""
        )
        sys.exit(1)

    # FIXED: ensure the directory for DB_PATH exists (Railway Volume mounts
    # the root /data but doesn't guarantee subdirectory creation).
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    await init_db()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(admin_router)
    dp.include_router(user_router)

    # allowed_updates must explicitly list "chat_join_request", or Telegram
    # will never deliver ChatJoinRequest updates to on_chat_join_request()
    # below -- without this the "don't wait for admin approval" behavior is
    # silently dead even though the handler itself is correct.
    allowed_updates = dp.resolve_used_update_types()
    if "chat_join_request" not in allowed_updates:
        allowed_updates = list(allowed_updates) + ["chat_join_request"]

    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Gmap Agent Referral Bot starting polling...")
    await dp.start_polling(bot, allowed_updates=allowed_updates)


if __name__ == "__main__":
    asyncio.run(main())
