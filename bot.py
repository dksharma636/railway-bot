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
"""

import asyncio
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
                created_at         TEXT NOT NULL
            )
            """
        )
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
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO users (user_id, username, first_name, referred_by, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                user_id,
                username,
                first_name,
                referred_by,
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
            "SELECT user_id, username, first_name, referral_count FROM users "
            "ORDER BY referral_count DESC LIMIT 10"
        )
        top10 = list(await cursor.fetchall())

    return {
        "total_users": total_users,
        "gate_verified": gate_verified,
        "completed_referrals": completed_referrals,
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


async def maybe_credit_referral(user_id: int, bot: Bot) -> None:
    user = await get_user(user_id)
    if user is None or user["referred_by"] is None or user["referral_credited"]:
        return

    referrer_id = user["referred_by"]
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

def gate_keyboard(channels: list[aiosqlite.Row]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"📢 Join {ch['title']}", url=ch["invite_link"])]
        for ch in channels
    ]
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="gate_check")])
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

async def render_gate(bot: Bot, chat_id: int, edit_message: Optional[Message] = None) -> None:
    channels = await get_channels()
    text = "🔒 One Quick Step\n\nJoin the channel(s) below, then tap ✅ I've Joined."
    kb = gate_keyboard(channels)
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

        await create_user(user_id, username, first_name, referred_by)
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
        await render_gate(bot, message.chat.id)
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
        await message.answer("That's not a valid number. Send a whole number greater than 0.")
        return

    await set_setting("required_referrals", text)
    await state.clear()
    await message.answer(f"✅ Required referrals set to {text}.", reply_markup=admin_panel_keyboard())


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
async def process_channel_forward(message: Message, state: FSMContext) -> None:
    origin = message.forward_origin
    if not isinstance(origin, MessageOriginChannel):
        await message.answer(
            "That doesn't look like a message forwarded from a channel. "
            "Please forward a post directly from the channel."
        )
        return

    await state.update_data(pending_channel_id=origin.chat.id, pending_channel_title=origin.chat.title)
    await state.set_state(AdminStates.waiting_channel_link)
    await message.answer(
        f"Got it — <b>{origin.chat.title}</b>.\n\nNow send the invite link for this channel."
    )


@admin_router.message(AdminStates.waiting_channel_link)
async def process_channel_link(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    channel_id = data.get("pending_channel_id")
    title = data.get("pending_channel_title")
    invite_link = (message.text or "").strip()

    if channel_id is None or not invite_link:
        await message.answer("Something went wrong — send the invite link again, or /admin to start over.")
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
        f"Joined: {user['created_at']}"
    )
    await message.answer(text, reply_markup=back_keyboard("adm_back"))


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
