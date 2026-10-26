
import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

import asyncpg
from aiohttp import web
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO"),
)
log = logging.getLogger("lottery-bot")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("TELEGRAM_ADMIN_ID", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
CURRENCY = os.getenv("CURRENCY", "ETB").strip().upper()
DEFAULT_LOTTERY_ID = os.getenv("LOTTERY_ID", "main").strip() or "main"

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is required")
if not ADMIN_ID_RAW.isdigit():
    raise RuntimeError("TELEGRAM_ADMIN_ID must be your numeric Telegram user ID")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

ADMIN_ID = int(ADMIN_ID_RAW)
POOL: Optional[asyncpg.Pool] = None

MAIN_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🎟 Buy Ticket"), KeyboardButton("🎫 My Tickets")],
        [KeyboardButton("💳 Payment History"), KeyboardButton("ℹ️ Payment Info")],
        [KeyboardButton("❓ Help")],
    ],
    resize_keyboard=True,
)

ADMIN_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📊 Admin Dashboard"), KeyboardButton("⚙️ Settings")],
        [KeyboardButton("👥 Users"), KeyboardButton("🚫 Block Number")],
        [KeyboardButton("✅ Unblock Number"), KeyboardButton("🧾 Pending Payments")],
        [KeyboardButton("⬅️ Main Menu")],
    ],
    resize_keyboard=True,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT NOT NULL DEFAULT '',
    last_name TEXT NOT NULL DEFAULT '',
    is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tickets (
    id BIGSERIAL PRIMARY KEY,
    lottery_id TEXT NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    number INTEGER NOT NULL CHECK (number BETWEEN 0 AND 9999),
    ticket_type TEXT NOT NULL CHECK (ticket_type IN ('full', 'half')),
    share_no SMALLINT,
    amount NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    status TEXT NOT NULL DEFAULT 'reserved'
        CHECK (status IN ('reserved', 'paid', 'cancelled', 'expired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK ((ticket_type = 'full' AND share_no IS NULL) OR
           (ticket_type = 'half' AND share_no IN (1,2)))
);

-- A full ticket occupies the number as a whole; a half ticket occupies one of
-- two shares. A full ticket uses share_no=0 in the allocation table.
CREATE TABLE IF NOT EXISTS allocations (
    id BIGSERIAL PRIMARY KEY,
    lottery_id TEXT NOT NULL,
    number INTEGER NOT NULL CHECK (number BETWEEN 0 AND 9999),
    share_no SMALLINT NOT NULL CHECK (share_no IN (0,1,2)),
    ticket_id BIGINT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    UNIQUE (lottery_id, number, share_no)
);

CREATE TABLE IF NOT EXISTS blocked_numbers (
    id BIGSERIAL PRIMARY KEY,
    lottery_id TEXT NOT NULL,
    number INTEGER NOT NULL CHECK (number BETWEEN 0 AND 9999),
    blocked_by BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(lottery_id, number)
);

CREATE TABLE IF NOT EXISTS payments (
    id BIGSERIAL PRIMARY KEY,
    ticket_id BIGINT NOT NULL REFERENCES tickets(id),
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    amount NUMERIC(12,2) NOT NULL,
    receipt_file_id TEXT NOT NULL,
    receipt_caption TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'rejected')),
    reviewed_by BIGINT,
    review_note TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_tickets_user_created
    ON tickets(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_payments_user_created
    ON payments(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_payments_pending
    ON payments(status, created_at);
CREATE INDEX IF NOT EXISTS idx_tickets_expiry
    ON tickets(status, expires_at);
"""

DEFAULT_SETTINGS = {
    "lottery_id": DEFAULT_LOTTERY_ID,
    "lottery_name": "Main Lottery",
    "full_price": "100.00",
    "half_price": "50.00",
    "reservation_minutes": "30",
    "payment_instructions": "Send payment using the payment method announced by the administrator, then upload a clear receipt here.",
    "number_min": "0",
    "number_max": "9999",
    "sales_open": "true",
}


def money(value) -> str:
    return f"{Decimal(str(value)):.2f} {CURRENCY}"


async def db():
    if POOL is None:
        raise RuntimeError("Database pool is not initialized")
    return POOL


async def init_db():
    global POOL
    # Render provides an internal PostgreSQL URL; asyncpg expects postgres:// or postgresql://.
    dsn = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    POOL = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=5,
        command_timeout=30,
        ssl="require" if "localhost" not in dsn and "127.0.0.1" not in dsn else None,
    )
    async with POOL.acquire() as conn:
        await conn.execute(SCHEMA)
        for key, value in DEFAULT_SETTINGS.items():
            await conn.execute(
                "INSERT INTO settings(key, value) VALUES($1, $2) ON CONFLICT(key) DO NOTHING",
                key, value,
            )
        await conn.execute(
            """INSERT INTO users(user_id, first_name, is_blocked)
               VALUES($1, 'Administrator', FALSE)
               ON CONFLICT(user_id) DO NOTHING""",
            ADMIN_ID,
        )
    log.info("PostgreSQL initialized")


async def get_setting(key: str, default: str = "") -> str:
    pool = await db()
    async with pool.acquire() as conn:
        value = await conn.fetchval("SELECT value FROM settings WHERE key=$1", key)
    return value if value is not None else default


async def set_setting(key: str, value: str):
    pool = await db()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO settings(key,value) VALUES($1,$2)
               ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value""",
            key, value,
        )


async def register_user(update: Update) -> bool:
    user = update.effective_user
    if not user:
        return False
    pool = await db()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO users(user_id, username, first_name, last_name, last_seen_at)
               VALUES($1,$2,$3,$4,NOW())
               ON CONFLICT(user_id) DO UPDATE SET
                 username=EXCLUDED.username,
                 first_name=EXCLUDED.first_name,
                 last_name=EXCLUDED.last_name,
                 last_seen_at=NOW()""",
            user.id, user.username, user.first_name or "", user.last_name or "",
        )
        blocked = await conn.fetchval("SELECT is_blocked FROM users WHERE user_id=$1", user.id)
    return not bool(blocked)


async def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID)


async def user_is_blocked(user_id: int) -> bool:
    pool = await db()
    async with pool.acquire() as conn:
        result = await conn.fetchval("SELECT is_blocked FROM users WHERE user_id=$1", user_id)
    return bool(result)


async def guard_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    allowed = await register_user(update)
    if not allowed:
        if update.effective_message:
            await update.effective_message.reply_text("Your account is currently blocked. Contact the administrator.")
        return False
    return True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard_user(update, context):
        return
    await update.effective_message.reply_text(
        "Welcome to the lottery bot.\n\n"
        "Choose Buy Ticket to reserve an available number. Upload a payment receipt after reserving; "
        "your ticket is valid only after an administrator approves the payment.\n\n"
        "Use /help for instructions.",
        reply_markup=ADMIN_MENU if await is_admin(update) else MAIN_MENU,
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard_user(update, context):
        return
    await update.effective_message.reply_text(
        "How to use this bot:\n"
        "1. Tap 🎟 Buy Ticket.\n"
        "2. Choose Full or Half ticket.\n"
        "3. Enter a number in the configured range.\n"
        "4. Upload a photo or image document of your payment receipt.\n"
        "5. Wait for administrator review.\n\n"
        "Your number is reserved temporarily. An uploaded receipt does not mean payment is approved. "
        "Check 🎫 My Tickets and 💳 Payment History for status.",
        reply_markup=MAIN_MENU,
    )


async def payment_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard_user(update, context):
        return
    instructions = await get_setting("payment_instructions", DEFAULT_SETTINGS["payment_instructions"])
    await update.effective_message.reply_text(
        f"Payment instructions:\n{instructions}\n\n"
        "Upload your receipt after reserving a ticket. Do not send your receipt to an unofficial account."
    )


async def buy_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard_user(update, context):
        return
    if (await get_setting("sales_open", "true")).lower() != "true":
        await update.effective_message.reply_text("Ticket sales are currently closed.")
        return
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Full ticket", callback_data="buy:full"),
         InlineKeyboardButton("Half ticket", callback_data="buy:half")]
    ])
    await update.effective_message.reply_text("Choose the ticket type:", reply_markup=keyboard)


async def buy_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await register_user(update):
        await query.edit_message_text("Your account is blocked. Contact the administrator.")
        return
    ticket_type = query.data.split(":")[1]
    if ticket_type not in ("full", "half"):
        return
    context.user_data["ticket_type"] = ticket_type
    await query.edit_message_text(
        f"You selected a {ticket_type} ticket.\n"
        "Send the number you want, using the configured number range. "
        "For a half ticket, the bot will allocate one of the two available halves."
    )
    context.user_data["awaiting_number"] = True


async def create_reservation(update: Update, context: ContextTypes.DEFAULT_TYPE, number: int):
    user_id = update.effective_user.id
    ticket_type = context.user_data.get("ticket_type")
    if ticket_type not in ("full", "half"):
        await update.effective_message.reply_text("Please tap 🎟 Buy Ticket first.")
        return
    if (await get_setting("sales_open", "true")).lower() != "true":
        await update.effective_message.reply_text("Ticket sales are currently closed.")
        return

    try:
        low = int(await get_setting("number_min", "0"))
        high = int(await get_setting("number_max", "9999"))
        reservation_minutes = max(1, min(1440, int(await get_setting("reservation_minutes", "30"))))
        price_key = "full_price" if ticket_type == "full" else "half_price"
        amount = Decimal(await get_setting(price_key, "100.00" if ticket_type == "full" else "50.00"))
        lottery_id = await get_setting("lottery_id", DEFAULT_LOTTERY_ID)
    except (ValueError, InvalidOperation):
        await update.effective_message.reply_text("The lottery settings are invalid. Please contact the administrator.")
        return

    if number < low or number > high:
        await update.effective_message.reply_text(f"Choose a number from {low} to {high}.")
        return
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=reservation_minutes)
    pool = await db()
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Serialize allocation decisions for this number, preventing concurrent double-booking.
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext($1), $2)",
                    lottery_id, number,
                )
                # Release expired reservations for this number before checking availability.
                # A pending receipt keeps its reservation locked for admin review.
                await conn.execute(
                    """UPDATE tickets t SET status='expired'
                       WHERE t.lottery_id=$1 AND t.number=$2 AND t.status='reserved'
                         AND t.expires_at<=NOW()
                         AND NOT EXISTS (SELECT 1 FROM payments p
                                         WHERE p.ticket_id=t.id AND p.status='pending')""",
                    lottery_id, number,
                )
                await conn.execute(
                    """DELETE FROM allocations a USING tickets t
                       WHERE a.ticket_id=t.id AND t.lottery_id=$1 AND t.number=$2
                         AND t.status IN ('expired','cancelled')""",
                    lottery_id, number,
                )
                blocked = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM blocked_numbers WHERE lottery_id=$1 AND number=$2)",
                    lottery_id, number,
                )
                if blocked:
                    raise ValueError("That number is blocked by the administrator.")
                occupied = await conn.fetch(
                    """SELECT a.share_no, t.status, t.expires_at
                       FROM allocations a JOIN tickets t ON t.id=a.ticket_id
                       WHERE a.lottery_id=$1 AND a.number=$2
                         AND t.status IN ('reserved','paid')
                         AND (t.status='paid' OR t.expires_at > NOW())""",
                    lottery_id, number,
                )
                occupied_shares = {int(r["share_no"]) for r in occupied}
                if ticket_type == "full":
                    if occupied_shares:
                        raise ValueError("That number is already reserved or sold. Please choose another.")
                    share_no = None
                    ticket_id = await conn.fetchval(
                        """INSERT INTO tickets(lottery_id,user_id,number,ticket_type,share_no,amount,status,expires_at)
                           VALUES($1,$2,$3,'full',NULL,$4,'reserved',$5) RETURNING id""",
                        lottery_id, user_id, number, amount, expires_at,
                    )
                    await conn.execute(
                        """INSERT INTO allocations(lottery_id,number,share_no,ticket_id)
                           VALUES($1,$2,0,$3)""",
                        lottery_id, number, ticket_id,
                    )
                else:
                    if 0 in occupied_shares:
                        raise ValueError("That number is already reserved or sold as a full ticket.")
                    available = next((s for s in (1, 2) if s not in occupied_shares), None)
                    if available is None:
                        raise ValueError("Both halves of that number are already reserved or sold.")
                    share_no = available
                    ticket_id = await conn.fetchval(
                        """INSERT INTO tickets(lottery_id,user_id,number,ticket_type,share_no,amount,status,expires_at)
                           VALUES($1,$2,$3,'half',$4,$5,'reserved',$6) RETURNING id""",
                        lottery_id, user_id, number, share_no, amount, expires_at,
                    )
                    await conn.execute(
                        """INSERT INTO allocations(lottery_id,number,share_no,ticket_id)
                           VALUES($1,$2,$3,$4)""",
                        lottery_id, number, share_no, ticket_id,
                    )
    except ValueError as e:
        await update.effective_message.reply_text(str(e))
        return
    except asyncpg.UniqueViolationError:
        await update.effective_message.reply_text("That number was just taken. Please choose another.")
        return
    context.user_data["awaiting_number"] = False
    context.user_data["awaiting_receipt_ticket_id"] = int(ticket_id)
    await update.effective_message.reply_text(
        f"Number {number} reserved successfully.\n"
        f"Ticket ID: #{ticket_id}\n"
        f"Type: {ticket_type.title()}"
        + (f" (half {share_no})" if share_no else "")
        + f"\nAmount due: {money(amount)}\n"
        f"Reservation expires in {reservation_minutes} minutes.\n\n"
        f"{await get_setting('payment_instructions', DEFAULT_SETTINGS['payment_instructions'])}\n\n"
        "Now upload a clear receipt photo or send an image as a document."
    )


async def my_tickets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard_user(update, context):
        return
    pool = await db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, lottery_id, number, ticket_type, share_no, amount, status, created_at, expires_at
               FROM tickets WHERE user_id=$1 ORDER BY created_at DESC LIMIT 20""",
            update.effective_user.id,
        )
    if not rows:
        await update.effective_message.reply_text("You have no tickets yet.")
        return
    lines = ["Your latest tickets:"]
    for r in rows:
        share = f", half {r['share_no']}" if r["ticket_type"] == "half" else ""
        status = r["status"].upper()
        if status == "RESERVED" and r["expires_at"] < datetime.now(timezone.utc):
            status = "EXPIRED"
        lines.append(
            f"#{r['id']} | No. {r['number']} | {r['ticket_type'].title()}{share} | "
            f"{money(r['amount'])} | {status}"
        )
    await update.effective_message.reply_text("\n".join(lines))


async def payment_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard_user(update, context):
        return
    pool = await db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT p.id, p.ticket_id, p.amount, p.status, p.created_at, p.review_note,
                      t.number, t.ticket_type
               FROM payments p JOIN tickets t ON t.id=p.ticket_id
               WHERE p.user_id=$1 ORDER BY p.created_at DESC LIMIT 20""",
            update.effective_user.id,
        )
    if not rows:
        await update.effective_message.reply_text("You have no payment receipts in history.")
        return
    lines = ["Payment history:"]
    for r in rows:
        note = f" — {r['review_note']}" if r["review_note"] else ""
        lines.append(
            f"Payment #{r['id']} / Ticket #{r['ticket_id']} / No. {r['number']} / "
            f"{money(r['amount'])} / {r['status'].upper()}{note}"
        )
    await update.effective_message.reply_text("\n".join(lines))


async def handle_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard_user(update, context):
        return
    ticket_id = context.user_data.get("awaiting_receipt_ticket_id")
    if not ticket_id:
        await update.effective_message.reply_text("Reserve a ticket first, then upload its receipt.")
        return
    message = update.effective_message
    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.document and (message.document.mime_type or "").startswith("image/"):
        file_id = message.document.file_id
    if not file_id:
        await message.reply_text("Please upload a receipt as a photo or an image document.")
        return

    pool = await db()
    async with pool.acquire() as conn:
        async with conn.transaction():
            ticket = await conn.fetchrow(
                """SELECT id, user_id, amount, status, expires_at FROM tickets
                   WHERE id=$1 AND user_id=$2 FOR UPDATE""",
                int(ticket_id), update.effective_user.id,
            )
            if not ticket:
                context.user_data.pop("awaiting_receipt_ticket_id", None)
                await message.reply_text("Ticket not found. Please reserve a ticket again.")
                return
            if ticket["status"] != "reserved" or ticket["expires_at"] < datetime.now(timezone.utc):
                await conn.execute("UPDATE tickets SET status='expired' WHERE id=$1 AND status='reserved'", int(ticket_id))
                context.user_data.pop("awaiting_receipt_ticket_id", None)
                await message.reply_text("This reservation expired. Please reserve a number again.")
                return
            existing = await conn.fetchval(
                "SELECT id FROM payments WHERE ticket_id=$1 AND status='pending' LIMIT 1",
                int(ticket_id),
            )
            if existing:
                await message.reply_text(f"Receipt for ticket #{ticket_id} is already waiting for admin review.")
                return
            payment_id = await conn.fetchval(
                """INSERT INTO payments(ticket_id,user_id,amount,receipt_file_id,receipt_caption)
                   VALUES($1,$2,$3,$4,$5) RETURNING id""",
                int(ticket_id), update.effective_user.id, ticket["amount"], file_id,
                message.caption or "",
            )
            ticket_info = await conn.fetchrow(
                "SELECT number,ticket_type,share_no FROM tickets WHERE id=$1", int(ticket_id)
            )

    context.user_data.pop("awaiting_receipt_ticket_id", None)
    await message.reply_text(
        f"Receipt submitted for ticket #{ticket_id}. Payment #{payment_id} is pending admin approval."
    )
    admin_keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Approve", callback_data=f"pay:approve:{payment_id}"),
        InlineKeyboardButton("❌ Reject", callback_data=f"pay:reject:{payment_id}"),
    ]])
    username = update.effective_user.username
    try:
        await context.bot.send_photo(
            chat_id=ADMIN_ID,
            photo=file_id,
            caption=(
                f"Payment review needed\nPayment #{payment_id}\nTicket #{ticket_id}\n"
                f"User ID: {update.effective_user.id}\nUsername: @{username or 'none'}\n"
                f"Number: {ticket_info['number']}\nType: {ticket_info['ticket_type']}"
                + (f" (half {ticket_info['share_no']})" if ticket_info["share_no"] else "")
                + f"\nAmount: {money(ticket['amount'])}\nCaption: {message.caption or '-'}"
            ),
            reply_markup=admin_keyboard,
        )
    except TelegramError:
        log.exception("Could not notify admin about payment %s", payment_id)
        await message.reply_text("Your receipt is saved, but the administrator could not be notified automatically. Please contact support.")


async def payment_review_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin(update):
        await query.answer("Not authorized", show_alert=True)
        return
    await query.answer()
    _, action, payment_id_raw = query.data.split(":")
    payment_id = int(payment_id_raw)
    pool = await db()
    user_id = None
    ticket_id = None
    async with pool.acquire() as conn:
        async with conn.transaction():
            payment = await conn.fetchrow(
                "SELECT * FROM payments WHERE id=$1 FOR UPDATE", payment_id
            )
            if not payment:
                await query.edit_message_caption("Payment record not found.")
                return
            if payment["status"] != "pending":
                try:
                    await query.edit_message_caption(
                        (query.message.caption or "") + f"\n\nAlready reviewed: {payment['status'].upper()}."
                    )
                except TelegramError:
                    pass
                return
            user_id = payment["user_id"]
            ticket_id = payment["ticket_id"]
            if action == "approve":
                ticket = await conn.fetchrow(
                    "SELECT status, expires_at FROM tickets WHERE id=$1 FOR UPDATE", ticket_id
                )
                if not ticket or ticket["status"] != "reserved":
                    await conn.execute(
                        """UPDATE payments SET status='rejected', reviewed_by=$2,
                           reviewed_at=NOW(), review_note='Reservation is no longer active'
                           WHERE id=$1""",
                        payment_id, ADMIN_ID,
                    )
                    await query.edit_message_caption("Cannot approve: reservation is no longer active.")
                    return
                await conn.execute("UPDATE payments SET status='approved', reviewed_by=$2, reviewed_at=NOW() WHERE id=$1",
                                   payment_id, ADMIN_ID)
                await conn.execute("UPDATE tickets SET status='paid' WHERE id=$1", ticket_id)
                note = "approved"
            else:
                await conn.execute(
                    """UPDATE payments SET status='rejected', reviewed_by=$2, reviewed_at=NOW(),
                       review_note='Rejected by administrator' WHERE id=$1""",
                    payment_id, ADMIN_ID,
                )
                note = "rejected"
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=(f"Payment #{payment_id} for ticket #{ticket_id} has been {note} by the administrator."
                  + ("\nYour ticket is confirmed." if note == "approved" else
                     "\nYou can upload a corrected receipt while your reservation remains active. If it expires, reserve again.")),
        )
    except TelegramError:
        log.warning("Could not notify user %s about payment %s", user_id, payment_id)
    try:
        await query.edit_message_caption(
            (query.message.caption or "") + f"\n\nReview result: {note.upper()} by admin {ADMIN_ID}"
        )
    except TelegramError:
        pass


async def admin_required(update: Update) -> bool:
    if not await is_admin(update):
        if update.effective_message:
            await update.effective_message.reply_text("Administrator only.")
        return False
    await register_user(update)
    return True


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_required(update):
        return
    await update.effective_message.reply_text("Administrator menu:", reply_markup=ADMIN_MENU)


async def admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_required(update):
        return
    pool = await db()
    async with pool.acquire() as conn:
        users = await conn.fetchval("SELECT COUNT(*) FROM users")
        blocked_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE is_blocked=TRUE")
        pending = await conn.fetchval("SELECT COUNT(*) FROM payments WHERE status='pending'")
        paid = await conn.fetchval("SELECT COUNT(*) FROM tickets WHERE status='paid'")
        reserved = await conn.fetchval("SELECT COUNT(*) FROM tickets WHERE status='reserved' AND expires_at>NOW()")
        revenue = await conn.fetchval("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved'")
    await update.effective_message.reply_text(
        f"Admin dashboard\nUsers: {users} (blocked: {blocked_users})\n"
        f"Pending payments: {pending}\nPaid tickets: {paid}\nActive reservations: {reserved}\n"
        f"Approved payments: {money(revenue)}",
        reply_markup=ADMIN_MENU,
    )


async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_required(update):
        return
    keys = ["lottery_id", "lottery_name", "full_price", "half_price", "reservation_minutes",
            "payment_instructions", "number_min", "number_max", "sales_open"]
    values = {k: await get_setting(k, DEFAULT_SETTINGS.get(k, "")) for k in keys}
    await update.effective_message.reply_text(
        "Current settings:\n"
        + "\n".join(f"{k}: {values[k]}" for k in keys)
        + "\n\nSet a value with one of these commands:\n"
        "/set full_price 100\n/set half_price 50\n/set reservation_minutes 30\n"
        "/set lottery_name My Lottery\n/set lottery_id main\n/set number_min 0\n"
        "/set number_max 9999\n/set sales_open true\n"
        "/set payment_instructions Your payment instructions",
        reply_markup=ADMIN_MENU,
    )


async def set_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_required(update):
        return
    if len(context.args) < 2:
        await update.effective_message.reply_text("Usage: /set KEY VALUE")
        return
    key = context.args[0].lower()
    value = " ".join(context.args[1:]).strip()
    allowed = set(DEFAULT_SETTINGS)
    if key not in allowed:
        await update.effective_message.reply_text("Unknown setting. Use /settings to see supported settings.")
        return
    if key in ("full_price", "half_price"):
        try:
            if Decimal(value) < 0:
                raise InvalidOperation
            value = str(Decimal(value).quantize(Decimal("0.01")))
        except (InvalidOperation, ValueError):
            await update.effective_message.reply_text("Price must be a non-negative number, e.g. /set full_price 100")
            return
    if key in ("reservation_minutes", "number_min", "number_max"):
        try:
            number = int(value)
            if key == "reservation_minutes" and not 1 <= number <= 1440:
                raise ValueError
            if key in ("number_min", "number_max") and not 0 <= number <= 9999:
                raise ValueError
        except ValueError:
            await update.effective_message.reply_text("Invalid numeric setting.")
            return
    if key == "sales_open" and value.lower() not in ("true", "false"):
        await update.effective_message.reply_text("sales_open must be true or false.")
        return
    await set_setting(key, value.lower() if key == "sales_open" else value)
    await update.effective_message.reply_text(f"Updated {key}.")


async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_required(update):
        return
    pool = await db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT user_id, username, first_name, is_blocked, created_at
               FROM users ORDER BY created_at DESC LIMIT 30"""
        )
    if not rows:
        await update.effective_message.reply_text("No users yet.")
        return
    lines = ["Latest users (up to 30):"]
    for r in rows:
        lines.append(
            f"{r['user_id']} | {r['first_name']} | @{r['username'] or 'none'} | "
            f"{'BLOCKED' if r['is_blocked'] else 'active'}"
        )
    lines.append("\nBlock/unblock: /blockuser USER_ID or /unblockuser USER_ID")
    await update.effective_message.reply_text("\n".join(lines), reply_markup=ADMIN_MENU)


async def block_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_required(update):
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await update.effective_message.reply_text("Usage: /blockuser USER_ID")
        return
    target = int(context.args[0])
    if target == ADMIN_ID:
        await update.effective_message.reply_text("You cannot block the configured administrator.")
        return
    pool = await db()
    async with pool.acquire() as conn:
        result = await conn.execute("UPDATE users SET is_blocked=TRUE WHERE user_id=$1", target)
    await update.effective_message.reply_text("User blocked." if result.endswith("1") else "User not found.")


async def unblock_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_required(update):
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await update.effective_message.reply_text("Usage: /unblockuser USER_ID")
        return
    pool = await db()
    async with pool.acquire() as conn:
        result = await conn.execute("UPDATE users SET is_blocked=FALSE WHERE user_id=$1", int(context.args[0]))
    await update.effective_message.reply_text("User unblocked." if result.endswith("1") else "User not found.")


async def block_number_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_required(update):
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await update.effective_message.reply_text("Usage: /blocknumber NUMBER")
        return
    number = int(context.args[0])
    low, high = int(await get_setting("number_min", "0")), int(await get_setting("number_max", "9999"))
    if not low <= number <= high:
        await update.effective_message.reply_text(f"Number must be from {low} to {high}.")
        return
    pool = await db()
    async with pool.acquire() as conn:
        lottery_id = await get_setting("lottery_id", DEFAULT_LOTTERY_ID)
        active = await conn.fetchval(
            """SELECT EXISTS(SELECT 1 FROM allocations a JOIN tickets t ON t.id=a.ticket_id
               WHERE a.lottery_id=$1 AND a.number=$2 AND t.status IN ('reserved','paid')
               AND (t.status='paid' OR t.expires_at>NOW()))""",
            lottery_id, number,
        )
        if active:
            await update.effective_message.reply_text("That number has an active reservation or paid ticket; it cannot be blocked.")
            return
        await conn.execute(
            """INSERT INTO blocked_numbers(lottery_id,number,blocked_by)
               VALUES($1,$2,$3) ON CONFLICT(lottery_id,number) DO NOTHING""",
            lottery_id, number, ADMIN_ID,
        )
    await update.effective_message.reply_text(f"Number {number} blocked for lottery {lottery_id}.")


async def unblock_number_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_required(update):
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await update.effective_message.reply_text("Usage: /unblocknumber NUMBER")
        return
    pool = await db()
    async with pool.acquire() as conn:
        lottery_id = await get_setting("lottery_id", DEFAULT_LOTTERY_ID)
        result = await conn.execute(
            "DELETE FROM blocked_numbers WHERE lottery_id=$1 AND number=$2",
            lottery_id, int(context.args[0]),
        )
    await update.effective_message.reply_text("Number unblocked." if result.endswith("1") else "That number was not blocked.")


async def blocked_numbers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_required(update):
        return
    pool = await db()
    async with pool.acquire() as conn:
        lottery_id = await get_setting("lottery_id", DEFAULT_LOTTERY_ID)
        rows = await conn.fetch(
            "SELECT number FROM blocked_numbers WHERE lottery_id=$1 ORDER BY number LIMIT 100",
            lottery_id,
        )
    await update.effective_message.reply_text(
        "Blocked numbers: " + (", ".join(str(r["number"]) for r in rows) if rows else "none")
        + "\nUse /blocknumber NUMBER or /unblocknumber NUMBER."
    )


async def pending_payments_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_required(update):
        return
    pool = await db()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT p.id,p.ticket_id,p.user_id,p.amount,p.created_at,t.number
               FROM payments p JOIN tickets t ON t.id=p.ticket_id
               WHERE p.status='pending' ORDER BY p.created_at LIMIT 20"""
        )
    if not rows:
        await update.effective_message.reply_text("No pending payments.", reply_markup=ADMIN_MENU)
        return
    lines = ["Pending payments (receipt was sent to the admin chat when submitted):"]
    for r in rows:
        lines.append(
            f"Payment #{r['id']} | Ticket #{r['ticket_id']} | No. {r['number']} | "
            f"User {r['user_id']} | {money(r['amount'])}"
        )
    await update.effective_message.reply_text("\n".join(lines), reply_markup=ADMIN_MENU)


async def expire_reservations(context: ContextTypes.DEFAULT_TYPE):
    pool = await db()
    async with pool.acquire() as conn:
        # Keep tickets with a pending receipt in reserved state until admin reviews them.
        await conn.execute(
            """UPDATE tickets t SET status='expired'
               WHERE t.status='reserved' AND t.expires_at<NOW()
                 AND NOT EXISTS(SELECT 1 FROM payments p
                                WHERE p.ticket_id=t.id AND p.status='pending')"""
        )
        await conn.execute(
            """DELETE FROM allocations a USING tickets t
               WHERE a.ticket_id=t.id AND t.status IN ('expired','cancelled')"""
        )


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.effective_message.text or "").strip()
    if text == "🎟 Buy Ticket":
        await buy_ticket(update, context)
    elif text == "🎫 My Tickets":
        await my_tickets(update, context)
    elif text == "💳 Payment History":
        await payment_history(update, context)
    elif text == "ℹ️ Payment Info":
        await payment_info(update, context)
    elif text == "❓ Help":
        await help_command(update, context)
    elif text == "📊 Admin Dashboard":
        await admin_dashboard(update, context)
    elif text == "⚙️ Settings":
        await settings_menu(update, context)
    elif text == "👥 Users":
        await users_command(update, context)
    elif text == "🚫 Block Number":
        await update.effective_message.reply_text("Use /blocknumber NUMBER")
    elif text == "✅ Unblock Number":
        await update.effective_message.reply_text("Use /unblocknumber NUMBER")
    elif text == "🧾 Pending Payments":
        await pending_payments_command(update, context)
    elif text == "⬅️ Main Menu":
        if await is_admin(update):
            await update.effective_message.reply_text("Main menu:", reply_markup=MAIN_MENU)
        else:
            await update.effective_message.reply_text("Main menu:", reply_markup=MAIN_MENU)
    elif context.user_data.get("awaiting_number"):
        if not text.isdigit():
            await update.effective_message.reply_text("Send a whole number, for example 25.")
            return
        await create_reservation(update, context, int(text))
    else:
        if not await guard_user(update, context):
            return
        await update.effective_message.reply_text("Please choose an option from the menu or type /help.", reply_markup=MAIN_MENU)


async def post_init(application: Application):
    await init_db()
    application.job_queue.run_repeating(expire_reservations, interval=60, first=60)
    log.info("Bot is ready")


async def post_shutdown(application: Application):
    global POOL
    if POOL:
        await POOL.close()
        POOL = None


def main():
    application = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("admin", admin_command))
    application.add_handler(CommandHandler("settings", settings_menu))
    application.add_handler(CommandHandler("set", set_command))
    application.add_handler(CommandHandler("users", users_command))
    application.add_handler(CommandHandler("blockuser", block_user_command))
    application.add_handler(CommandHandler("unblockuser", unblock_user_command))
    application.add_handler(CommandHandler("blocknumber", block_number_command))
    application.add_handler(CommandHandler("unblocknumber", unblock_number_command))
    application.add_handler(CommandHandler("blockednumbers", blocked_numbers_command))
    application.add_handler(CommandHandler("pendingpayments", pending_payments_command))
    application.add_handler(CallbackQueryHandler(buy_type_callback, pattern=r"^buy:(full|half)$"))
    application.add_handler(CallbackQueryHandler(payment_review_callback, pattern=r"^pay:(approve|reject):\d+$"))
    application.add_handler(MessageHandler(filters.PHOTO | filters.Document.IMAGE, handle_receipt))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
