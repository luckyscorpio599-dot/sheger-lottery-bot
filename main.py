"""Telegram lottery bot with admin-verified payments and PostgreSQL persistence.

Required environment variables:
    TELEGRAM_BOT_TOKEN  Telegram bot token from BotFather.
    TELEGRAM_ADMIN_ID   Numeric Telegram user ID of the administrator.
    DATABASE_URL        Render PostgreSQL connection URL.
"""

import asyncio
from contextlib import contextmanager
import html
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import re
import secrets
import psycopg2
from psycopg2.extras import DictCursor

from telegram import (
    CopyTextButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.error import TelegramError, RetryAfter
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

DATABASE_URL = (os.getenv("DATABASE_URL") or os.getenv("RENDER_DATABASE_URL") or "").strip()

DEFAULT_SETTINGS = {
    "lottery_name": "ሸገር ሎተሪ",
    "welcome_instructions": (
        "📌 የተሳትፎ መመሪያ፦\n"
        "1️⃣ የዕጣ አይነት ይምረጡ።\n"
        "2️⃣ ክፍያ ፈጽመው ደረሰኙን ይላኩ።\n"
        "3️⃣ አድሚን ካጸደቀው በኋላ የዕድል ቁጥር ይምረጡ።"
    ),
    "welcome_prompt": "ለመጀመር ከታች የዕጣ አይነት ይምረጡ፦",
    "payment_intro": (
        "እባክዎ የመረጡትን የዕጣ መጠን ክፍያ በሚከተለው የባንክ ሂሳብ ያስገቡ፦"
    ),
    "receipt_instruction": (
        "📷 ክፍያውን ከፈጸሙ በኋላ የደረሰኙን ፎቶ ወደዚህ ይላኩ።"
    ),
    "bank_name": "",
    "bank_account": "",
    "account_holder": "",
    "full_ticket_label": "ሙሉ ዕጣ",
    "full_ticket_price": "2500",
    "half_ticket_label": "ግማሽ ዕጣ",
    "half_ticket_price": "1250",
    "first_number": "1",
    "last_number": "6000",
}

ADMIN_SETTING_FIELDS = {
    "lottery_name": ("🏷️ የሎተሪ ስም", "text"),
    "welcome_instructions": ("📋 የተሳትፎ መመሪያ", "text"),
    "welcome_prompt": ("👋 የመጀመሪያ መልእክት", "text"),
    "payment_intro": ("💳 የክፍያ መመሪያ", "text"),
    "receipt_instruction": ("🧾 የደረሰኝ መመሪያ", "text"),
    "bank_name": ("🏦 የባንክ ስም", "text"),
    "bank_account": ("🔢 የሂሳብ ቁጥር", "text"),
    "account_holder": ("👤 የሂሳብ ባለቤት", "text"),
    "full_ticket_label": ("🎫 የሙሉ ዕጣ ስም", "text"),
    "full_ticket_price": ("💵 የሙሉ ዕጣ ዋጋ", "price"),
    "half_ticket_label": ("🎟️ የግማሽ ዕጣ ስም", "text"),
    "half_ticket_price": ("💵 የግማሽ ዕጣ ዋጋ", "price"),
    "first_number": ("🔢 የመጀመሪያ ቁጥር", "number"),
    "last_number": ("🔢 የመጨረሻ ቁጥር", "number"),
    "pending_payment_hours": ("⏳ Pending የክፍያ ጊዜ (ሰዓት)", "number"),
    "draw_datetime": ("📅 የዕጣ ማውጫ ቀንና ሰዓት", "text"),
    "draw_status": ("📌 የዕጣ ቀን ሁኔታ", "text"),
    "public_sales_report": ("📊 ለተጠቃሚ የሚታይ የሽያጭ ሪፖርት", "text"),
    "support_text": ("🆘 Support መረጃ", "text"),
    "tutorial_text": ("📚 Tutorial ጽሑፍ", "text"),
}


class TelegramTokenRedactionFilter(logging.Filter):
    @staticmethod
    def redact(value):
        value = re.sub(r"(/bot)[^/\s?]+", r"\1[REDACTED]", value)
        return re.sub(
            r"(?<!\w)\d{8,}:[A-Za-z0-9_-]{20,}",
            "[REDACTED_TELEGRAM_TOKEN]",
            value,
        )

    def filter(self, record):
        record.msg = self.redact(record.getMessage())
        record.args = ()
        if record.exc_info:
            formatter = logging.Formatter()
            record.exc_text = self.redact(formatter.formatException(record.exc_info))
            record.exc_info = None
        elif record.exc_text:
            record.exc_text = self.redact(record.exc_text)
        return True


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
for handler in logging.getLogger().handlers:
    handler.addFilter(TelegramTokenRedactionFilter())
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Small compatibility wrapper so bot query/business logic stays unchanged."""

    def __init__(self, connection):
        self.connection = connection

    @staticmethod
    def _sql(query):
        # Translate the small set of SQLite SQL forms used by the bot to PostgreSQL.
        query = query.replace("?", "%s")
        query = re.sub(r"\bBEGIN\s+IMMEDIATE\b", "BEGIN", query, flags=re.IGNORECASE)
        query = re.sub(
            r"GROUP_CONCAT\(([^)]+)\)",
            r"STRING_AGG(CAST(\1 AS TEXT), ',')",
            query,
            flags=re.IGNORECASE,
        )
        ignored = query.lstrip().upper().startswith("INSERT OR IGNORE")
        if ignored:
            query = query.replace("INSERT OR IGNORE", "INSERT", 1)
            query = query.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
        return query

    def execute(self, query, params=None):
        cursor = self.connection.cursor(cursor_factory=DictCursor)
        cursor.execute(self._sql(query), params or ())
        return cursor

    def executemany(self, query, params):
        cursor = self.connection.cursor(cursor_factory=DictCursor)
        cursor.executemany(self._sql(query), params)
        return cursor

    def executescript(self, script):
        cursor = self.connection.cursor(cursor_factory=DictCursor)
        for statement in script.split(";"):
            if statement.strip():
                cursor.execute(statement)
        return cursor

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()


def connect_db():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is missing. Add the Internal Database URL from your Render PostgreSQL database to the bot's environment variables."
        )
    connection_url = DATABASE_URL
    if "sslmode=" not in connection_url:
        separator = "&" if "?" in connection_url else "?"
        connection_url += separator + "sslmode=require"
    raw_connection = psycopg2.connect(connection_url, connect_timeout=15)
    raw_connection.autocommit = False
    return DatabaseConnection(raw_connection)


@contextmanager
def db_session():
    db = connect_db()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def infer_ticket_code(ticket_code, ticket_type="", settings=None):
    if ticket_code in {"full", "half"}:
        return ticket_code
    value = (ticket_type or "").casefold()
    if settings:
        half_label = (settings.get("half_ticket_label") or "").casefold()
        full_label = (settings.get("full_ticket_label") or "").casefold()
        if half_label and value.startswith(half_label):
            return "half"
        if full_label and value.startswith(full_label):
            return "full"
    if "half" in value or "ግማሽ" in value:
        return "half"
    return "full"


def init_db():
    with db_session() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS payments (
                id BIGSERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                ticket_type TEXT NOT NULL,
                ticket_code TEXT,
                receipt_file_id TEXT NOT NULL,
                status TEXT NOT NULL
                    CHECK (status IN ('pending', 'approved', 'rejected', 'assigned', 'failed')),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE UNIQUE INDEX IF NOT EXISTS one_active_payment_per_user
                ON payments(user_id)
                WHERE status IN ('pending', 'approved');

            CREATE TABLE IF NOT EXISTS tickets (
                number TEXT PRIMARY KEY,
                payment_id INTEGER NOT NULL UNIQUE
                    REFERENCES payments(id),
                user_id INTEGER NOT NULL,
                assigned_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS bot_settings (
                setting_key TEXT PRIMARY KEY,
                setting_value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                display_name TEXT NOT NULL,
                phone_number TEXT NOT NULL,
                username TEXT,
                address TEXT,
                registered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS ticket_allocations (
                number TEXT NOT NULL,
                half_slot INTEGER NOT NULL CHECK (half_slot IN (1, 2)),
                payment_id INTEGER NOT NULL REFERENCES payments(id),
                user_id INTEGER NOT NULL,
                assigned_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (number, half_slot)
            );
            CREATE INDEX IF NOT EXISTS ticket_allocations_payment
                ON ticket_allocations(payment_id);
            CREATE INDEX IF NOT EXISTS ticket_allocations_user
                ON ticket_allocations(user_id);

            CREATE TABLE IF NOT EXISTS blocked_numbers (
                number TEXT PRIMARY KEY,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                reserved_name TEXT
            );

            CREATE TABLE IF NOT EXISTS schema_migrations (
                migration_key TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        blocked_columns = {row["column_name"] for row in db.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='blocked_numbers'"
        ).fetchall()}
        if "reserved_name" not in blocked_columns:
            db.execute("ALTER TABLE blocked_numbers ADD COLUMN reserved_name TEXT")
        payment_columns = {
            row["column_name"]
            for row in db.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = 'payments'"
            ).fetchall()
        }
        if "ticket_code" not in payment_columns:
            db.execute("ALTER TABLE payments ADD COLUMN ticket_code TEXT")
        if "ticket_type" not in payment_columns:
            # Older PostgreSQL databases may predate the ticket_type column.
            # Add it without dropping or replacing existing payment records.
            db.execute("ALTER TABLE payments ADD COLUMN ticket_type TEXT")
            db.execute(
                """
                UPDATE payments
                SET ticket_type = CASE
                    WHEN ticket_code = 'half' THEN 'ግማሽ'
                    ELSE 'ሙሉ'
                END
                WHERE ticket_type IS NULL OR ticket_type = ''
                """
            )
        payment_columns = {row["column_name"] for row in db.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='payments'"
        ).fetchall()}
        if "payment_method_id" not in payment_columns:
            db.execute("ALTER TABLE payments ADD COLUMN payment_method_id BIGINT")
        if "ticket_quantity" not in payment_columns:
            db.execute("ALTER TABLE payments ADD COLUMN ticket_quantity INTEGER NOT NULL DEFAULT 1")
        if "total_amount" not in payment_columns:
            db.execute("ALTER TABLE payments ADD COLUMN total_amount NUMERIC NOT NULL DEFAULT 0")
        # Older versions limited one payment to at most two allocation rows.
        # Remove that obsolete constraint so one payment can cover multiple tickets.
        unique_constraints = db.execute("""SELECT conname FROM pg_constraint
            WHERE conrelid='ticket_allocations'::regclass AND contype='u'
            AND pg_get_constraintdef(oid) LIKE '%%payment_id%%half_slot%%'""").fetchall()
        for constraint in unique_constraints:
            db.execute(f'ALTER TABLE ticket_allocations DROP CONSTRAINT "{constraint["conname"]}"')
        # Additive v3 schema: preserves existing users, payments and allocations.
        user_columns = {row["column_name"] for row in db.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='users'"
        ).fetchall()}
        if "address" not in user_columns:
            db.execute("ALTER TABLE users ADD COLUMN address TEXT")
        db.executescript("""
            CREATE TABLE IF NOT EXISTS payment_methods (
                id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, account TEXT NOT NULL DEFAULT '',
                holder TEXT NOT NULL DEFAULT '', instruction TEXT NOT NULL DEFAULT '',
                active BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS user_profile_history (
                id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, old_name TEXT, new_name TEXT,
                old_phone TEXT, new_phone TEXT, old_address TEXT, new_address TEXT,
                changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS lottery_content (
                content_key TEXT PRIMARY KEY, content_value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS broadcast_log (
                id BIGSERIAL PRIMARY KEY, admin_id BIGINT NOT NULL, body TEXT NOT NULL,
                sent_count INTEGER NOT NULL DEFAULT 0, failed_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS admin_audit_log (
                id BIGSERIAL PRIMARY KEY, admin_id BIGINT NOT NULL, action TEXT NOT NULL,
                details TEXT NOT NULL DEFAULT '', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS lottery_draw_results (
                id BIGSERIAL PRIMARY KEY, winning_number TEXT NOT NULL, announcement TEXT NOT NULL DEFAULT '',
                admin_id BIGINT NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # v3 settings; expiry is intentionally admin-configurable.
        for key, value in {
            "pending_payment_hours": "24", "draw_datetime": "Not announced",
            "draw_status": "ቀን ተወስኗል", "public_sales_report": "",
            "support_text": "", "tutorial_text": "", "tutorial_file_id": "",
            "tutorial_file_type": "", "report_target": "", "report_note": "",
        }.items():
            db.execute("INSERT OR IGNORE INTO bot_settings(setting_key,setting_value) VALUES (?,?)", (key,value))
        db.executemany(
            """
            INSERT OR IGNORE INTO bot_settings (setting_key, setting_value)
            VALUES (?, ?)
            """,
            DEFAULT_SETTINGS.items(),
        )
        payment_settings_migration = "clear_demo_payment_settings_v1"
        if not db.execute(
            "SELECT 1 FROM schema_migrations WHERE migration_key = ?",
            (payment_settings_migration,),
        ).fetchone():
            # Earlier versions seeded these demonstration values into SQLite.
            # Clear them once so setup cannot accidentally direct payments there.
            legacy_payment_defaults = {
                "bank_name": "ንግድ ባንክ (CBE)",
                "bank_account": "1000000000000",
                "account_holder": "ሸገር ሎተሪ",
            }
            for key, old_value in legacy_payment_defaults.items():
                db.execute(
                    """
                    UPDATE bot_settings SET setting_value = ''
                    WHERE setting_key = ? AND setting_value = ?
                    """,
                    (key, old_value),
                )
            db.execute(
                "INSERT INTO schema_migrations (migration_key) VALUES (?)",
                (payment_settings_migration,),
            )
        migration_settings = DEFAULT_SETTINGS.copy()
        migration_settings.update(
            {
                row["setting_key"]: row["setting_value"]
                for row in db.execute(
                    "SELECT setting_key, setting_value FROM bot_settings"
                ).fetchall()
            }
        )
        payments_without_code = db.execute(
            "SELECT id, ticket_type FROM payments WHERE ticket_code IS NULL"
        ).fetchall()
        for payment in payments_without_code:
            db.execute(
                "UPDATE payments SET ticket_code = ? WHERE id = ?",
                (
                    infer_ticket_code(
                        None, payment["ticket_type"], migration_settings
                    ),
                    payment["id"],
                ),
            )
        legacy_migration = db.execute(
            "SELECT 1 FROM schema_migrations WHERE migration_key = ?",
            ("legacy_tickets_to_slots_v1",),
        ).fetchone()
        if not legacy_migration:
            # Older databases may have a legacy `tickets` table with a different
            # layout. Only read it when every column required by this migration exists.
            legacy_ticket_columns = {
                row["column_name"]
                for row in db.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = current_schema() AND table_name = 'tickets'"
                ).fetchall()
            }
            required_legacy_columns = {"number", "payment_id", "user_id", "assigned_at"}
            if required_legacy_columns.issubset(legacy_ticket_columns):
                legacy_rows = db.execute(
                    """
                    SELECT t.number, t.payment_id, t.user_id, t.assigned_at,
                           p.ticket_code, p.ticket_type
                    FROM tickets t
                    JOIN payments p ON p.id = t.payment_id
                    ORDER BY p.id
                    """
                ).fetchall()
            else:
                # No compatible legacy ticket schema exists. The current bot uses
                # ticket_allocations; do not crash startup trying to migrate it.
                legacy_rows = []
            for row in legacy_rows:
                code = infer_ticket_code(
                    row["ticket_code"], row["ticket_type"], migration_settings
                )
                db.execute(
                    "UPDATE payments SET ticket_code = ? WHERE id = ? AND ticket_code IS NULL",
                    (code, row["payment_id"]),
                )
                slots = (1, 2) if code == "full" else (1,)
                for slot in slots:
                    db.execute(
                        """
                        INSERT OR IGNORE INTO ticket_allocations
                            (number, half_slot, payment_id, user_id, assigned_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            row["number"],
                            slot,
                            row["payment_id"],
                            row["user_id"],
                            row["assigned_at"],
                        ),
                    )
            db.execute(
                "INSERT INTO schema_migrations (migration_key) VALUES (?)",
                ("legacy_tickets_to_slots_v1",),
            )


def get_settings():
    with db_session() as db:
        rows = db.execute(
            "SELECT setting_key, setting_value FROM bot_settings"
        ).fetchall()
    settings = DEFAULT_SETTINGS.copy()
    settings.update({row["setting_key"]: row["setting_value"] for row in rows})
    return settings


def active_payment_methods():
    with db_session() as db:
        return db.execute("SELECT * FROM payment_methods WHERE active=TRUE ORDER BY id").fetchall()

async def prompt_payment_method(message, context, ticket_code):
    methods=active_payment_methods()
    if methods:
        rows=[[InlineKeyboardButton(m["name"],callback_data=f"paymethod:{m['id']}")] for m in methods]
        await message.reply_text("የክፍያ አማራጭ ይምረጡ፦",reply_markup=InlineKeyboardMarkup(rows))
        return
    settings=get_settings()
    if not bank_details_configured(settings):
        await message.reply_text("የክፍያ መረጃው ገና አልተዘጋጀም። እባክዎ አድሚኑን ያነጋግሩ።")
        return
    context.user_data["payment_method_id"]=None
    await message.reply_text(bank_info_text(settings,ticket_code),parse_mode="HTML",reply_markup=bank_info_keyboard(settings))

async def payment_method_callback(update, context):
    q=update.callback_query
    if not await require_private_chat(update): return
    if get_registered_user(q.from_user.id) is None:
        await q.answer("መጀመሪያ ይመዝገቡ።",show_alert=True); return
    try: mid=int((q.data or '').split(':',1)[1])
    except (ValueError,IndexError): await q.answer(); return
    with db_session() as db: method=db.execute("SELECT * FROM payment_methods WHERE id=? AND active=TRUE",(mid,)).fetchone()
    if not method: await q.answer("ይህ የክፍያ አማራጭ አይገኝም።",show_alert=True); return
    await q.answer()
    context.user_data['payment_method_id']=mid
    ticket_code=context.user_data.get('ticket_code')
    if ticket_code not in {'full','half'}:
        await q.message.reply_text("እባክዎ የዕጣ አይነት እንደገና ይምረጡ።"); return
    settings=get_settings()
    quantity = max(1, int(context.user_data.get('ticket_quantity', 1)))
    price=ticket_types(settings)[ticket_code]['price']
    total_price = price * quantity
    body=(f"💳 <b>{html.escape(method['name'])}</b>\n\nየትኬት ብዛት፦ {quantity}\nየአንዱ ዋጋ፦ {format_price(price)} ብር\n<b>ጠቅላላ ክፍያ፦ {format_price(total_price)} ብር</b>\n"
          f"የሂሳብ/ስልክ ቁጥር፦ <code>{html.escape(method['account'])}</code>\n"
          f"የባለቤት ስም፦ {html.escape(method['holder'])}\n\n"
          f"{html.escape(method['instruction'])}\n\n{html.escape(settings['receipt_instruction'])}")
    await q.message.reply_text(
        body,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
            "📋 የሂሳብ ቁጥሩን ቅዳ", copy_text=CopyTextButton(text=method['account'])
        )]]),
    )

def bank_details_configured(settings=None):
    settings = settings or get_settings()
    return all(
        (settings.get(key) or "").strip()
        for key in ("bank_name", "bank_account", "account_holder")
    )


def update_setting(key, value):
    allowed_feature_settings = {"public_sales_report", "tutorial_file_id", "tutorial_file_type", "tutorial_text"}
    if key not in ADMIN_SETTING_FIELDS and key not in allowed_feature_settings:
        raise ValueError("Unknown setting.")
    with db_session() as db:
        db.execute(
            """
            INSERT INTO bot_settings (setting_key, setting_value) VALUES (?, ?)
            ON CONFLICT(setting_key) DO UPDATE SET setting_value = excluded.setting_value
            """,
            (key, value),
        )


def get_registered_user(user_id):
    with db_session() as db:
        return db.execute(
            "SELECT user_id, display_name, phone_number, username, address FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()


def save_registered_user(user_id, display_name, phone_number, username):
    with db_session() as db:
        db.execute(
            """
            INSERT INTO users (user_id, display_name, phone_number, username)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                display_name = excluded.display_name,
                phone_number = excluded.phone_number,
                username = excluded.username
            """,
            (user_id, display_name, phone_number, username),
        )
        return db.execute(
            "SELECT user_id, display_name, phone_number, username, address FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()


def get_registered_users_page(offset, limit=10):
    with db_session() as db:
        return db.execute(
            """
            SELECT user_id, display_name, phone_number, username
            FROM users
            ORDER BY registered_at DESC, user_id DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()


def count_registered_users():
    with db_session() as db:
        return db.execute("SELECT COUNT(*) FROM users").fetchone()[0]


def get_number_states(first_number, last_number):
    with db_session() as db:
        occupied_rows = db.execute(
            """
            SELECT number, COUNT(*) AS occupied
            FROM ticket_allocations
            WHERE CAST(number AS INTEGER) BETWEEN ? AND ?
            GROUP BY number
            """,
            (first_number, last_number),
        ).fetchall()
        blocked_rows = db.execute(
            """
            SELECT number FROM blocked_numbers
            WHERE CAST(number AS INTEGER) BETWEEN ? AND ?
            """,
            (first_number, last_number),
        ).fetchall()
    # Existing allocations remain stored as four-digit strings for backward compatibility.
    occupied = {str(int(row["number"])): row["occupied"] for row in occupied_rows}
    blocked = {str(int(row["number"])) for row in blocked_rows}
    states = {}
    for number in range(first_number, last_number + 1):
        key = str(number)
        stored_key = f"{number:04d}"
        used = occupied.get(key, 0)
        if key in blocked or used >= 2:
            states[key] = "taken"
        elif used == 1:
            states[key] = "half"
        else:
            states[key] = "available"
    return states


def get_number_allocations(number):
    number = canonical_number(number)
    with db_session() as db:
        return db.execute(
            """
            SELECT a.payment_id, a.user_id, COUNT(*) AS occupied_slots,
                   GROUP_CONCAT(a.half_slot) AS half_slots,
                   p.ticket_code, p.ticket_type, p.status,
                   u.display_name, u.phone_number
            FROM ticket_allocations a
            JOIN payments p ON p.id = a.payment_id
            LEFT JOIN users u ON u.user_id = a.user_id
            WHERE a.number = ?
            GROUP BY a.payment_id, a.user_id, p.ticket_code, p.ticket_type,
                     p.status, u.display_name, u.phone_number
            ORDER BY a.payment_id
            """,
            (number,),
        ).fetchall()


def canonical_number(number):
    value = str(number).strip()
    if not value.isascii() or not value.isdigit():
        raise ValueError("ትክክለኛ ቁጥር ያስገቡ።")
    return f"{int(value):04d}"


def is_number_blocked(number):
    number = canonical_number(number)
    with db_session() as db:
        return db.execute(
            "SELECT 1 FROM blocked_numbers WHERE number = ?", (number,)
        ).fetchone() is not None


def reserve_number(number, admin_id, reserved_name=""):
    number = canonical_number(number)
    settings = get_settings()
    if not (int(settings["first_number"]) <= int(number) <= int(settings["last_number"])):
        raise ValueError("ቁጥሩ በአሁኑ የዕጣ ክልል ውስጥ አይደለም።")
    db = connect_db()
    try:
        db.execute("BEGIN IMMEDIATE")
        occupied = db.execute(
            "SELECT 1 FROM ticket_allocations WHERE number = ? LIMIT 1", (number,)
        ).fetchone()
        blocked = db.execute(
            "SELECT 1 FROM blocked_numbers WHERE number = ?", (number,)
        ).fetchone()
        if occupied or blocked:
            raise NumberAlreadyTaken
        db.execute(
            "INSERT INTO blocked_numbers (number, created_by, reserved_name) VALUES (?, ?, ?)",
            (number, admin_id, (reserved_name or "").strip() or None),
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def release_admin_block(number):
    number = canonical_number(number)
    with db_session() as db:
        cursor = db.execute("DELETE FROM blocked_numbers WHERE number = ?", (number,))
        return cursor.rowcount > 0


def release_ticket_assignment(payment_id):
    db = connect_db()
    try:
        db.execute("BEGIN IMMEDIATE")
        payment = db.execute(
            """
            SELECT user_id, status FROM payments
            WHERE id = ? AND status = 'assigned'
            """,
            (payment_id,),
        ).fetchone()
        if payment is None:
            db.rollback()
            return None
        other_active = db.execute(
            """
            SELECT 1 FROM payments
            WHERE user_id = ? AND id != ? AND status IN ('pending', 'approved')
            LIMIT 1
            """,
            (payment["user_id"], payment_id),
        ).fetchone()
        if other_active:
            db.rollback()
            raise ValueError("ይህ ተጠቃሚ ሌላ የክፍያ ጥያቄ እየተካሄደ ነው።")
        allocations = db.execute(
            """
            SELECT number FROM ticket_allocations
            WHERE payment_id = ? ORDER BY number LIMIT 1
            """,
            (payment_id,),
        ).fetchone()
        if allocations is None:
            db.rollback()
            return None
        db.execute(
            "DELETE FROM ticket_allocations WHERE payment_id = ?", (payment_id,)
        )
        db.commit()
        return payment["user_id"], allocations["number"]
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def ticket_types(settings=None):
    settings = settings or get_settings()
    return {
        "full": {
            "label": settings["full_ticket_label"],
            "price": int(settings["full_ticket_price"]),
        },
        "half": {
            "label": settings["half_ticket_label"],
            "price": int(settings["half_ticket_price"]),
        },
    }


def format_price(price):
    return f"{price:,}"


def ticket_type_description(ticket_code, settings=None):
    item = ticket_types(settings)[ticket_code]
    return f"{item['label']} ({format_price(item['price'])} ብር)"


def bank_info_text(settings, ticket_code, quantity=1):
    ticket = ticket_type_description(ticket_code, settings)
    unit_price = ticket_types(settings)[ticket_code]['price']
    quantity = max(1, int(quantity or 1))
    return (
        f"💳 <b>{html.escape(ticket)} × {quantity}</b>\n"
        f"የአንዱ ዋጋ፦ {format_price(unit_price)} ብር\n"
        f"<b>ጠቅላላ ክፍያ፦ {format_price(unit_price * quantity)} ብር</b>\n\n"
        f"{html.escape(settings['payment_intro'])}\n\n"
        f"▪️ <b>የባንክ ስም፦</b> <code>{html.escape(settings['bank_name'])}</code>\n"
        f"▪️ <b>የሂሳብ ቁጥር፦</b> <code>{html.escape(settings['bank_account'])}</code>\n"
        f"▪️ <b>የሂሳብ ባለቤት፦</b> <code>{html.escape(settings['account_holder'])}</code>\n\n"
        f"{html.escape(settings['receipt_instruction'])}"
    )


def bank_info_keyboard(settings):
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📋 የሂሳብ ቁጥሩን ቅዳ",
                    copy_text=CopyTextButton(text=settings["bank_account"]),
                )
            ],
            [
                InlineKeyboardButton(
                    "📋 የባንክ ስም ቅዳ",
                    copy_text=CopyTextButton(text=settings["bank_name"]),
                ),
                InlineKeyboardButton(
                    "📋 የባለቤት ስም ቅዳ",
                    copy_text=CopyTextButton(text=settings["account_holder"]),
                ),
            ],
        ]
    )


def admin_settings_keyboard():
    rows = []
    for field, (label, _kind) in ADMIN_SETTING_FIELDS.items():
        rows.append(
            [InlineKeyboardButton(f"✏️ {label}", callback_data=f"admin:edit:{field}")]
        )
    rows.append(
        [
            InlineKeyboardButton("🔢 የቁጥር ሁኔታ", callback_data="manage:numbers:0"),
            InlineKeyboardButton("👥 ተጠቃሚዎች", callback_data="manage:users:0"),
        ]
    )
    rows.append([InlineKeyboardButton("🔄 አድስ", callback_data="admin:home")])
    return InlineKeyboardMarkup(rows)


def admin_settings_summary():
    settings = get_settings()
    full = ticket_types(settings)["full"]
    half = ticket_types(settings)["half"]
    return (
        "⚙️ <b>የአድሚን ቅንብሮች</b>\n\n"
        f"🏷️ ሎተሪ፦ {html.escape(settings['lottery_name'])}\n"
        f"🏦 ባንክ፦ {html.escape(settings['bank_name'])}\n"
        f"🔢 ሂሳብ፦ <code>{html.escape(settings['bank_account'])}</code>\n"
        f"👤 ባለቤት፦ {html.escape(settings['account_holder'])}\n"
        f"🎫 {html.escape(full['label'])}፦ {format_price(full['price'])} ብር\n"
        f"🎟️ {html.escape(half['label'])}፦ {format_price(half['price'])} ብር\n"
        f"🔢 ቁጥሮች፦ {int(settings['first_number'])}–"
        f"{int(settings['last_number'])}\n\n"
        "ለመቀየር ከታች አንዱን ይምረጡ።"
    )


def validate_admin_setting(key, raw_value):
    if key not in ADMIN_SETTING_FIELDS:
        raise ValueError("Unknown setting.")
    value = (raw_value or "").strip()
    if not value:
        raise ValueError("እባክዎ ባዶ ያልሆነ ዋጋ ያስገቡ።")

    kind = ADMIN_SETTING_FIELDS[key][1]
    if kind == "text":
        if key in {"full_ticket_label", "half_ticket_label"}:
            limit = 40
        elif key in {
            "welcome_instructions",
            "welcome_prompt",
            "payment_intro",
            "receipt_instruction",
        }:
            limit = 1000
        else:
            limit = 128
        if len(value) > limit:
            raise ValueError(f"ጽሑፉ ከ{limit} ቁምፊ መብለጥ የለበትም።")
        return value

    normalized = value.replace(",", "").replace(" ", "")
    if not normalized.isascii() or not normalized.isdigit():
        raise ValueError("እባክዎ ትክክለኛ ቁጥር ያስገቡ።")
    number = int(normalized)

    if kind == "price":
        if not 1 <= number <= 100_000_000:
            raise ValueError("ዋጋው ከ1 እስከ 100,000,000 ብር መሆን አለበት።")
        return str(number)

    if not 1 <= number <= 9999:
        raise ValueError("የዕጣ ቁጥር ከ0001 እስከ 9999 መሆን አለበት።")
    settings = get_settings()
    if key == "first_number":
        if number > int(settings["last_number"]):
            raise ValueError("የመጀመሪያ ቁጥሩ ከመጨረሻ ቁጥር መብለጥ የለበትም።")
        with db_session() as db:
            lowest = db.execute(
                """
                SELECT MIN(CAST(number AS INTEGER))
                FROM (
                    SELECT number FROM ticket_allocations
                    UNION ALL
                    SELECT number FROM blocked_numbers
                )
                """
            ).fetchone()[0]
        if lowest is not None and number > lowest:
            raise ValueError("የተመደቡ ወይም በአድሚን የተያዙ ቁጥሮችን የሚያካትት ክልል ይምረጡ።")
    else:
        if number < int(settings["first_number"]):
            raise ValueError("የመጨረሻ ቁጥሩ ከመጀመሪያ ቁጥር ያነሰ መሆን የለበትም።")
        with db_session() as db:
            highest = db.execute(
                """
                SELECT MAX(CAST(number AS INTEGER))
                FROM (
                    SELECT number FROM ticket_allocations
                    UNION ALL
                    SELECT number FROM blocked_numbers
                )
                """
            ).fetchone()[0]
        if highest is not None and number < highest:
            raise ValueError("የተመደቡ ወይም በአድሚን የተያዙ ቁጥሮችን የሚያካትት ክልል ይምረጡ።")
    return str(number)


def get_active_payment(user_id):
    with db_session() as db:
        return db.execute(
            """
            SELECT id, ticket_type, status, ticket_quantity
            FROM payments
            WHERE user_id = ? AND status IN ('pending', 'approved', 'assigned')
            ORDER BY id DESC LIMIT 1
            """,
            (user_id,),
        ).fetchone()


def get_assigned_ticket(user_id):
    with db_session() as db:
        return db.execute(
            """
            SELECT p.ticket_type, p.ticket_code, t.number
            FROM payments p
            JOIN ticket_allocations t ON t.payment_id = p.id
            WHERE p.user_id = ? AND p.status = 'assigned'
            GROUP BY p.id, p.ticket_type, p.ticket_code, t.number
            ORDER BY p.id DESC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()


def get_user_tickets(user_id):
    with db_session() as db:
        return db.execute(
            """
            SELECT p.id AS payment_id, p.ticket_type, p.ticket_code, a.number,
                   MIN(a.assigned_at) AS assigned_at
            FROM ticket_allocations a
            JOIN payments p ON p.id = a.payment_id
            WHERE a.user_id = ? AND p.status = 'assigned'
            GROUP BY p.id, p.ticket_type, p.ticket_code, a.number
            ORDER BY MIN(a.assigned_at) DESC, p.id DESC
            """,
            (user_id,),
        ).fetchall()


def get_payment_number(payment_id):
    with db_session() as db:
        row = db.execute(
            """
            SELECT number FROM ticket_allocations
            WHERE payment_id = ? ORDER BY number LIMIT 1
            """,
            (payment_id,),
        ).fetchone()
    return row["number"] if row else None


def get_in_progress_payment(user_id):
    with db_session() as db:
        return db.execute(
            """
            SELECT id, ticket_type, status, ticket_quantity
            FROM payments
            WHERE user_id = ? AND status IN ('pending', 'approved')
            ORDER BY id DESC LIMIT 1
            """,
            (user_id,),
        ).fetchone()


def get_assignable_payment(payment_id, user_id):
    """Return an approved or released payment belonging to this user."""
    with db_session() as db:
        payment = db.execute(
            """
            SELECT status, ticket_quantity FROM payments
            WHERE id = ? AND user_id = ? AND status IN ('approved', 'assigned')
            """,
            (payment_id, user_id),
        ).fetchone()
        if payment is None:
            return None
        if payment["status"] == "assigned":
            allocation = db.execute(
                "SELECT 1 FROM ticket_allocations WHERE payment_id = ? LIMIT 1",
                (payment_id,),
            ).fetchone()
            if allocation is not None:
                return None
        return payment


def create_payment(user_id, ticket_type, receipt_file_id, ticket_code=None, payment_method_id=None, quantity=1, total_amount=0):
    with db_session() as db:
        cursor = db.execute(
            """
            INSERT INTO payments
                (user_id, ticket_type, ticket_code, receipt_file_id, payment_method_id,
                 ticket_quantity, total_amount, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
            RETURNING id
            """,
            (user_id, ticket_type, ticket_code, receipt_file_id, payment_method_id,
             quantity, total_amount),
        )
        return cursor.fetchone()["id"]


def mark_payment_failed(payment_id):
    with db_session() as db:
        db.execute(
            "UPDATE payments SET status = 'failed' WHERE id = ? AND status = 'pending'",
            (payment_id,),
        )


def resolve_payment(payment_id, action):
    """Atomically approve/reject a pending payment. Returns its user ID or None."""
    new_status = "approved" if action == "approve" else "rejected"
    with db_session() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            """
            SELECT user_id FROM payments
            WHERE id = ? AND status = 'pending'
            """,
            (payment_id,),
        ).fetchone()
        if row is None:
            db.rollback()
            return None
        db.execute(
            "UPDATE payments SET status = ? WHERE id = ?",
            (new_status, payment_id),
        )
        db.commit()
        return row["user_id"]


class PaymentNotApproved(Exception):
    pass


class NumberAlreadyTaken(Exception):
    pass


class NoNumbersAvailable(Exception):
    pass


def assign_ticket(payment_id, user_id, requested_number=None):
    """Atomically assign one or both halves of a lottery number."""
    settings = get_settings()
    first_number = int(settings["first_number"])
    last_number = int(settings["last_number"])
    if requested_number is not None:
        if (
            not str(requested_number).isascii()
            or not str(requested_number).isdigit()
            or not 1 <= len(str(requested_number)) <= 4
            or not (first_number <= int(requested_number) <= last_number)
        ):
            raise ValueError(
                f"Choose a number from {first_number} to {last_number}."
            )
        requested_number = f"{int(requested_number):04d}"

    db = connect_db()
    try:
        db.execute("BEGIN IMMEDIATE")
        payment = db.execute(
            """
            SELECT status, ticket_code, ticket_type
            FROM payments WHERE id = ? AND user_id = ?
            """,
            (payment_id, user_id),
        ).fetchone()
        if payment is None:
            raise PaymentNotApproved

        ticket_code = infer_ticket_code(
            payment["ticket_code"], payment["ticket_type"], settings
        )
        if payment["status"] == "assigned":
            row = db.execute(
                """
                SELECT number FROM ticket_allocations
                WHERE payment_id = ? ORDER BY number LIMIT 1
                """,
                (payment_id,),
            ).fetchone()
            if row is not None:
                db.commit()
                return row["number"], True

        if payment["status"] not in {"approved", "assigned"}:
            raise PaymentNotApproved

        occupied_rows = db.execute(
            "SELECT number, half_slot FROM ticket_allocations"
        ).fetchall()
        occupied = {}
        for row in occupied_rows:
            occupied.setdefault(row["number"], set()).add(row["half_slot"])
        blocked = {
            row["number"]
            for row in db.execute("SELECT number FROM blocked_numbers").fetchall()
        }

        if requested_number is None:
            available = [
                f"{number:04d}"
                for number in range(first_number, last_number + 1)
                if f"{number:04d}" not in blocked
                and (
                    not occupied.get(f"{number:04d}")
                    if ticket_code == "full"
                    else len(occupied.get(f"{number:04d}", set())) < 2
                )
            ]
            if not available:
                raise NoNumbersAvailable
            number = secrets.choice(available)
        else:
            number = requested_number
            current_slots = occupied.get(number, set())
            if number in blocked or (
                ticket_code == "full" and current_slots
            ) or (
                ticket_code == "half" and len(current_slots) >= 2
            ):
                raise NumberAlreadyTaken

        used_slots = occupied.get(number, set())
        slots_to_assign = (
            (1, 2)
            if ticket_code == "full"
            else (1 if 1 not in used_slots else 2,)
        )
        for half_slot in slots_to_assign:
            db.execute(
                """
                INSERT INTO ticket_allocations
                    (number, half_slot, payment_id, user_id)
                VALUES (?, ?, ?, ?)
                """,
                (number, half_slot, payment_id, user_id),
            )
        db.execute(
            "UPDATE payments SET status = 'assigned', ticket_code = ? WHERE id = ?",
            (ticket_code, payment_id),
        )
        db.commit()
        return number, False
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def number_mode_keyboard(payment_id):
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🎲 እራሴ የዕድል ቁጥር ልምረጥ",
                    callback_data=f"mode:manual:{payment_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    "✨ የዕድል ቁጥሬን በራስ-ሰር ይምረጥ",
                    callback_data=f"mode:auto:{payment_id}",
                )
            ],
        ]
    )


def ticket_selection_keyboard(settings=None):
    types = ticket_types(settings)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"🎫 {types['full']['label']} ({format_price(types['full']['price'])} ብር)",
                    callback_data="ticket:full",
                )
            ],
            [
                InlineKeyboardButton(
                    f"🎟️ {types['half']['label']} ({format_price(types['half']['price'])} ብር)",
                    callback_data="ticket:half",
                )
            ],
            [InlineKeyboardButton("📋 የቁጥሮችን ሁኔታ እይ", callback_data="view_numbers")],
            [InlineKeyboardButton("🎟️ ብዙ (ከሁለት በላይ) ለመቁረጥ", callback_data="ticket:multi")],
        ]
    )


def multi_ticket_type_keyboard(settings=None):
    types = ticket_types(settings)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🎫 {types['full']['label']} ({format_price(types['full']['price'])} ብር/አንዱ)", callback_data="multi_type:full")],
        [InlineKeyboardButton(f"🎟️ {types['half']['label']} ({format_price(types['half']['price'])} ብር/አንዱ)", callback_data="multi_type:half")],
    ])


MY_TICKETS_BUTTON = "የኔ ትኬቶች"


def main_menu_keyboard(is_admin=False):
    rows = [[KeyboardButton("🎫 የኔ ትኬቶች"), KeyboardButton("🛒 ተጨማሪ ለመቁረጥ")],
            [KeyboardButton("🔢 የዕጣ ቁጥሮች ሁኔታ"), KeyboardButton("📅 መቼ ይወጣል?")],
            [KeyboardButton("🆘 እገዛ / Support")]]
    if is_admin:
        rows.extend([[KeyboardButton("⚙️ /admin"), KeyboardButton("🛠 /adminv3")],
                     [KeyboardButton("🔢 አድሚን ቁጥር ቁጥጥር"), KeyboardButton("👥 ተጠቃሚዎች")],
                     [KeyboardButton("📎 /tutorialupload"), KeyboardButton("❌ /cancel")],
                     [KeyboardButton("/start"), KeyboardButton("/Admin"), KeyboardButton("/Adminv3")]])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True)


def buy_ticket_keyboard():
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("ለመቁረጥ", callback_data="ticket:order")]]
    )


def repeat_confirmation_keyboard(ticket_code):
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("አዎ", callback_data=f"repeat:yes:{ticket_code}"),
            InlineKeyboardButton("አይ", callback_data=f"repeat:no:{ticket_code}"),
        ]]
    )


def registration_phone_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📱 ስልክ ቁጥሬን አጋራ", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def normalize_phone(value):
    normalized = re.sub(r"[\s().-]", "", value or "")
    if not re.fullmatch(r"\+?\d{7,15}", normalized):
        return None
    return normalized


def is_private_chat(update):
    chat = update.effective_chat
    return chat is not None and chat.type == "private"


async def require_private_chat(update):
    if is_private_chat(update):
        return True
    query = update.callback_query
    if query is not None:
        await query.answer(
            "For privacy, please use this bot in a private chat.",
            show_alert=True,
        )
    else:
        message = update.effective_message
        if message is not None:
            await message.reply_text(
                "For privacy, please open a private chat with this bot and send /start."
            )
    return False


async def notify_admin(context, text, parse_mode=None, reply_markup=None):
    try:
        await context.bot.send_message(
            chat_id=context.bot_data["admin_id"],
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
        )
    except TelegramError as exc:
        logger.warning("Could not send an admin notification (%s)", type(exc).__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_private_chat(update):
        return
    user = update.effective_user
    profile = get_registered_user(user.id)
    if profile is None:
        context.user_data.pop("registration_name", None)
        context.user_data["registration_step"] = "name"
        await update.message.reply_text("ሎተሪውን ለመቀጠል እባክዎ ሙሉ ስምዎን ያስገቡ።")
        return
    await show_lottery_menu(update, context, profile)


async def complete_registration(update, context, phone_number):
    user = update.effective_user
    display_name = context.user_data.get("registration_name", "").strip()
    profile = save_registered_user(
        user.id, display_name, phone_number, user.username
    )
    context.user_data.pop("registration_name", None)
    context.user_data.pop("registration_step", None)
    await notify_admin(
        context,
        "🆕 <b>አዲስ ተጠቃሚ ተመዝግቧል</b>\n\n"
        f"👤 ስም፦ {html.escape(profile['display_name'])}\n"
        f"📞 ስልክ፦ <code>{html.escape(profile['phone_number'])}</code>\n"
        f"🆔 Telegram ID፦ <code>{user.id}</code>",
        parse_mode="HTML",
    )
    await update.message.reply_text(
        "✅ ምዝገባዎ ተጠናቋል።",
        reply_markup=ReplyKeyboardRemove(),
    )
    await show_lottery_menu(update, context, profile)


async def registration_contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_private_chat(update):
        return
    if context.user_data.get("registration_step") != "phone":
        return
    contact = update.message.contact
    if contact.user_id != update.effective_user.id:
        await update.message.reply_text(
            "እባክዎ የራስዎን ስልክ ቁጥር በታች ያለውን ቁልፍ በመጫን ያጋሩ።",
            reply_markup=registration_phone_keyboard(),
        )
        return
    phone_number = normalize_phone(contact.phone_number)
    if phone_number is None:
        await update.message.reply_text(
            "ስልክ ቁጥሩ ትክክል አይደለም። እባክዎ ቁጥሩን በጽሑፍ ይላኩ።",
            reply_markup=ReplyKeyboardRemove(),
        )
        return
    await complete_registration(update, context, phone_number)


async def show_lottery_menu(update, context, profile):
    user = update.effective_user
    settings = get_settings()
    if user.id == context.bot_data["admin_id"]:
        await update.message.reply_text("የአድሚን ቋሚ ቁልፎች፦", reply_markup=main_menu_keyboard(is_admin=True))
    active = get_active_payment(user.id)

    if active:
        status = active["status"]
        if status == "pending":
            await update.message.reply_text(
                "⏳ የክፍያ ደረሰኝዎ በአድሚን እየተጣራ ነው። እባክዎ ውሳኔውን ይጠብቁ።"
            )
        elif status == "approved":
            await update.message.reply_text(
                "🎉 ክፍያዎ ጸድቋል። የዕድል ቁጥርዎን ይምረጡ፦",
                reply_markup=number_mode_keyboard(active["id"]),
            )
        else:
            with db_session() as db:
                tickets = db.execute(
                    """
                    SELECT DISTINCT number FROM ticket_allocations
                    WHERE payment_id = ? ORDER BY number
                    """,
                    (active["id"],),
                ).fetchall()
            if tickets:
                number_list = ', '.join(f"#{int(t['number'])}" for t in tickets)
                await update.message.reply_text(
                    f"✅ የተመዘገቡት የዕድል ቁጥሮች፦ {number_list}\n\n"
                    "ተጨማሪ ዕጣ ለመግዛት አዲስ የዕጣ አይነት ይምረጡ፦",
                    reply_markup=ticket_selection_keyboard(settings),
                )
            else:
                await update.message.reply_text(
                    "አድሚን የቁጥርዎን ምደባ አስወግዷል። እባክዎ አዲስ ቁጥር ይምረጡ፦",
                    reply_markup=number_mode_keyboard(active["id"]),
                )
        return

    context.user_data.pop("ticket_type", None)
    context.user_data.pop("ticket_code", None)
    welcome_text = (
        f"ሰላም <b>{html.escape(profile['display_name'])}</b>! "
        f"እንኳን ወደ <b>{html.escape(settings['lottery_name'])}</b> በደህና መጡ።\n\n"
        f"{html.escape(settings['welcome_instructions'])}\n\n"
        f"{html.escape(settings['welcome_prompt'])}"
    )
    if user.id == context.bot_data["admin_id"]:
        welcome_text += "\n\n⚙️ የአድሚን ቅንብሮች፦ /admin"
    await update.message.reply_text(
        welcome_text,
        parse_mode="HTML",
        reply_markup=ticket_selection_keyboard(settings),
    )
    await update.message.reply_text("ዋና ማውጫ፦", reply_markup=main_menu_keyboard(is_admin=(user.id == context.bot_data["admin_id"])))


async def ticket_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await require_private_chat(update):
        return
    await query.answer()

    if query.data == "ticket:multi":
        if get_registered_user(query.from_user.id) is None:
            await query.message.reply_text("እባክዎ መጀመሪያ /start በመጫን ይመዝገቡ።")
            return
        active_multi = get_active_payment(query.from_user.id)
        if active_multi and active_multi['status'] in {'pending', 'approved'}:
            await query.message.reply_text("⏳ አስቀድሞ የክፍያ ጥያቄ ወይም ያልተመደበ ክፍያ አለዎት። ያንን ሂደት መጀመሪያ ያጠናቅቁ።")
            return
        context.user_data['multi_ticket_step'] = 'quantity'
        context.user_data['ticket_quantity'] = 0
        await query.message.reply_text("ምን ያህል ትኬት ነው መቁረጥ የምትፈልገው? ከ2 እስከ 100 ያለ ቁጥር አስገባ።")
        return
    view_match = re.fullmatch(r"view_numbers(?::(\d+))?", query.data or "")
    if view_match:
        await show_numbers_and_auto_delete(query, context, page=int(view_match.group(1) or 0))
        return
    range_match = re.fullmatch(r"view_numbers_range:(\d+):(\d+)", query.data or "")
    if range_match:
        range_index, page = map(int, range_match.groups())
        await show_numbers_and_auto_delete(query, context, page=page, range_index=range_index)
        return
    page_match = re.fullmatch(r"view_numbers_page:(\d+):(\d+)", query.data or "")
    if page_match:
        range_index, page = map(int, page_match.groups())
        await show_numbers_and_auto_delete(query, context, page=page, range_index=range_index)
        return

    if get_registered_user(query.from_user.id) is None:
        await query.message.reply_text("እባክዎ መጀመሪያ /start በመጫን ይመዝገቡ።")
        return

    active = get_active_payment(query.from_user.id)
    if active and active["status"] == "pending":
        await query.message.reply_text(
            "⏳ የክፍያ ደረሰኝዎ በአድሚን እየተጣራ ነው። እባክዎ ውሳኔውን ይጠብቁ።"
        )
        return
    if active and active["status"] == "approved":
        await query.message.reply_text(
            "🎉 ክፍያዎ ጸድቋል። እባክዎ የዕድል ቁጥርዎን ይምረጡ፦",
            reply_markup=number_mode_keyboard(active["id"]),
        )
        return
    if active and active["status"] == "assigned" and get_payment_number(active["id"]) is None:
        await query.message.reply_text(
            "አድሚን የቁጥርዎን ምደባ አስወግዷል። አዲስ ቁጥር ይምረጡ፦",
            reply_markup=number_mode_keyboard(active["id"]),
        )
        return

    ticket_code = query.data.split(":", maxsplit=1)[1]
    assigned = get_assigned_ticket(query.from_user.id)
    if assigned:
        await query.message.reply_text(
            f"🎫 ቁጥር #{assigned['number']} ከዚህ በፊት ቆርጠዋል።\n"
            "ድጋሜ መቁረጥ ይፈልጋሉ?",
            reply_markup=repeat_confirmation_keyboard(ticket_code),
        )
        return

    settings = get_settings()
    context.user_data["ticket_quantity"] = 1
    context.user_data["ticket_code"] = ticket_code
    context.user_data["ticket_type"] = ticket_type_description(ticket_code, settings)
    await query.message.reply_text(f"ለእርስዎ <b>{html.escape(context.user_data['ticket_type'])}</b> መርጠዋል።",parse_mode="HTML")
    await prompt_payment_method(query.message, context, ticket_code)


async def repeat_selection_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await require_private_chat(update):
        return
    await query.answer()
    match = re.fullmatch(r"repeat:(yes|no):(full|half)", query.data or "")
    if not match:
        return
    answer, ticket_code = match.groups()
    if answer == "no":
        await query.message.reply_text("እናመሰግናለን መልካም ዕድል")
        return

    active_payment = get_active_payment(query.from_user.id)
    if (
        active_payment
        and active_payment["status"] == "assigned"
        and get_payment_number(active_payment["id"]) is None
    ):
        await query.message.reply_text(
            "አድሚን የቁጥርዎን ምደባ አስወግዷል። አዲስ ቁጥር ይምረጡ፦",
            reply_markup=number_mode_keyboard(active_payment["id"]),
        )
        return

    active = get_in_progress_payment(query.from_user.id)
    if active:
        if active["status"] == "pending":
            await query.message.reply_text(
                "⏳ የክፍያ ደረሰኝዎ በአድሚን እየተጣራ ነው። እባክዎ ውሳኔውን ይጠብቁ።"
            )
        else:
            await query.message.reply_text(
                "🎉 ክፍያዎ ጸድቋል። እባክዎ የዕድል ቁጥርዎን ይምረጡ፦",
                reply_markup=number_mode_keyboard(active["id"]),
            )
        return

    settings = get_settings()
    context.user_data["ticket_quantity"] = 1
    context.user_data["ticket_code"] = ticket_code
    context.user_data["ticket_type"] = ticket_type_description(ticket_code, settings)
    await query.message.reply_text("✅ አዲስ ዕጣ ለመቀጠል የክፍያ አማራጭ ይምረጡ።")
    await prompt_payment_method(query.message, context, ticket_code)


async def handle_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_private_chat(update):
        return
    user = update.effective_user
    profile = get_registered_user(user.id)
    if profile is None:
        await update.message.reply_text("እባክዎ መጀመሪያ /start በመጫን ይመዝገቡ።")
        return
    ticket_type = context.user_data.get("ticket_type")
    ticket_code = context.user_data.get("ticket_code")
    if not ticket_type or ticket_code not in {"full", "half"}:
        await update.message.reply_text(
            "እባክዎ መጀመሪያ /start በመጫን የዕጣ አይነት ይምረጡ።"
        )
        return

    active = get_in_progress_payment(user.id)
    if active:
        await update.message.reply_text(
            "አስቀድሞ የክፍያ ጥያቄ አለዎት። አድሚኑ እስኪመልስ ይጠብቁ።"
        )
        return

    try:
        payment_id = create_payment(
            user.id,
            ticket_type,
            update.message.photo[-1].file_id,
            ticket_code=ticket_code,
            payment_method_id=context.user_data.get("payment_method_id"),
            quantity=max(1, int(context.user_data.get("ticket_quantity", 1))),
            total_amount=(ticket_types(get_settings())[ticket_code]["price"] * max(1, int(context.user_data.get("ticket_quantity", 1)))),
        )
    except psycopg2.IntegrityError:
        await update.message.reply_text(
            "አስቀድሞ የክፍያ ጥያቄ አለዎት። አድሚኑ እስኪመልስ ይጠብቁ።"
        )
        return
    username = f"@{user.username}" if user.username else "የለም"
    caption = (
        "📥 <b>አዲስ ክፍያ ደረሰኝ</b>\n\n"
        f"👤 <b>ተጠቃሚ፦</b> {html.escape(profile['display_name'])} ({html.escape(username)})\n"
        f"📞 <b>ስልክ፦</b> <code>{html.escape(profile['phone_number'])}</code>\n"
        f"🆔 <b>ID፦</b> <code>{user.id}</code>\n"
        f"🎫 <b>አይነት፦</b> {html.escape(ticket_type)}\n"
        f"🔢 <b>ብዛት፦</b> {max(1, int(context.user_data.get('ticket_quantity', 1)))}\n"
        f"💰 <b>ጠቅላላ ክፍያ፦</b> {format_price(ticket_types(get_settings())[ticket_code]['price'] * max(1, int(context.user_data.get('ticket_quantity', 1))))} ብር\n"
        f"🔖 <b>ጥያቄ፦</b> <code>#{payment_id}</code>"
    )
    admin_keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ አጽድቅ", callback_data=f"approve:{payment_id}"
                ),
                InlineKeyboardButton(
                    "❌ ውድቅ አድርግ", callback_data=f"reject:{payment_id}"
                ),
            ]
        ]
    )

    try:
        await context.bot.send_photo(
            chat_id=context.bot_data["admin_id"],
            photo=update.message.photo[-1].file_id,
            caption=caption,
            parse_mode="HTML",
            reply_markup=admin_keyboard,
        )
    except TelegramError:
        logger.exception("Could not forward payment receipt to the administrator")
        mark_payment_failed(payment_id)
        await update.message.reply_text(
            "ደረሰኙን ለአድሚን መላክ አልተቻለም። አድሚኑ ቦቱን እንደጀመረ ያረጋግጡና እንደገና /start ይጫኑ።"
        )
        return

    context.user_data.pop("ticket_type", None)
    context.user_data.pop("ticket_code", None)
    context.user_data.pop("payment_method_id", None)
    context.user_data.pop("ticket_quantity", None)
    context.user_data.pop("multi_ticket_step", None)
    await update.message.reply_text(
        "⏳ የክፍያ ደረሰኝዎ ለአድሚን ተልኳል። ከጸደቀ በኋላ የዕድል ቁጥር መምረጫው ይከፈትልዎታል።"
    )


async def admin_approval_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await require_private_chat(update):
        return
    if query.from_user.id != context.bot_data["admin_id"]:
        await query.answer("ይህን ውሳኔ ማድረግ የሚችለው አድሚኑ ብቻ ነው።", show_alert=True)
        return

    match = re.fullmatch(r"(approve|reject):(\d+)", query.data or "")
    if not match:
        await query.answer()
        return

    action, payment_id_text = match.groups()
    payment_id = int(payment_id_text)
    user_id = resolve_payment(payment_id, action)
    if user_id is None:
        await query.answer("ይህ ጥያቄ ቀድሞ ተወስኗል ወይም አልተገኘም።", show_alert=True)
        return
    await query.answer()

    decision = "✅ <b>ጸድቋል</b>" if action == "approve" else "❌ <b>ውድቅ ተደርጓል</b>"
    try:
        await query.edit_message_caption(
            caption=f"{query.message.caption or ''}\n\n{decision}",
            parse_mode="HTML",
            reply_markup=None,
        )
    except TelegramError:
        logger.exception("Could not update the administrator's receipt message")

    if action == "approve":
        await context.bot.send_message(
            chat_id=user_id,
            text="🎉 <b>ክፍያዎ ጸድቋል!</b>\n\nየዕድል ቁጥርዎን እንዴት መምረጥ ይፈልጋሉ?",
            parse_mode="HTML",
            reply_markup=number_mode_keyboard(payment_id),
        )
    else:
        await context.bot.send_message(
            chat_id=user_id,
            text="❌ የክፍያ ደረሰኝዎ ውድቅ ተደርጓል። ደረሰኙን አረጋግጠው እንደገና /start ይጫኑ።",
        )


async def admin_settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_private_chat(update):
        return
    if update.effective_user.id != context.bot_data["admin_id"]:
        await update.message.reply_text("ይህን ቅንብር መቀየር የሚችለው አድሚኑ ብቻ ነው።")
        return
    context.user_data.pop("admin_setting_edit", None)
    context.user_data.pop("admin_message_user_id", None)
    context.user_data.pop("registration_step", None)
    context.user_data.pop("registration_name", None)
    await update.message.reply_text(
        admin_settings_summary(),
        parse_mode="HTML",
        reply_markup=admin_settings_keyboard(),
    )
    await update.message.reply_text("የአድሚን ቋሚ ማውጫ ከታች አለ።", reply_markup=main_menu_keyboard(is_admin=True))


async def admin_settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await require_private_chat(update):
        return
    if query.from_user.id != context.bot_data["admin_id"]:
        await query.answer("ይህን ቅንብር መቀየር የሚችለው አድሚኑ ብቻ ነው።", show_alert=True)
        return
    await query.answer()
    context.user_data.pop("admin_message_user_id", None)

    if query.data == "admin:home":
        context.user_data.pop("admin_setting_edit", None)
        await query.message.reply_text(
            admin_settings_summary(),
            parse_mode="HTML",
            reply_markup=admin_settings_keyboard(),
        )
        return
    if query.data == "admin:cancel":
        context.user_data.pop("admin_setting_edit", None)
        await query.message.reply_text("የቅንብር ማስተካከያው ተሰርዟል።")
        return

    match = re.fullmatch(r"admin:edit:([a-z_]+)", query.data or "")
    if not match or match.group(1) not in ADMIN_SETTING_FIELDS:
        return
    field = match.group(1)
    context.user_data.pop("admin_message_user_id", None)
    context.user_data["admin_setting_edit"] = field
    label, kind = ADMIN_SETTING_FIELDS[field]
    current = html.escape(get_settings()[field])
    extra = (
        " ቁጥር ብቻ ያስገቡ።"
        if kind in {"number", "price"}
        else " አዲሱን ጽሑፍ ይላኩ።"
    )
    await query.message.reply_text(
        f"✏️ <b>{html.escape(label)}</b>\n"
        f"ያለው ዋጋ፦\n<code>{current}</code>\n\n"
        f"{extra}\n/cancel በመጻፍ ማቋረጥ ይችላሉ።",
        parse_mode="HTML",
    )


async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_private_chat(update):
        return
    registration_step = context.user_data.get("registration_step")
    if registration_step == "name":
        display_name = " ".join((update.message.text or "").split())
        if not 2 <= len(display_name) <= 80:
            await update.message.reply_text("እባክዎ ከ2 እስከ 80 ቁምፊ ያለውን ሙሉ ስም ያስገቡ።")
            return
        context.user_data["registration_name"] = display_name
        context.user_data["registration_step"] = "phone"
        await update.message.reply_text(
            "አሁን ስልክ ቁጥርዎን ያጋሩ ወይም በጽሑፍ ይላኩ።",
            reply_markup=registration_phone_keyboard(),
        )
        return
    if registration_step == "phone":
        phone_number = normalize_phone(update.message.text)
        if phone_number is None:
            await update.message.reply_text(
                "የስልክ ቁጥሩ ትክክል አይደለም። የአገር ኮድ ጨምሮ 7–15 አሃዝ ያስገቡ።"
            )
            return
        await complete_registration(update, context, phone_number)
        return

    reserve_state = context.user_data.get("admin_reserve_number")
    if reserve_state:
        if update.effective_user.id != context.bot_data["admin_id"]:
            context.user_data.pop("admin_reserve_number", None)
            return
        reserved_name = " ".join((update.message.text or "").split())
        if len(reserved_name) < 2 or len(reserved_name) > 80:
            await update.message.reply_text("እባክዎ ከ2 እስከ 80 ፊደል ያለውን ስም ያስገቡ።")
            return
        number, page = reserve_state
        try:
            reserve_number(number, update.effective_user.id, reserved_name)
            await update.message.reply_text(f"✅ ቁጥር {int(number)} በ{html.escape(reserved_name)} ስም ተይዟል።", parse_mode="HTML")
            audit(update.effective_user.id, "reserve_number_named", f"{number}:{reserved_name}")
        except (NumberAlreadyTaken, ValueError) as exc:
            await update.message.reply_text(str(exc) if isinstance(exc, ValueError) else "ቁጥሩ ቀድሞ ተይዟል።")
        context.user_data.pop("admin_reserve_number", None)
        return

    field = context.user_data.get("admin_setting_edit")
    if field:
        if update.effective_user.id != context.bot_data["admin_id"]:
            context.user_data.pop("admin_setting_edit", None)
            return
        try:
            value = validate_admin_setting(field, update.message.text)
            update_setting(field, value)
        except ValueError as exc:
            await update.message.reply_text(str(exc))
            return
        context.user_data.pop("admin_setting_edit", None)
        await update.message.reply_text(
            "✅ ቅንብሩ ተቀይሯል።\n\n" + admin_settings_summary(),
            parse_mode="HTML",
            reply_markup=admin_settings_keyboard(),
        )
        return
    recipient_id = context.user_data.get("admin_message_user_id")
    if recipient_id is not None:
        if update.effective_user.id != context.bot_data["admin_id"]:
            context.user_data.pop("admin_message_user_id", None)
            return
        message_text = (update.message.text or "").strip()
        if not message_text or len(message_text) > 4096:
            await update.message.reply_text("እባክዎ ከ1 እስከ 4096 ቁምፊ ያለውን መልእክት ይላኩ።")
            return
        try:
            await context.bot.send_message(chat_id=recipient_id, text=message_text)
        except TelegramError as exc:
            logger.warning("Could not deliver an admin message (%s)", type(exc).__name__)
            await update.message.reply_text(
                "መልእክቱ አልተላከም። ተጠቃሚው ቦቱን እንደጀመረ ያረጋግጡ።"
            )
            context.user_data.pop("admin_message_user_id", None)
            return
        context.user_data.pop("admin_message_user_id", None)
        await update.message.reply_text("✅ መልእክቱ ተልኳል።")
        return
    if context.user_data.get('manual_multi_payment_id') is not None:
        payment_id = int(context.user_data['manual_multi_payment_id'])
        numbers = context.user_data.setdefault('manual_multi_numbers', [])
        raw = (update.message.text or '').strip()
        st = get_settings(); qty = 1
        with db_session() as db:
            row = db.execute("SELECT ticket_quantity FROM payments WHERE id=? AND user_id=?", (payment_id, update.effective_user.id)).fetchone()
        qty = int(row['ticket_quantity'] or 1) if row else 1
        if not raw.isascii() or not raw.isdigit() or not int(st['first_number']) <= int(raw) <= int(st['last_number']):
            await update.message.reply_text(f"ከ{st['first_number']} እስከ {st['last_number']} ያለ ቁጥር ያስገቡ።"); return
        n = int(raw)
        if n in numbers:
            await update.message.reply_text("ይህን ቁጥር አስቀድመው መርጠዋል። ሌላ ቁጥር ያስገቡ።"); return
        numbers.append(n)
        if len(numbers) < qty:
            await update.message.reply_text(f"{len(numbers)+1}ኛውን ቁጥር ያስገቡ። ({len(numbers)}/{qty} ተመርጧል)"); return
        await finish_multiple_selection(update.message.reply_text, payment_id, update.effective_user.id, numbers, context)
        return
    if await multi_ticket_text_handler(update, context): return
    if await v3_text_handler(update, context): return
    if await profile_edit_handler(update, context): return
    # An approved payment's manual number entry must be handled before menu fallback.
    if context.user_data.get("manual_payment_id") is not None:
        await manual_number_selection(update, context)
        return
    if await menu_text_handler(update, context): return
    # Ignore unrelated text safely and return the customer to the service menu.
    if update.message.text and get_registered_user(update.effective_user.id):
        await update.message.reply_text(feature_menu_text(), reply_markup=main_menu_keyboard(is_admin=(update.effective_user.id == context.bot_data.get("admin_id"))))
        return


async def edit_admin_view(query, text, reply_markup=None):
    try:
        return await query.edit_message_text(
            text=text,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )
    except TelegramError:
        return await query.message.reply_text(
            text=text,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )


def admin_number_page_keyboard(page, page_count, states):
    rows = []
    current_row = []
    icons = {"available": "🟢", "half": "🟡", "taken": "🔴"}
    for number, state in states.items():
        current_row.append(
            InlineKeyboardButton(
                f"{number} {icons[state]}",
                callback_data=f"manage:number:{number}:{page}",
            )
        )
        if len(current_row) == 5:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    nav = []
    if page > 0:
        nav.append(
            InlineKeyboardButton("⬅️ Previous", callback_data=f"manage:numbers:{page - 1}")
        )
    if page + 1 < page_count:
        nav.append(
            InlineKeyboardButton("Next ➡️", callback_data=f"manage:numbers:{page + 1}")
        )
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⚙️ ወደ አድሚን ምናሌ", callback_data="manage:home")])
    return InlineKeyboardMarkup(rows)


async def show_admin_number_page(query, page):
    settings = get_settings()
    first = int(settings["first_number"])
    last = int(settings["last_number"])
    page_size = 50
    page_count = max(1, (last - first + page_size) // page_size)
    page = max(0, min(page, page_count - 1))
    start = first + page * page_size
    end = min(last, start + page_size - 1)
    states = get_number_states(start, end)
    lines = [
        f"<code>{number}</code> {number_status_text(state)}"
        for number, state in states.items()
    ]
    text = (
        f"📊 <b>የቁጥሮች ሁኔታ ({start}–{end})</b>\n"
        f"ገጽ {page + 1}/{page_count}\n"
        "🟢 ክፍት · 🟡 ግማሽ አለ · 🔴 ተይዟል\n\n"
        + "\n".join(lines)
        + "\n\nቁጥር በመጫን ያስተዳድሩት።"
    )
    await edit_admin_view(
        query,
        text,
        admin_number_page_keyboard(page, page_count, states),
    )


def admin_users_keyboard(users, offset, total, page_size=10):
    rows = []
    for user in users:
        label = f"{user['display_name'][:24]} · {user['phone_number'][:18]}"
        rows.append(
            [
                InlineKeyboardButton(
                    label,
                    callback_data=f"manage:user:{user['user_id']}:{offset}",
                )
            ]
        )
    nav = []
    if offset > 0:
        nav.append(
            InlineKeyboardButton(
                "⬅️ Previous",
                callback_data=f"manage:users:{max(0, offset - page_size)}",
            )
        )
    if offset + len(users) < total:
        nav.append(
            InlineKeyboardButton(
                "Next ➡️",
                callback_data=f"manage:users:{offset + page_size}",
            )
        )
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⚙️ ወደ አድሚን ምናሌ", callback_data="manage:home")])
    return InlineKeyboardMarkup(rows)


async def show_admin_users_page(query, offset):
    page_size = 10
    total = count_registered_users()
    offset = max(0, min(offset, max(0, total - 1)))
    users = get_registered_users_page(offset, page_size)
    if not users:
        text = "👥 ገና የተመዘገበ ተጠቃሚ የለም።"
    else:
        text = (
            f"👥 <b>ተመዝጋቢዎች</b> · {offset + 1}–{offset + len(users)} / {total}\n\n"
            "ተጠቃሚን ይምረጡ።"
        )
    await edit_admin_view(
        query,
        text,
        admin_users_keyboard(users, offset, total, page_size),
    )


async def show_admin_number_detail(query, number, page):
    allocations = get_number_allocations(number)
    blocked = is_number_blocked(number)
    settings = get_settings()
    state = get_number_states(int(number), int(number))[str(int(number))]
    lines = [
        f"🔢 <b>ቁጥር {int(number)}</b> — {number_status_text(state)}",
        "",
    ]
    if blocked:
        with db_session() as db:
            reserved_row = db.execute("SELECT reserved_name FROM blocked_numbers WHERE number=?", (canonical_number(number),)).fetchone()
        reserved_name = (reserved_row["reserved_name"] if reserved_row else None)
        lines.append("⛔ በአድሚን ተይዟል።" + (f" · 👤 {html.escape(reserved_name)}" if reserved_name else ""))
    for allocation in allocations:
        code = infer_ticket_code(
            allocation["ticket_code"], allocation["ticket_type"], settings
        )
        type_label = "ሙሉ" if code == "full" else "ግማሽ"
        name = allocation["display_name"] or f"Telegram {allocation['user_id']}"
        phone = allocation["phone_number"] or "ስልክ አልተመዘገበም"
        lines.append(
            f"👤 {html.escape(name)} · {type_label} · "
            f"{allocation['occupied_slots']}/2 ግማሽ · "
            f"<code>{html.escape(phone)}</code>"
        )
    if not allocations and not blocked:
        lines.append("እስካሁን ማንም አልያዘውም።")

    rows = []
    if blocked:
        rows.append(
            [
                InlineKeyboardButton(
                    "🗑️ የአድሚን ይዞታን ይልቀቁ",
                    callback_data=f"manage:unblock:{number}:{page}",
                )
            ]
        )
    elif allocations:
        for allocation in allocations:
            name = (allocation["display_name"] or str(allocation["user_id"]))[:20]
            rows.append(
                [
                    InlineKeyboardButton(
                        f"🔓 {name} ቁጥሩን ይቀይር",
                        callback_data=(
                            f"manage:release_ticket:{allocation['payment_id']}:{page}"
                        ),
                    )
                ]
            )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    "📌 ቁጥሩን ያስይዙ",
                    callback_data=f"manage:reserve:{number}:{page}",
                )
            ]
        )
    rows.append(
        [InlineKeyboardButton("⬅️ ወደ ቁጥር ዝርዝር", callback_data=f"manage:numbers:{page}")]
    )
    await edit_admin_view(query, "\n".join(lines), InlineKeyboardMarkup(rows))


def admin_user_detail_keyboard(user_id, offset):
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✉️ መልእክት ላክ",
                    callback_data=f"manage:message:{user_id}:{offset}",
                )
            ],
            [InlineKeyboardButton("🗑️ ተጠቃሚውን ከቦቱ ሰርዝ", callback_data=f"manage:delete_user:{user_id}:{offset}")],
            [
                InlineKeyboardButton(
                    "⬅️ ወደ ተጠቃሚዎች",
                    callback_data=f"manage:users:{offset}",
                )
            ],
        ]
    )


async def admin_management_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await require_private_chat(update):
        return
    if query.from_user.id != context.bot_data["admin_id"]:
        await query.answer("ይህን ማስተዳደር የሚችለው አድሚኑ ብቻ ነው።", show_alert=True)
        return
    await query.answer()
    data = query.data or ""
    context.user_data.pop("admin_setting_edit", None)
    if not data.startswith("manage:message:"):
        context.user_data.pop("admin_message_user_id", None)

    if data == "manage:home":
        await edit_admin_view(
            query,
            admin_settings_summary(),
            admin_settings_keyboard(),
        )
        return

    match = re.fullmatch(r"manage:numbers:(\d+)", data)
    if match:
        await show_admin_number_page(query, int(match.group(1)))
        return

    match = re.fullmatch(r"manage:number:(\d{1,4}):(\d+)", data)
    if match:
        await show_admin_number_detail(query, match.group(1), int(match.group(2)))
        return

    match = re.fullmatch(r"manage:reserve:(\d{1,4}):(\d+)", data)
    if match:
        number, page = match.groups()
        await edit_admin_view(
            query,
            f"ቁጥር <b>{number}</b>ን እንደ ተይዘ ማስመዝገብ ይፈልጋሉ?",
            InlineKeyboardMarkup(
                [[
                    InlineKeyboardButton(
                        "✅ አዎ",
                        callback_data=f"manage:confirm_reserve:{number}:{page}",
                    ),
                    InlineKeyboardButton(
                        "አይ",
                        callback_data=f"manage:number:{number}:{page}",
                    ),
                ]]
            ),
        )
        return

    match = re.fullmatch(r"manage:confirm_reserve:(\d{1,4}):(\d+)", data)
    if match:
        number, page = match.groups()
        try:
            if is_number_blocked(number) or get_number_states(int(number), int(number)).get(str(int(number))) != "available":
                raise NumberAlreadyTaken
            context.user_data["admin_reserve_number"] = (number, int(page))
            await query.message.reply_text(f"ቁጥር {int(number)}ን በማን ስም ልያዝ? የተቆራጩን/የተያዘለትን ስም ይጻፉ።")
        except (NumberAlreadyTaken, ValueError):
            await query.message.reply_text("ቁጥሩ ቀድሞ ተይዟል።")
        return

    match = re.fullmatch(r"manage:unblock:(\d{1,4}):(\d+)", data)
    if match:
        number, page = match.groups()
        await edit_admin_view(
            query,
            f"በአድሚን የተያዘውን <b>{number}</b> ነጻ ማድረግ ይፈልጋሉ?",
            InlineKeyboardMarkup(
                [[
                    InlineKeyboardButton(
                        "✅ አዎ",
                        callback_data=f"manage:confirm_unblock:{number}:{page}",
                    ),
                    InlineKeyboardButton(
                        "አይ",
                        callback_data=f"manage:number:{number}:{page}",
                    ),
                ]]
            ),
        )
        return

    match = re.fullmatch(r"manage:confirm_unblock:(\d{1,4}):(\d+)", data)
    if match:
        number, page = match.groups()
        released = release_admin_block(number)
        await query.message.reply_text(
            f"✅ ቁጥር {number} ነጻ ሆኗል።" if released else "ይህ ቁጥር አስቀድሞ ነጻ ነው።"
        )
        await show_admin_number_page(query, int(page))
        return

    match = re.fullmatch(r"manage:release_ticket:(\d+):(\d+)", data)
    if match:
        payment_id, page = map(int, match.groups())
        with db_session() as db:
            assignment = db.execute(
                """
                SELECT a.number, a.user_id, u.display_name, p.ticket_code, p.ticket_type
                FROM ticket_allocations a
                JOIN payments p ON p.id = a.payment_id
                LEFT JOIN users u ON u.user_id = a.user_id
                WHERE a.payment_id = ?
                ORDER BY a.half_slot LIMIT 1
                """,
                (payment_id,),
            ).fetchone()
        if assignment is None:
            await query.message.reply_text("ይህ የቁጥር ምደባ አልተገኘም።")
            return
        name = assignment["display_name"] or f"Telegram {assignment['user_id']}"
        await edit_admin_view(
            query,
            f"የ<b>{html.escape(name)}</b> ቁጥር <b>{assignment['number']}</b>ን ማስወገድ?\n"
            "ክፍያው ይቆያል፤ ተጠቃሚው አዲስ ቁጥር እንዲመርጥ ይላክለታል።",
            InlineKeyboardMarkup(
                [[
                    InlineKeyboardButton(
                        "✅ አዎ፣ ቁጥሩን ይቀይር",
                        callback_data=f"manage:confirm_release:{payment_id}:{page}",
                    ),
                    InlineKeyboardButton(
                        "አይ",
                        callback_data=f"manage:number:{assignment['number']}:{page}",
                    ),
                ]]
            ),
        )
        return

    match = re.fullmatch(r"manage:confirm_release:(\d+):(\d+)", data)
    if match:
        payment_id, page = map(int, match.groups())
        try:
            released = release_ticket_assignment(payment_id)
        except ValueError as exc:
            await query.message.reply_text(str(exc))
            return
        if released is None:
            await query.message.reply_text("ይህ ቁጥር ቀድሞ ተቀይሯል ወይም አልተገኘም።")
            return
        user_id, number = released
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"ℹ️ አድሚኑ ቁጥር #{int(number)}ን ከምደባዎ አስወግዷል። "
                    "ክፍያዎ አልተሰረዘም፤ ከታች አዲስ ቁጥር ይምረጡ፦"
                ),
                reply_markup=number_mode_keyboard(payment_id),
            )
            notice = "✅ ቁጥሩ ተለቋል፤ ተጠቃሚው አዲስ ቁጥር እንዲመርጥ ተነግሮታል።"
        except TelegramError as exc:
            logger.warning("Could not notify user about ticket release (%s)", type(exc).__name__)
            notice = "ቁጥሩ ተለቋል፣ ግን መልእክቱ ለተጠቃሚው አልደረሰም።"
        await query.message.reply_text(notice)
        await show_admin_number_page(query, page)
        return

    match = re.fullmatch(r"manage:users:(\d+)", data)
    if match:
        await show_admin_users_page(query, int(match.group(1)))
        return

    match = re.fullmatch(r"manage:user:(\d+):(\d+)", data)
    if match:
        user_id, offset = map(int, match.groups())
        profile = get_registered_user(user_id)
        if profile is None:
            await query.message.reply_text("ይህ ተጠቃሚ አልተገኘም።")
            return
        text = (
            "👤 <b>የተጠቃሚ መረጃ</b>\n\n"
            f"ስም፦ {html.escape(profile['display_name'])}\n"
            f"ስልክ፦ <code>{html.escape(profile['phone_number'])}</code>\n"
            f"Telegram ID፦ <code>{user_id}</code>\n"
            f"Username፦ {html.escape('@' + profile['username'] if profile['username'] else 'የለም')}"
        )
        await edit_admin_view(
            query, text, admin_user_detail_keyboard(user_id, offset)
        )
        return

    match = re.fullmatch(r"manage:delete_user:(\d+):(\d+)", data)
    if match:
        user_id, offset = map(int, match.groups())
        if user_id == context.bot_data["admin_id"]:
            await query.message.reply_text("የአድሚን መለያ ከዚህ ሊሰረዝ አይችልም።")
            return
        await edit_admin_view(query, f"Telegram ID <code>{user_id}</code> የተጠቃሚ መለያን ከቦቱ መሰረዝ ይፈልጋሉ?\nየክፍያና የትኬት ታሪክ ለሂሳብ መዝገብ ይቀመጣል።", InlineKeyboardMarkup([[InlineKeyboardButton("⚠️ አዎ፣ ሰርዝ", callback_data=f"manage:confirm_delete_user:{user_id}:{offset}"), InlineKeyboardButton("ተወው", callback_data=f"manage:user:{user_id}:{offset}")]]))
        return

    match = re.fullmatch(r"manage:confirm_delete_user:(\d+):(\d+)", data)
    if match:
        user_id, offset = map(int, match.groups())
        if user_id == context.bot_data["admin_id"]:
            await query.message.reply_text("የአድሚን መለያ ከዚህ ሊሰረዝ አይችልም።")
            return
        with db_session() as db:
            result = db.execute("DELETE FROM users WHERE user_id=?", (user_id,))
        audit(query.from_user.id, "delete_user_profile", str(user_id))
        await query.message.reply_text("✅ የተጠቃሚው የቦት መገለጫ ተሰርዟል። የክፍያ/ትኬት ታሪኩ ግን ተጠብቋል።")
        await show_admin_users_page(query, offset)
        return

    match = re.fullmatch(r"manage:message:(\d+):(\d+)", data)
    if match:
        user_id, _offset = map(int, match.groups())
        if get_registered_user(user_id) is None:
            await query.message.reply_text("ይህ ተጠቃሚ አልተገኘም።")
            return
        context.user_data.pop("admin_setting_edit", None)
        context.user_data["admin_message_user_id"] = user_id
        await query.message.reply_text(
            f"ለ Telegram <code>{user_id}</code> የሚልኩትን መልእክት ይጻፉ።\n"
            "/cancel በመጻፍ ማቋረጥ ይችላሉ።",
            parse_mode="HTML",
        )


def assign_multiple_tickets(payment_id, user_id, requested_numbers=None):
    settings = get_settings()
    first, last = int(settings['first_number']), int(settings['last_number'])
    db = connect_db()
    try:
        db.execute("BEGIN IMMEDIATE")
        payment = db.execute("SELECT status,ticket_code,ticket_type,ticket_quantity FROM payments WHERE id=? AND user_id=?", (payment_id,user_id)).fetchone()
        if not payment or payment['status'] not in {'approved','assigned'}:
            raise PaymentNotApproved
        if payment['status'] == 'assigned':
            rows = db.execute("SELECT DISTINCT number FROM ticket_allocations WHERE payment_id=? ORDER BY number", (payment_id,)).fetchall()
            db.commit()
            return [str(int(r['number'])) for r in rows], True
        qty = max(1, int(payment['ticket_quantity'] or 1))
        code = infer_ticket_code(payment['ticket_code'], payment['ticket_type'], settings)
        occupied_rows = db.execute("SELECT number,half_slot FROM ticket_allocations").fetchall()
        occupied = {}
        for row in occupied_rows: occupied.setdefault(row['number'], set()).add(int(row['half_slot']))
        blocked = {r['number'] for r in db.execute("SELECT number FROM blocked_numbers").fetchall()}
        if requested_numbers is None:
            pool=[]
            for n in range(first,last+1):
                key=f'{n:04d}'; slots=occupied.get(key,set())
                if key in blocked: continue
                if code == 'full' and not slots: pool.append(n)
                elif code == 'half' and len(slots)<2: pool.append(n)
            if len(pool)<qty: raise NoNumbersAvailable
            selected=secrets.SystemRandom().sample(pool,qty)
        else:
            selected=[int(x) for x in requested_numbers]
            if len(selected)!=qty or len(set(selected))!=qty or any(n<first or n>last for n in selected):
                raise ValueError('ቁጥሮቹ ትክክል አይደሉም።')
        for n in selected:
            key=f'{n:04d}'; slots=occupied.get(key,set())
            if key in blocked or (code=='full' and slots) or (code=='half' and len(slots)>=2):
                raise NumberAlreadyTaken
            slots_to_use=(1,2) if code=='full' else ((1,) if 1 not in slots else (2,))
            for slot in slots_to_use:
                db.execute("INSERT INTO ticket_allocations(number,half_slot,payment_id,user_id) VALUES(?,?,?,?)",(key,slot,payment_id,user_id))
            occupied[key]=slots.union(slots_to_use)
        db.execute("UPDATE payments SET status='assigned',ticket_code=? WHERE id=?",(code,payment_id))
        db.commit()
        return selected, False
    except Exception:
        db.rollback(); raise
    finally: db.close()


async def finish_multiple_selection(send_reply, payment_id, user_id, numbers, context):
    try:
        assigned, existed = assign_multiple_tickets(payment_id,user_id,numbers)
    except (PaymentNotApproved, NumberAlreadyTaken, NoNumbersAvailable, ValueError) as exc:
        if isinstance(exc, PaymentNotApproved): msg='ክፍያው አልጸደቀም።'
        elif isinstance(exc, NumberAlreadyTaken): msg='ከተመረጡት ቁጥሮች አንዱ ተይዟል። እባክዎ እንደገና ይምረጡ።'
        elif isinstance(exc, NoNumbersAvailable): msg='በቂ ክፍት ቁጥሮች የሉም።'
        else: msg=str(exc)
        await send_reply(msg)
        if context.user_data.get('manual_multi_payment_id') == payment_id:
            context.user_data['manual_multi_numbers'] = []
            await send_reply("እባክዎ 1ኛውን ቁጥር እንደገና ያስገቡ።")
        return False
    numbers_text=', '.join(f'#{int(n)}' for n in assigned)
    await send_reply(f"🎉 እንኳን ደስ አለዎት!\nየተመደቡልዎት የዕድል ቁጥሮች፦\n<b>{numbers_text}</b>",parse_mode='HTML')
    context.user_data.pop('manual_multi_payment_id',None); context.user_data.pop('manual_multi_numbers',None)
    return True


async def multi_ticket_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not await require_private_chat(update): return
    await q.answer()
    code = (q.data or '').split(':')[-1]
    quantity = int(context.user_data.get('ticket_quantity', 0))
    if code not in {'full', 'half'} or quantity < 2:
        await q.message.reply_text("እባክዎ የትኬት ብዛትን እንደገና ያስገቡ።")
        return
    settings = get_settings()
    context.user_data['ticket_code'] = code
    context.user_data['ticket_type'] = ticket_type_description(code, settings)
    context.user_data['multi_ticket_step'] = None
    total = ticket_types(settings)[code]['price'] * quantity
    await q.message.reply_text(
        f"የመረጡት፦ {html.escape(context.user_data['ticket_type'])} × {quantity}\n"
        f"ጠቅላላ ክፍያ፦ <b>{format_price(total)} ብር</b>\n\nየክፍያ አማራጭ ይምረጡ፦",
        parse_mode='HTML')
    await prompt_payment_method(q.message, context, code)


async def multi_ticket_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    step = context.user_data.get('multi_ticket_step')
    if step != 'quantity': return False
    raw = (update.message.text or '').strip()
    if not raw.isascii() or not raw.isdigit() or not 2 <= int(raw) <= 100:
        await update.message.reply_text("ከ2 እስከ 100 ያለ ትክክለኛ የትኬት ብዛት ያስገቡ።")
        return True
    context.user_data['ticket_quantity'] = int(raw)
    context.user_data['multi_ticket_step'] = 'type'
    await update.message.reply_text("ሙሉ ነው ወይስ በየግማሽ? የትኬት አይነት ይምረጡ፦", reply_markup=multi_ticket_type_keyboard(get_settings()))
    return True


async def number_mode_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await require_private_chat(update):
        return
    await query.answer()
    if get_registered_user(query.from_user.id) is None:
        await query.message.reply_text("እባክዎ መጀመሪያ /start በመጫን ይመዝገቡ።")
        return
    match = re.fullmatch(r"mode:(auto|manual):(\d+)", query.data or "")
    if not match:
        return
    mode, payment_id_text = match.groups()
    payment_id = int(payment_id_text)
    payment = get_assignable_payment(payment_id, query.from_user.id)
    if payment is None:
        await query.message.reply_text(
            "ይህ ክፍያ ለእርስዎ አልተፈቀደም ወይም ቁጥሩ ቀድሞ ተመድቧል።"
        )
        return

    quantity = int(payment['ticket_quantity'] or 1)
    if quantity > 1 and mode == "manual":
        context.user_data['manual_multi_payment_id'] = payment_id
        context.user_data['manual_multi_numbers'] = []
        await show_numbers_and_auto_delete(query, context, page=0, range_index=0)
        await query.message.reply_text(f"✏️ 1ኛውን ቁጥር ከ{get_settings()['first_number']} እስከ {get_settings()['last_number']} ያስገቡ። በተራ ይጠይቃል።")
        return
    if quantity > 1 and mode == "auto":
        await finish_multiple_selection(query.message.reply_text, payment_id, query.from_user.id, None, context)
        return
    if mode == "manual":
        context.user_data["manual_payment_id"] = payment_id
        await show_numbers_and_auto_delete(query, context, page=0, range_index=0)
        settings = get_settings()
        await query.message.reply_text(
            f"✏️ ከ <b>{int(settings['first_number'])} እስከ "
            f"{int(settings['last_number'])}</b> ያለ ቁጥር ይጻፉ።\n"
            "ምሳሌ፦ 7 ወይም 100 ወይም 452",
            parse_mode="HTML",
        )
        return

    await finish_ticket_selection(
        query.message.reply_text,
        payment_id,
        query.from_user.id,
        requested_number=None,
        context=context,
    )


async def finish_ticket_selection(
    send_reply, payment_id, user_id, requested_number, context
):
    try:
        number, already_assigned = assign_ticket(
            payment_id, user_id, requested_number=requested_number
        )
    except PaymentNotApproved:
        await send_reply("ክፍያው አልጸደቀም ወይም ጥያቄው አልተገኘም።")
        return
    except NumberAlreadyTaken:
        state = get_number_states(int(requested_number), int(requested_number)).get(
            requested_number, "taken"
        )
        with db_session() as db:
            payment = db.execute(
                "SELECT ticket_code, ticket_type FROM payments WHERE id = ?",
                (payment_id,),
            ).fetchone()
        code = infer_ticket_code(
            payment["ticket_code"], payment["ticket_type"], get_settings()
        ) if payment else "full"
        if state == "half" and code == "full":
            await send_reply(
                f"🔒 #{requested_number} ላይ አንድ ግማሽ ተይዟል፤ ለሙሉ ዕጣ ሌላ ቁጥር ይምረጡ።"
            )
            return
        await send_reply(
            f"🔒 #{requested_number} ቀድሞ ተይዟል። ሌላ ቁጥር ይምረጡ።"
        )
        return
    except NoNumbersAvailable:
        await send_reply("ይቅርታ፣ ሁሉም የዕጣ ቁጥሮች ተይዘዋል።")
        return
    except ValueError:
        settings = get_settings()
        await send_reply(
            f"ከ {int(settings['first_number'])} እስከ "
            f"{int(settings['last_number'])} ያለ ትክክለኛ ቁጥር ይጻፉ።"
        )
        return

    prefix = "የተመዘገበው" if already_assigned else "የመረጡት"
    await send_reply(
        f"🎉 እንኳን ደስ አለዎት!\n\n🎫 {prefix} የዕድል ቁጥርዎ፦ <b>#{number}</b>\n\n"
        "ቁጥርዎ በውድድሩ ላይ ተመዝግቧል።",
        parse_mode="HTML",
    )
    if not already_assigned:
        profile = get_registered_user(user_id)
        with db_session() as db:
            payment = db.execute(
                "SELECT ticket_code, ticket_type FROM payments WHERE id = ?",
                (payment_id,),
            ).fetchone()
        if profile and payment:
            ticket_code = infer_ticket_code(
                payment["ticket_code"], payment["ticket_type"], get_settings()
            )
            await notify_admin(
                context,
                "🎟️ <b>ተጠቃሚው ቁጥር መርጧል</b>\n\n"
                f"👤 ስም፦ {html.escape(profile['display_name'])}\n"
                f"📞 ስልክ፦ <code>{html.escape(profile['phone_number'])}</code>\n"
                f"🆔 Telegram ID፦ <code>{user_id}</code>\n"
                f"🎫 አይነት፦ {'ሙሉ' if ticket_code == 'full' else 'ግማሽ'}\n"
                f"🔢 ቁጥር፦ <code>{number}</code>",
                parse_mode="HTML",
            )


async def manual_number_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    payment_id = context.user_data.get("manual_payment_id")
    if payment_id is None:
        return
    text = (update.message.text or "").strip()
    await finish_ticket_selection(
        update.message.reply_text,
        payment_id,
        update.effective_user.id,
        requested_number=text,
        context=context,
    )
    with db_session() as db:
        row = db.execute(
            "SELECT status FROM payments WHERE id = ? AND user_id = ?",
            (payment_id, update.effective_user.id),
        ).fetchone()
    if row is None or row["status"] == "assigned":
        context.user_data.pop("manual_payment_id", None)


def number_status_text(state):
    return {
        "available": "🟢",
        "half": "🟡 ግማሽ አለ",
        "taken": "🔴 ተይዟል",
    }[state]


def public_numbers_keyboard(page, page_count, range_index=0):
    rows = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"view_numbers_page:{range_index}:{page - 1}"))
    if page + 1 < page_count:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"view_numbers_page:{range_index}:{page + 1}"))
    if nav:
        rows.append(nav)
    ranges = [(1,1000),(1001,2000),(2001,3000),(3001,4000),(4001,5000),(5001,6000)]
    for start in (0, 3):
        rows.append([InlineKeyboardButton(f"{ranges[i][0]}-{ranges[i][1]}", callback_data=f"view_numbers_range:{i}:0") for i in range(start, start + 3)])
    return InlineKeyboardMarkup(rows)


async def show_numbers_and_auto_delete(query, context, page=0, range_index=0):
    settings = get_settings()
    configured_first = int(settings["first_number"])
    configured_last = int(settings["last_number"])
    ranges = [(1,1000),(1001,2000),(2001,3000),(3001,4000),(4001,5000),(5001,6000)]
    range_index = max(0, min(int(range_index), 5))
    range_first, range_last = ranges[range_index]
    first_number = max(configured_first, range_first)
    last_number = min(configured_last, range_last)
    if first_number > last_number:
        first_number, last_number = configured_first, configured_last
    page_size = 100
    page_count = max(1, (last_number - first_number + page_size) // page_size)
    page = max(0, min(int(page), page_count - 1))
    page_start = first_number + page * page_size
    page_end = min(last_number, page_start + page_size - 1)
    states = get_number_states(page_start, page_end)
    with db_session() as db:
        sold_rows = db.execute("""
            SELECT a.number, STRING_AGG(DISTINCT u.display_name, ', ' ORDER BY u.display_name) AS names
            FROM ticket_allocations a LEFT JOIN users u ON u.user_id=a.user_id
            WHERE CAST(a.number AS INTEGER) BETWEEN ? AND ?
            GROUP BY a.number
        """, (page_start, page_end)).fetchall()
        reserved_rows = db.execute("""
            SELECT number, reserved_name FROM blocked_numbers
            WHERE CAST(number AS INTEGER) BETWEEN ? AND ?
        """, (page_start, page_end)).fetchall()
    names = {str(int(r["number"])): (r["names"] or "") for r in sold_rows}
    reserved = {str(int(r["number"])): (r["reserved_name"] or "") for r in reserved_rows}
    lines = []
    compact_status = {"available": "🟢", "half": "🟡", "taken": "🔴"}
    for number, state in states.items():
        line = f"{number} {compact_status[state]}"
        name = names.get(str(number), "") or reserved.get(str(number), "")
        if name:
            # Keep 100 entries safely below Telegram's 4096-character message limit.
            short_name = name[:18] + ("…" if len(name) > 18 else "")
            line += f" · {html.escape(short_name)}"
        lines.append(line)
    # Count numbers sold/remaining from actual allocation state, not admin-entered text.
    sold = sum(1 for v in states.values() if v in ("half", "taken"))
    remaining = len(states) - sold
    text = (
        f"📋 <b>የቁጥሮች ዝርዝር ({page_start}–{page_end})</b> · {page+1}/{page_count}\n"
        f"🟢 ክፍት · 🟡 ግማሽ አለ · 🔴 ተይዟል\n"
        f"በዚህ ገጽ የተሸጡ/የተያዙ፦ {sold} · የቀሩ፦ {remaining}\n\n"
        + "\n".join(lines)
    )
    markup = public_numbers_keyboard(page, page_count, range_index)
    if query.data and (query.data.startswith("view_numbers:") or query.data.startswith("view_numbers_range:") or query.data.startswith("view_numbers_page:")):
        message = await query.edit_message_text(text=text, parse_mode="HTML", reply_markup=markup)
    else:
        message = await query.message.reply_text(text=text, parse_mode="HTML", reply_markup=markup)
        async def delete_later(chat_id, message_id):
            await asyncio.sleep(180)
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            except TelegramError:
                logger.info("The temporary number-status message could not be deleted")
        context.application.create_task(delete_later(message.chat_id, message.message_id))


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_private_chat(update):
        return
    context.user_data.pop("ticket_type", None)
    context.user_data.pop("ticket_code", None)
    context.user_data.pop("manual_payment_id", None)
    context.user_data.pop("manual_multi_payment_id", None)
    context.user_data.pop("manual_multi_numbers", None)
    context.user_data.pop("multi_ticket_step", None)
    context.user_data.pop("ticket_quantity", None)
    context.user_data.pop("admin_reserve_number", None)
    context.user_data.pop("admin_setting_edit", None)
    context.user_data.pop("admin_message_user_id", None)
    context.user_data.pop("registration_step", None)
    context.user_data.pop("registration_name", None)
    context.user_data.pop("admin_action", None)
    await update.message.reply_text(
        "ሂደቱ ተቋርጧል። ደረሰኝ አስቀድመው ከላኩ የአድሚን ግምገማው ይቀጥላል።",
        reply_markup=main_menu_keyboard(is_admin=(update.effective_user.id == context.bot_data.get("admin_id"))),
    )



# ------------------------- Sheger Lottery v3 features -------------------------
def feature_menu_text():
    return "እባኮትን የሚፈልጉትን አገልግሎት ይምረጡ።"

def list_payment_methods():
    with db_session() as db:
        return db.execute("SELECT * FROM payment_methods WHERE active=TRUE ORDER BY id").fetchall()

def audit(admin_id, action, details=""):
    with db_session() as db:
        db.execute("INSERT INTO admin_audit_log(admin_id,action,details) VALUES(?,?,?)", (admin_id, action, details[:2000]))

async def show_user_tickets(update, context):
    uid=update.effective_user.id
    with db_session() as db:
        rows=db.execute("""SELECT p.id,p.ticket_type,p.status,p.created_at,
          STRING_AGG(DISTINCT ta.number, ', ') AS numbers
          FROM payments p LEFT JOIN ticket_allocations ta ON ta.payment_id=p.id
          WHERE p.user_id=? GROUP BY p.id ORDER BY p.id DESC LIMIT 30""",(uid,)).fetchall()
    if not rows:
        await update.message.reply_text("ገና የገዙት ትኬት የለም።",reply_markup=main_menu_keyboard(is_admin=(update.effective_user.id == context.bot_data.get("admin_id")))); return
    lines=["🎫 <b>የእኔ የግዢ ታሪክ</b>"]
    for r in rows:
        lines.append(f"\n#{r['id']} | {html.escape(r['ticket_type'])}\nቁጥር፦ {html.escape(r['numbers'] or 'ገና አልተመደበም')}\nሁኔታ፦ {html.escape(r['status'])}\nቀን፦ {html.escape(str(r['created_at']))}")
    await update.message.reply_text("\n".join(lines),parse_mode="HTML",reply_markup=main_menu_keyboard(is_admin=(update.effective_user.id == context.bot_data.get("admin_id"))))

async def show_public_number_report(update, context):
    st=get_settings()
    with db_session() as db:
        agg=db.execute("""SELECT COUNT(DISTINCT number) AS sold_numbers, COUNT(*) AS sold_slots
          FROM ticket_allocations""").fetchone()
        paid=db.execute("""SELECT COALESCE(SUM(CASE WHEN ticket_code='half' THEN CAST(? AS NUMERIC) ELSE CAST(? AS NUMERIC) END),0) AS revenue
          FROM payments WHERE status='assigned'""",(st['half_ticket_price'],st['full_ticket_price'])).fetchone()
    first,last=int(st['first_number']),int(st['last_number'])
    total=last-first+1
    sold=int(agg['sold_numbers'] or 0)
    # Admin-written report fields are shown separately; computed allocation counts are not hand-entered.
    manual=(st.get('public_sales_report') or '').strip()
    # User explicitly chose a manual report: figures and collection amounts are admin-entered.
    remaining=max(0,total-sold)
    msg=("📊 <b>የሽያጭ ሪፖርት</b>\n\n"+
         f"በአጠቃላይ የቁጥር መጠን፦ {total}\n"
         f"በሽያጭ/ምደባ የተያዙ፦ {sold}\n"
         f"የቀሩ ቁጥሮች፦ {remaining}\n\n"+
         (html.escape(manual) if manual else "የአድሚን ተጨማሪ ሪፖርት ገና አልተሞላም።"))
    await update.message.reply_text(msg,parse_mode="HTML",reply_markup=main_menu_keyboard(is_admin=(update.effective_user.id == context.bot_data.get("admin_id"))))
    # The detailed 100-number list is a separate temporary message, removed after 3 minutes.
    class _PublicQuery:
        data = "view_numbers"
        message = update.message
    await show_numbers_and_auto_delete(_PublicQuery(), context, page=0, range_index=0)

async def menu_text_handler(update, context):
    txt=(update.message.text or '').strip()
    uid=update.effective_user.id
    if uid == context.bot_data.get('admin_id'):
        if txt in {"⚙️ /admin", "/Admin", "/admin"}:
            await admin_settings_command(update, context); return True
        if txt in {"🛠 /adminv3", "/Adminv3", "/adminv3"}:
            await admin_v3_command(update, context); return True
        if txt == "🔢 አድሚን ቁጥር ቁጥጥር":
            await update.message.reply_text("የቁጥር ዝርዝር፦", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("የቁጥር ሁኔታ", callback_data="manage:numbers:0")]])); return True
        if txt == "👥 ተጠቃሚዎች":
            await update.message.reply_text("ተጠቃሚዎች፦", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("የተጠቃሚዎች ዝርዝር", callback_data="manage:users:0")]])); return True
        if txt == "📎 /tutorialupload":
            await tutorial_upload_command(update, context); return True
        if txt == "❌ /cancel":
            await cancel(update, context); return True
    if txt in {"🎫 የኔ ትኬቶች", MY_TICKETS_BUTTON}:
        await show_user_tickets(update,context); return True
    if txt=="🔢 የዕጣ ቁጥሮች ሁኔታ":
        await show_public_number_report(update,context); return True
    if txt=="📅 መቼ ይወጣል?":
        st=get_settings(); await update.message.reply_text(f"📅 <b>የዕጣ ማውጫ መረጃ</b>\n{html.escape(st.get('draw_datetime','Not announced'))}\n{html.escape(st.get('draw_status',''))}",parse_mode="HTML",reply_markup=main_menu_keyboard(is_admin=(update.effective_user.id == context.bot_data.get("admin_id")))); return True
    if txt=="🆘 እገዛ / Support":
        st=get_settings(); await update.message.reply_text(st.get('support_text') or "የSupport መረጃ ገና አልተዘጋጀም።",reply_markup=main_menu_keyboard(is_admin=(update.effective_user.id == context.bot_data.get("admin_id")))); return True
    if txt in {"🛒 ተጨማሪ ለመቁረጥ","አዲስ ትኬት ለመቁረጥ"}:
        await update.message.reply_text("የዕጣ አይነት ይምረጡ፦",reply_markup=ticket_selection_keyboard(get_settings())); return True
    return False

async def admin_v3_command(update, context):
    if update.effective_user.id!=context.bot_data['admin_id']:
        await update.message.reply_text("የአድሚን ፈቃድ ያስፈልጋል።"); return
    kb=InlineKeyboardMarkup([
      [InlineKeyboardButton("➕ የክፍያ አማራጭ ጨምር",callback_data="v3:addmethod")],
      [InlineKeyboardButton("📋 የክፍያ አማራጮች",callback_data="v3:methods")],
      [InlineKeyboardButton("📣 Broadcast",callback_data="v3:broadcast")],
      [InlineKeyboardButton("📊 የእጅ ሪፖርት መሙላት",callback_data="v3:report")],
      [InlineKeyboardButton("🏆 የዕጣ ውጤት ማስገባት",callback_data="v3:winner")],
      [InlineKeyboardButton("🆘 Support ጽሑፍ",callback_data="v3:content")],
      [InlineKeyboardButton("📚 Tutorial ጽሑፍ አስተካክል",callback_data="v3:tutorialtext")],
      [InlineKeyboardButton("📎 Tutorial ፎቶ/ቪዲዮ ጫን",callback_data="v3:tutorialupload")],
      [InlineKeyboardButton("⚙️ የጊዜ ገደብ እና ቀን",callback_data="v3:settings")],
    ])
    await update.message.reply_text("🛠 <b>የሸገር ሎተሪ ተጨማሪ አስተዳደር</b>",parse_mode="HTML",reply_markup=kb)
    await update.message.reply_text("የአድሚን ቋሚ ቁልፎች፦", reply_markup=main_menu_keyboard(is_admin=True))

async def v3_callback(update,context):
    q=update.callback_query
    if q.from_user.id!=context.bot_data['admin_id']:
        await q.answer("አድሚን ብቻ።",show_alert=True); return
    await q.answer()
    if q.data=='v3:addmethod':
        context.user_data['v3_state']='method_name'
        context.user_data.pop('v3_method_name',None); context.user_data.pop('v3_method_account',None); context.user_data.pop('v3_method_holder',None)
        await q.message.reply_text("የክፍያ አማራጩን ስም ይላኩ (ለምሳሌ CBE, Awash, Dashen, Telebirr)።"); return
    if q.data=='v3:tutorialupload':
        context.user_data['await_tutorial_upload']=True
        await q.message.reply_text("አሁን Tutorial ፎቶ ወይም ቪዲዮ ይላኩ። ከዚያ በኋላ በተጠቃሚዎች Tutorial ገጽ ላይ ይታያል።"); return
    if q.data=='v3:methods':
        methods=list_payment_methods()
        if not methods: await q.message.reply_text("የተጨመረ የክፍያ አማራጭ የለም።"); return
        for m in methods:
            await q.message.reply_text(f"#{m['id']} <b>{html.escape(m['name'])}</b>\nየሂሳብ ቁጥር፦ <code>{html.escape(m['account'])}</code>\nየባለቤት ስም፦ {html.escape(m['holder'])}\nመመሪያ፦ {html.escape(m['instruction'])}",parse_mode='HTML',reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("አቦዝን",callback_data=f"v3:disablemethod:{m['id']}")]]))
        return
    if q.data.startswith('v3:disablemethod:'):
        mid=int(q.data.rsplit(':',1)[1])
        with db_session() as db: db.execute("UPDATE payment_methods SET active=FALSE WHERE id=?",(mid,))
        audit(q.from_user.id,'disable_payment_method',str(mid)); await q.message.reply_text("የክፍያ አማራጩ ተዘግቷል።"); return
    if q.data=='v3:broadcast:confirm':
        body=context.user_data.pop('v3_broadcast_body',None)
        context.user_data.pop('v3_state',None)
        if not body: await q.message.reply_text('የBroadcast ጽሑፍ አልተገኘም።'); return
        with db_session() as db: users=db.execute("SELECT user_id FROM users ORDER BY user_id").fetchall()
        sent=failed=0
        for u in users:
            try:
                await context.bot.send_message(chat_id=u['user_id'], text=body)
                sent += 1
            except RetryAfter as exc:
                delay = getattr(exc, 'retry_after', 1)
                delay = delay.total_seconds() if hasattr(delay, 'total_seconds') else float(delay)
                await asyncio.sleep(delay + 1)
                try:
                    await context.bot.send_message(chat_id=u['user_id'], text=body)
                    sent += 1
                except TelegramError:
                    failed += 1
            except TelegramError:
                failed += 1
            await asyncio.sleep(0.10)
        with db_session() as db: db.execute("INSERT INTO broadcast_log(admin_id,body,sent_count,failed_count) VALUES(?,?,?,?)",(q.from_user.id,body,sent,failed))
        audit(q.from_user.id,'broadcast',f'sent={sent},failed={failed}')
        await q.message.reply_text(f'Broadcast ተጠናቋል። የተላከ፦ {sent} | ያልደረሰ፦ {failed}')
        return
    if q.data=='v3:broadcast:cancel':
        context.user_data.pop('v3_broadcast_body',None); context.user_data.pop('v3_state',None)
        await q.message.reply_text('Broadcast ተሰርዟል።'); return
    states={'v3:tutorialtext':'tutorialtext','v3:broadcast':'broadcast','v3:report':'report','v3:winner':'winner','v3:content':'content','v3:settings':'settings'}
    if q.data in states:
        context.user_data['v3_state']=states[q.data]
        prompts={'broadcast':'ለሁሉም ተጠቃሚዎች የሚላከውን ጽሑፍ ይላኩ።','report':'ለተጠቃሚዎች የሚታይ የሪፖርት ጽሑፍ ይላኩ።','winner':'የአሸናፊውን ቁጥር እና ማስታወቂያ በዚህ ቅርጽ ይላኩ፦ 1234 | የውጤት መልእክት','content':'Support ጽሑፍ ይላኩ።','tutorialtext':'Tutorial መመሪያ ጽሑፍ ይላኩ። ፎቶ/ቪዲዮ ለመጫን /tutorialupload ይጠቀሙ፣','settings':'Pending ጊዜ በሰዓት ይላኩ፤ በሚቀጥለው መልእክት የዕጣ ቀን ያስገቡ።'}
        await q.message.reply_text(prompts[q.data]); return

async def v3_text_handler(update,context):
    state=context.user_data.get('v3_state'); txt=(update.message.text or '').strip()
    if not state: return False
    if update.effective_user.id!=context.bot_data['admin_id']:
        context.user_data.pop('v3_state',None); return True
    if state=='method_name':
        if not txt or len(txt) > 80: await update.message.reply_text('የክፍያ አማራጩን ስም በትክክል ያስገቡ።'); return True
        context.user_data['v3_method_name']=txt; context.user_data['v3_state']='method_account'; prompt='የሂሳብ ቁጥር/ስልክ ይላኩ።'
    elif state=='method_account':
        if not txt or len(txt) > 100: await update.message.reply_text('ትክክለኛ የሂሳብ ቁጥር/ስልክ ያስገቡ።'); return True
        context.user_data['v3_method_account']=txt; context.user_data['v3_state']='method_holder'; prompt='የሂሳብ ባለቤት ስም ይላኩ።'
    elif state=='method_holder':
        if not txt or len(txt) > 100: await update.message.reply_text('የሂሳብ ባለቤት ስም ያስገቡ።'); return True
        context.user_data['v3_method_holder']=txt; context.user_data['v3_state']='method_instruction'; prompt='ለተጠቃሚው የሚታይ የክፍያ መመሪያ ይላኩ፤ ከሌለ ምንም ይላኩ።'
    elif state=='method_instruction':
        with db_session() as db:
            db.execute("INSERT INTO payment_methods(name,account,holder,instruction) VALUES(?,?,?,?)",(context.user_data.pop('v3_method_name'),context.user_data.pop('v3_method_account'),context.user_data.pop('v3_method_holder'),txt))
        context.user_data.pop('v3_state',None); audit(update.effective_user.id,'add_payment_method',txt[:100]); await update.message.reply_text("✅ አዲሱ የክፍያ አማራጭ ተቀምጧል። ሌላ ባንክ ለመጨመር /adminv3 ይጻፉ።"); return True
    elif state=='report':
        update_setting('public_sales_report',txt); context.user_data.pop('v3_state',None); audit(update.effective_user.id,'update_manual_report'); await update.message.reply_text("ሪፖርቱ ተቀምጧል።"); return True
    elif state=='broadcast':
        if not txt or len(txt)>4096: await update.message.reply_text('ከ1 እስከ 4096 ቁምፊ ያለው መልእክት ይላኩ።'); return True
        context.user_data['v3_broadcast_body']=txt
        context.user_data['v3_state']='broadcast_confirm'
        with db_session() as db: count=db.execute("SELECT COUNT(*) AS n FROM users").fetchone()['n']
        await update.message.reply_text(f'⚠️ <b>Broadcast ማረጋገጫ</b>\n\nይህ መልእክት ለ{count} ተጠቃሚዎች ይላካል፦\n\n{html.escape(txt)}',parse_mode='HTML',reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('✅ አረጋግጥና ላክ',callback_data='v3:broadcast:confirm'),InlineKeyboardButton('❌ ሰርዝ',callback_data='v3:broadcast:cancel')]])); return True
    elif state=='winner':
        m=re.match(r'^\s*(\d+)\s*\|\s*(.+)$',txt)
        if not m: await update.message.reply_text("ትክክለኛ ቅርጽ፦ 1234 | የውጤት መልእክት"); return True
        number,announcement=m.groups(); st=get_settings()
        if not int(st['first_number'])<=int(number)<=int(st['last_number']): await update.message.reply_text("ቁጥሩ ከተወሰነው ክልል ውጭ ነው።"); return True
        number = canonical_number(number)
        with db_session() as db:
            valid=db.execute("SELECT 1 FROM ticket_allocations WHERE number=? LIMIT 1",(number,)).fetchone()
            if not valid: await update.message.reply_text("ይህ ቁጥር ተሽጦ አልተገኘም።"); return True
            db.execute("INSERT INTO lottery_draw_results(winning_number,announcement,admin_id) VALUES(?,?,?)",(number,announcement,update.effective_user.id))
            winners=db.execute("SELECT DISTINCT user_id FROM ticket_allocations WHERE number=?",(number,)).fetchall()
        context.user_data.pop('v3_state',None); audit(update.effective_user.id,'announce_winner',number)
        await update.message.reply_text("ውጤቱ ተመዝግቧል። ለአሸናፊዎች ማሳወቂያ እየተላከ ነው።")
        for w in winners:
            try: await context.bot.send_message(w['user_id'],f"🏆 እንኳን ደስ አለዎት! የአሸናፊ ቁጥር #{int(number)}\n{announcement}")
            except TelegramError: pass
        await notify_admin(context,f"🏆 የዕጣ ውጤት፦ #{int(number)}\n{html.escape(announcement)}")
        return True
    elif state=='content':
        update_setting('support_text',txt); context.user_data.pop('v3_state',None); audit(update.effective_user.id,'update_support'); await update.message.reply_text("Support መረጃ ተቀምጧል።"); return True
    elif state=='tutorialtext':
        update_setting('tutorial_text',txt); context.user_data.pop('v3_state',None); audit(update.effective_user.id,'update_tutorial_text'); await update.message.reply_text("Tutorial ጽሑፍ ተቀምጧል።"); return True
    elif state=='settings':
        if not txt.isdigit() or not 1<=int(txt)<=720: await update.message.reply_text("ከ1 እስከ 720 ሰዓት ያስገቡ።"); return True
        update_setting('pending_payment_hours',txt); context.user_data['v3_state']='drawdate'; await update.message.reply_text("አሁን የዕጣ ቀንና ሰዓት ያስገቡ።"); return True
    elif state=='drawdate':
        update_setting('draw_datetime',txt); context.user_data.pop('v3_state',None); audit(update.effective_user.id,'update_draw_date',txt); await update.message.reply_text("የPending ጊዜና የዕጣ ቀን ተቀምጠዋል።"); return True
    if state in {'method_name','method_account','method_holder'}:
        await update.message.reply_text(prompt)
        return True
    return True

async def tutorial_upload_command(update,context):
    if update.effective_user.id!=context.bot_data['admin_id']:
        await update.message.reply_text('አድሚን ብቻ ይህን ማድረግ ይችላል።'); return
    context.user_data['await_tutorial_upload']=True
    await update.message.reply_text('አሁን Tutorial ፎቶ ወይም ቪዲዮ ይላኩ።')

async def tutorial_upload_handler(update,context):
    if update.effective_user.id!=context.bot_data['admin_id'] or not context.user_data.get('await_tutorial_upload'): return
    if update.message.video:
        fid=update.message.video.file_id; typ='video'
    elif update.message.photo:
        fid=update.message.photo[-1].file_id; typ='photo'
    else: return
    update_setting('tutorial_file_id',fid); update_setting('tutorial_file_type',typ)
    context.user_data.pop('await_tutorial_upload',None)
    await update.message.reply_text('Tutorial ፋይሉ ተቀምጧል።')

async def profile_edit_handler(update,context):
    step=context.user_data.get('profile_edit_step')
    if not step: return False
    txt=(update.message.text or '').strip()
    if step=='name':
        if not 2<=len(txt)<=80: await update.message.reply_text('ስም ከ2 እስከ 80 ቁምፊ ይሁን።'); return True
        context.user_data['profile_new_name']=txt; context.user_data['profile_edit_step']='phone'
        await update.message.reply_text('አዲሱን ስልክ ቁጥር ያስገቡ።'); return True
    if step=='phone':
        phone=normalize_phone(txt)
        if not phone: await update.message.reply_text('ትክክለኛ ስልክ ያስገቡ።'); return True
        context.user_data['profile_new_phone']=phone; context.user_data['profile_edit_step']='address'
        await update.message.reply_text('አድራሻዎን ያስገቡ፤ ካልፈለጉ /skip ይጻፉ።'); return True
    if step=='address':
        address='' if txt=='/skip' else txt
        old=get_registered_user(update.effective_user.id)
        newname=context.user_data.pop('profile_new_name'); newphone=context.user_data.pop('profile_new_phone')
        with db_session() as db:
            db.execute("UPDATE users SET display_name=?,phone_number=?,address=? WHERE user_id=?",(newname,newphone,address,update.effective_user.id))
            db.execute("INSERT INTO user_profile_history(user_id,old_name,new_name,old_phone,new_phone,old_address,new_address) VALUES(?,?,?,?,?,?,?)",(update.effective_user.id,old['display_name'],newname,old['phone_number'],newphone,old['address'] or '',address))
        context.user_data.pop('profile_edit_step',None)
        await notify_admin(context,f"👤 የተጠቃሚ መረጃ ተቀይሯል\nID: <code>{update.effective_user.id}</code>\nስም: {html.escape(newname)}\nስልክ: {html.escape(newphone)}\nአድራሻ: {html.escape(address or 'አልተሰጠም')}",parse_mode='HTML')
        await update.message.reply_text('መረጃዎ ተዘምኗል።',reply_markup=main_menu_keyboard(is_admin=(update.effective_user.id == context.bot_data.get("admin_id")))); return True
    return False

async def expire_pending_job(context):
    # Expire only pending requests whose receipt was submitted; approved/assigned never expire.
    hours=int(get_settings().get('pending_payment_hours') or 24)
    with db_session() as db:
        rows=db.execute("UPDATE payments SET status='failed' WHERE status='pending' AND created_at::timestamp < CURRENT_TIMESTAMP - (? * INTERVAL '1 hour') RETURNING id,user_id",(hours,)).fetchall()
    for r in rows:
        try: await context.bot.send_message(r['user_id'],"⌛ የክፍያ ጥያቄዎ የጊዜ ገደብ አልፎበታል። አዲስ ጥያቄ ለመጀመር የትኬት ግዢ ይምረጡ።")
        except TelegramError: pass


class HealthCheckHandler(BaseHTTPRequestHandler):
    """Minimal HTTP endpoint for Render Web Service health checks."""

    def do_GET(self):
        if self.path not in ("/", "/health"):
            self.send_error(404, "Not Found")
            return
        body = b"Sheger Lottery bot process is running\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        if self.path not in ("/", "/health"):
            self.send_error(404, "Not Found")
            return
        body = b"Sheger Lottery bot process is running\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()

    def log_message(self, format, *args):
        # Keep routine health-check requests out of the Render application logs.
        return


def start_health_server():
    """Bind the HTTP health endpoint to Render's assigned port."""
    port_value = (os.getenv("PORT") or "").strip()
    if not port_value:
        raise RuntimeError(
            "Render did not provide PORT. This deployment must run as a Web Service."
        )
    try:
        port = int(port_value)
    except ValueError as exc:
        raise RuntimeError("PORT must be a valid integer.") from exc
    if not 1 <= port <= 65535:
        raise RuntimeError("PORT must be between 1 and 65535.")

    server = ThreadingHTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.daemon_threads = True
    thread = threading.Thread(
        target=server.serve_forever,
        name="render-health-check",
        daemon=True,
    )
    thread.start()
    logging.info("Render health-check server listening on 0.0.0.0:%s", port)
    return server

def main():
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    admin_id_value = (os.getenv("TELEGRAM_ADMIN_ID") or "").strip()
    if not token:
        raise RuntimeError(
            "Set TELEGRAM_BOT_TOKEN in Render environment variables; do not put the token in source code."
        )
    if not admin_id_value:
        raise RuntimeError(
            "Set TELEGRAM_ADMIN_ID to the administrator's numeric Telegram ID in Render environment variables."
        )
    try:
        admin_id = int(admin_id_value)
    except ValueError as exc:
        raise RuntimeError("Set TELEGRAM_ADMIN_ID to the administrator's numeric Telegram ID in Render environment variables.") from exc
    if admin_id <= 0:
        raise RuntimeError("TELEGRAM_ADMIN_ID must be a positive numeric Telegram user ID.")

    init_db()
    application = Application.builder().token(token).build()
    application.bot_data["admin_id"] = admin_id

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(CommandHandler("admin", admin_settings_command))
    application.add_handler(CommandHandler("adminv3", admin_v3_command))
    application.add_handler(CommandHandler("tutorialupload", tutorial_upload_command))
    application.add_handler(
        CallbackQueryHandler(
            ticket_chosen,
            pattern=r"^(ticket:(full|half|multi)|view_numbers(?::\d+)?|view_numbers_range:\d+:\d+|view_numbers_page:\d+:\d+)$",
        )
    )
    application.add_handler(
        CallbackQueryHandler(repeat_selection_handler, pattern=r"^repeat:(yes|no):(full|half)$")
    )
    application.add_handler(
        CallbackQueryHandler(admin_approval_handler, pattern=r"^(approve|reject):\d+$")
    )
    application.add_handler(
        CallbackQueryHandler(admin_settings_callback, pattern=r"^admin:")
    )
    application.add_handler(
        CallbackQueryHandler(admin_management_callback, pattern=r"^manage:")
    )
    application.add_handler(CallbackQueryHandler(v3_callback, pattern=r"^v3:"))
    application.add_handler(CallbackQueryHandler(payment_method_callback, pattern=r"^paymethod:\d+$"))
    application.add_handler(MessageHandler((filters.PHOTO | filters.VIDEO), tutorial_upload_handler), group=1)
    application.add_handler(CallbackQueryHandler(multi_ticket_type_handler, pattern=r"^multi_type:(full|half)$"))
    application.add_handler(
        CallbackQueryHandler(number_mode_handler, pattern=r"^mode:(auto|manual):\d+$")
    )
    application.add_handler(MessageHandler(filters.PHOTO, handle_receipt))
    application.add_handler(
        MessageHandler(filters.CONTACT, registration_contact_handler)
    )
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler)
    )

    if application.job_queue:
        application.job_queue.run_repeating(expire_pending_job, interval=300, first=30, name="expire_pending_payments")

    # Render Web Services require an HTTP listener on 0.0.0.0:$PORT.
    # The bot itself continues to receive Telegram updates through polling.
    health_server = start_health_server()
    try:
        application.run_polling()
    finally:
        health_server.shutdown()
        health_server.server_close()


if __name__ == "__main__":
    main()
