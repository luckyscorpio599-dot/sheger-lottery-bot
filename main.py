import asyncio
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

import asyncpg
from aiohttp import web
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.error import TelegramError
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

# Sheger Lottery Bot: PostgreSQL + Render. This code never drops tables.
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("sheger-lottery")
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

DEFAULT_SETTINGS = {
    "lottery_id": DEFAULT_LOTTERY_ID,
    "lottery_name": "ሸገር ሎተሪ",
    "full_price": "2500.00",
    "half_price": "1250.00",
    "reservation_minutes": "30",
    "payment_instructions": "ክፍያውን በታች በተጠቀሰው የባንክ ሂሳብ ያስገቡ፤ ከዚያ የክፍያ ደረሰኙን ፎቶ ይላኩ።",
    "bank_name": "እባክዎ በAdmin ያዘምኑ",
    "bank_account": "እባክዎ በAdmin ያዘምኑ",
    "account_holder": "እባክዎ በAdmin ያዘምኑ",
    "receipt_instruction": "📷 የክፍያ ደረሰኙን ግልጽ ፎቶ ይላኩ።",
    "welcome_instructions": "📌 የተሳትፎ መመሪያ፦\n1️⃣ የዕጣ አይነት ይምረጡ።\n2️⃣ ቁጥር ያስይዙ።\n3️⃣ ክፍያ ፈጽመው ደረሰኙን ይላኩ።\n4️⃣ Admin ካጸደቀ ዕጣዎ ይረጋገጣል።",
    "welcome_prompt": "ከታች ያለውን ምናሌ ይጠቀሙ።",
    "number_min": "1",
    "number_max": "6000",
    "sales_open": "true",
}
MAIN_MENU = ReplyKeyboardMarkup([
    [KeyboardButton("🎟 ዕጣ ግዛ"), KeyboardButton("🎫 ዕጣዎቼ")],
    [KeyboardButton("💳 የክፍያ ታሪክ"), KeyboardButton("ℹ️ የክፍያ መረጃ")],
    [KeyboardButton("❓ እገዛ")],
], resize_keyboard=True)
ADMIN_MENU = ReplyKeyboardMarkup([
    [KeyboardButton("📊 Admin Dashboard"), KeyboardButton("⚙️ Settings")],
    [KeyboardButton("👥 Users"), KeyboardButton("🚫 Block Number")],
    [KeyboardButton("✅ Unblock Number"), KeyboardButton("🧾 Pending Payments")],
    [KeyboardButton("⬅️ Main Menu")],
], resize_keyboard=True)

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
 user_id BIGINT PRIMARY KEY, username TEXT, first_name TEXT NOT NULL DEFAULT '', last_name TEXT NOT NULL DEFAULT '',
 display_name TEXT, phone_number TEXT, is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS tickets (
 id BIGSERIAL PRIMARY KEY, lottery_id TEXT NOT NULL, user_id BIGINT NOT NULL REFERENCES users(user_id),
 number INTEGER NOT NULL CHECK(number BETWEEN 0 AND 9999), ticket_type TEXT NOT NULL CHECK(ticket_type IN ('full','half')),
 share_no SMALLINT, amount NUMERIC(12,2) NOT NULL CHECK(amount>=0),
 status TEXT NOT NULL DEFAULT 'reserved' CHECK(status IN ('reserved','paid','cancelled','expired')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), expires_at TIMESTAMPTZ NOT NULL,
 CHECK((ticket_type='full' AND share_no IS NULL) OR (ticket_type='half' AND share_no IN (1,2)))
);
CREATE TABLE IF NOT EXISTS allocations (
 id BIGSERIAL PRIMARY KEY, lottery_id TEXT NOT NULL, number INTEGER NOT NULL CHECK(number BETWEEN 0 AND 9999),
 share_no SMALLINT NOT NULL CHECK(share_no IN (0,1,2)), ticket_id BIGINT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
 UNIQUE(lottery_id,number,share_no)
);
CREATE TABLE IF NOT EXISTS blocked_numbers (
 id BIGSERIAL PRIMARY KEY, lottery_id TEXT NOT NULL, number INTEGER NOT NULL CHECK(number BETWEEN 0 AND 9999),
 blocked_by BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(lottery_id,number)
);
CREATE TABLE IF NOT EXISTS payments (
 id BIGSERIAL PRIMARY KEY, ticket_id BIGINT NOT NULL REFERENCES tickets(id), user_id BIGINT NOT NULL REFERENCES users(user_id),
 amount NUMERIC(12,2) NOT NULL, receipt_file_id TEXT NOT NULL, receipt_caption TEXT NOT NULL DEFAULT '',
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','approved','rejected')),
 reviewed_by BIGINT, review_note TEXT NOT NULL DEFAULT '', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), reviewed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_tickets_user_created ON tickets(user_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_payments_user_created ON payments(user_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_payments_pending ON payments(status,created_at);
CREATE INDEX IF NOT EXISTS idx_tickets_expiry ON tickets(status,expires_at);
"""

async def init_db():
    global POOL
    dsn = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    opts = dict(dsn=dsn, min_size=1, max_size=5, command_timeout=30)
    if "localhost" not in dsn and "127.0.0.1" not in dsn:
        opts["ssl"] = "require"
    POOL = await asyncpg.create_pool(**opts)
    async with POOL.acquire() as conn:
        async with conn.transaction():
            # If the old psycopg version created an incompatible payments table, archive it; do not delete it.
            exists = await conn.fetchval("SELECT to_regclass('public.payments') IS NOT NULL")
            if exists:
                cols = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='payments'")
                if "ticket_id" not in {r['column_name'] for r in cols}:
                    legacy = await conn.fetchval("SELECT to_regclass('public.legacy_payments') IS NOT NULL")
                    archive_name = "legacy_payments_archive" if legacy else "legacy_payments"
                    archive_exists = await conn.fetchval(f"SELECT to_regclass('public.{archive_name}') IS NOT NULL")
                    if archive_exists:
                        raise RuntimeError("Incompatible payments table and archive already exist. Back up and migrate manually before restarting.")
                    await conn.execute(f'ALTER TABLE payments RENAME TO "{archive_name}"')
                    log.warning("Preserved old payments table as %s", archive_name)
            # The first supplied version used a different blocked_numbers layout.
            blocked_exists = await conn.fetchval("SELECT to_regclass('public.blocked_numbers') IS NOT NULL")
            if blocked_exists:
                blocked_cols = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='blocked_numbers'")
                if "lottery_id" not in {r["column_name"] for r in blocked_cols}:
                    archive_exists = await conn.fetchval("SELECT to_regclass('public.legacy_blocked_numbers') IS NOT NULL")
                    if archive_exists:
                        raise RuntimeError("Incompatible blocked_numbers table and legacy archive already exist. Back up and migrate manually.")
                    await conn.execute('ALTER TABLE blocked_numbers RENAME TO legacy_blocked_numbers')
                    log.warning("Preserved old blocked_numbers table as legacy_blocked_numbers")
            await conn.execute(SCHEMA)
            # Additive columns let the users table work with either supplied version.
            for statement in (
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS phone_number TEXT",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_blocked BOOLEAN NOT NULL DEFAULT FALSE",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
            ):
                await conn.execute(statement)
            old_settings = await conn.fetchval("SELECT to_regclass('public.bot_settings') IS NOT NULL")
            if old_settings:
                await conn.execute("INSERT INTO settings(key,value) SELECT setting_key,setting_value FROM bot_settings ON CONFLICT(key) DO NOTHING")
                for old, new in (("full_ticket_price", "full_price"), ("half_ticket_price", "half_price"), ("first_number", "number_min"), ("last_number", "number_max")):
                    await conn.execute("INSERT INTO settings(key,value) SELECT $1,setting_value FROM bot_settings WHERE setting_key=$2 ON CONFLICT(key) DO NOTHING", new, old)
            for key, value in DEFAULT_SETTINGS.items():
                await conn.execute("INSERT INTO settings(key,value) VALUES($1,$2) ON CONFLICT(key) DO NOTHING", key, value)
            # Preserve old blocked numbers and make them active in the configured lottery.
            legacy_blocked = await conn.fetchval("SELECT to_regclass('public.legacy_blocked_numbers') IS NOT NULL")
            if legacy_blocked:
                await conn.execute(
                    """INSERT INTO blocked_numbers(lottery_id,number,blocked_by)
                       SELECT $1, number, created_by FROM legacy_blocked_numbers
                       ON CONFLICT(lottery_id,number) DO NOTHING""",
                    DEFAULT_LOTTERY_ID,
                )
            await conn.execute("UPDATE users SET display_name=COALESCE(NULLIF(display_name,''),NULLIF(BTRIM(first_name||' '||last_name),''),'ተጠቃሚ') WHERE display_name IS NULL OR display_name='' ")
            # Empty phone satisfies the legacy table's NOT NULL constraint until the user registers.
            await conn.execute("INSERT INTO users(user_id,first_name,display_name,phone_number,is_blocked) VALUES($1,'Administrator','Administrator','',FALSE) ON CONFLICT(user_id) DO NOTHING", ADMIN_ID)
    log.info("Database initialized without dropping tables")

async def db():
    if POOL is None: raise RuntimeError("Database pool is not initialized")
    return POOL
async def get_setting(key, default=""):
    pool = await db()
    async with pool.acquire() as conn: value = await conn.fetchval("SELECT value FROM settings WHERE key=$1", key)
    return value if value is not None else default
async def set_setting(key, value):
    pool = await db()
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO settings(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value", key, value)
def money(value): return f"{Decimal(str(value)):,.2f} ብር"
async def is_admin(update): return bool(update.effective_user and update.effective_user.id == ADMIN_ID)
async def get_user(user_id):
    pool = await db()
    async with pool.acquire() as conn: return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)
async def register_telegram_user(update):
    u = update.effective_user
    if not u: return False
    pool = await db()
    async with pool.acquire() as conn:
        await conn.execute("""INSERT INTO users(user_id,username,first_name,last_name,display_name,phone_number,last_seen_at)
          VALUES($1,$2,$3,$4,$5,'',NOW()) ON CONFLICT(user_id) DO UPDATE SET username=EXCLUDED.username,
          first_name=EXCLUDED.first_name,last_name=EXCLUDED.last_name,
          display_name=COALESCE(NULLIF(users.display_name,''),EXCLUDED.display_name),last_seen_at=NOW()""",
          u.id,u.username,u.first_name or "",u.last_name or "",u.full_name or "ተጠቃሚ")
        blocked = await conn.fetchval("SELECT is_blocked FROM users WHERE user_id=$1", u.id)
    return not bool(blocked)
async def ensure_access(update, context, require_phone=True):
    if not await register_telegram_user(update):
        if update.effective_message: await update.effective_message.reply_text("🚫 መለያዎ ታግዷል። Adminን ያነጋግሩ።")
        return False
    row = await get_user(update.effective_user.id)
    if require_phone and (not row or not row['phone_number']):
        context.user_data['registration_step'] = 'phone'
        if update.effective_message:
            kb = ReplyKeyboardMarkup([[KeyboardButton("📱 ስልኬን አጋራ", request_contact=True)]], resize_keyboard=True, one_time_keyboard=True)
            await update.effective_message.reply_text("ለመቀጠል ስልክ ቁጥርዎን ያስመዝግቡ። ከታች ያለውን ቁልፍ ይጫኑ ወይም ቁጥርዎን ይጻፉ።", reply_markup=kb)
        return False
    return True
async def show_menu(update, context):
    settings = {k: await get_setting(k,v) for k,v in DEFAULT_SETTINGS.items()}
    await update.effective_message.reply_text(f"ሰላም {update.effective_user.full_name or 'ተጠቃሚ'}! እንኳን ወደ {settings['lottery_name']} በደህና መጡ።\n\n{settings['welcome_instructions']}\n\n{settings['welcome_prompt']}", reply_markup=ADMIN_MENU if await is_admin(update) else MAIN_MENU)
async def save_phone(update, context, phone):
    phone = phone.strip(); digits = re.sub(r"\D", "", phone)
    if not 9 <= len(digits) <= 15:
        await update.effective_message.reply_text("የስልክ ቁጥሩ ትክክል አይደለም። ምሳሌ፦ 0911223344")
        return
    pool = await db(); u = update.effective_user
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET phone_number=$2,display_name=COALESCE(NULLIF(display_name,''),$3) WHERE user_id=$1",u.id,phone,u.full_name or 'ተጠቃሚ')
    context.user_data.pop('registration_step',None)
    await update.effective_message.reply_text("✅ ምዝገባዎ ተጠናቋል።", reply_markup=MAIN_MENU)
    await show_menu(update,context)
async def start(update, context):
    if not await ensure_access(update,context): return
    await show_menu(update,context)
async def help_command(update, context):
    if not await ensure_access(update,context): return
    await update.effective_message.reply_text("📌 አጠቃቀም፦\n1. 🎟 ዕጣ ግዛ ይጫኑ።\n2. ሙሉ ወይም ግማሽ ዕጣ ይምረጡ።\n3. ቁጥር ያስገቡ።\n4. ክፍያ ፈጽመው ደረሰኙን ይላኩ።\n5. Admin ሲያጸድቅ ዕጣዎ ይረጋገጣል።")
async def payment_info(update, context):
    if not await ensure_access(update,context): return
    s={k:await get_setting(k,v) for k,v in DEFAULT_SETTINGS.items()}
    await update.effective_message.reply_text(f"🏦 ባንክ፦ {s['bank_name']}\n🔢 ሂሳብ፦ {s['bank_account']}\n👤 ባለቤት፦ {s['account_holder']}\n\n{s['payment_instructions']}\n{s['receipt_instruction']}")
async def buy_ticket(update, context):
    if not await ensure_access(update,context): return
    if (await get_setting('sales_open','true')).lower()!='true':
        await update.effective_message.reply_text("⛔ የዕጣ ሽያጭ ለጊዜው ተዘግቷል።"); return
    full=await get_setting('full_price',DEFAULT_SETTINGS['full_price']); half=await get_setting('half_price',DEFAULT_SETTINGS['half_price'])
    kb=InlineKeyboardMarkup([[InlineKeyboardButton(f"🎫 ሙሉ ዕጣ ({money(full)})",callback_data='buy:full'),InlineKeyboardButton(f"🎟 ግማሽ ዕጣ ({money(half)})",callback_data='buy:half')]])
    await update.effective_message.reply_text("የዕጣ አይነት ይምረጡ፦",reply_markup=kb)
async def buy_type_callback(update, context):
    q=update.callback_query; await q.answer()
    if not await register_telegram_user(update): await q.edit_message_text("🚫 መለያዎ ታግዷል።"); return
    row=await get_user(q.from_user.id)
    if not row or not row['phone_number']:
        context.user_data['registration_step']='phone'; await q.message.reply_text("ስልክ ቁጥርዎን ለማስመዝገብ /start ይጫኑ።"); return
    kind=q.data.split(':')[1]
    if kind not in ('full','half'): return
    context.user_data['ticket_type']=kind; context.user_data['awaiting_number']=True
    await q.edit_message_text(f"{'ሙሉ' if kind=='full' else 'ግማሽ'} ዕጣ መርጠዋል። የሚፈልጉትን ቁጥር ያስገቡ።")
async def create_reservation(update,context,number):
    kind=context.user_data.get('ticket_type'); user_id=update.effective_user.id
    if kind not in ('full','half'): await update.effective_message.reply_text("መጀመሪያ 🎟 ዕጣ ግዛ ይጫኑ።"); return
    if (await get_setting('sales_open','true')).lower()!='true': await update.effective_message.reply_text("⛔ ሽያጭ ተዘግቷል።"); return
    try:
        low=int(await get_setting('number_min','1')); high=int(await get_setting('number_max','6000'))
        mins=max(1,min(1440,int(await get_setting('reservation_minutes','30'))))
        amount=Decimal(await get_setting('full_price' if kind=='full' else 'half_price',DEFAULT_SETTINGS['full_price' if kind=='full' else 'half_price']))
        lottery=await get_setting('lottery_id',DEFAULT_LOTTERY_ID)
        if low<0 or high>9999 or low>high or amount<0: raise ValueError
    except (ValueError,InvalidOperation): await update.effective_message.reply_text("⚠️ የሎተሪ ቅንብሮች ትክክል አይደሉም።"); return
    if not low<=number<=high: await update.effective_message.reply_text(f"ከ {low} እስከ {high} ያለ ቁጥር ያስገቡ።"); return
    expires=datetime.now(timezone.utc)+timedelta(minutes=mins); pool=await db()
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute('SELECT pg_advisory_xact_lock(hashtext($1),$2)',lottery,number)
                await conn.execute("""UPDATE tickets t SET status='expired' WHERE t.lottery_id=$1 AND t.number=$2 AND t.status='reserved' AND t.expires_at<=NOW() AND NOT EXISTS(SELECT 1 FROM payments p WHERE p.ticket_id=t.id AND p.status='pending')""",lottery,number)
                await conn.execute("DELETE FROM allocations a USING tickets t WHERE a.ticket_id=t.id AND t.lottery_id=$1 AND t.number=$2 AND t.status IN ('expired','cancelled')",lottery,number)
                blocked=await conn.fetchval('SELECT EXISTS(SELECT 1 FROM blocked_numbers WHERE lottery_id=$1 AND number=$2)',lottery,number)
                if blocked: raise ValueError('ይህ ቁጥር በAdmin ታግዷል።')
                occupied=await conn.fetch("""SELECT a.share_no FROM allocations a JOIN tickets t ON t.id=a.ticket_id WHERE a.lottery_id=$1 AND a.number=$2 AND t.status IN ('reserved','paid') AND (t.status='paid' OR t.expires_at>NOW())""",lottery,number)
                shares={int(r['share_no']) for r in occupied}
                if kind=='full':
                    if shares: raise ValueError('ይህ ቁጥር ተይዟል። ሌላ ይምረጡ።')
                    share=None
                    tid=await conn.fetchval("INSERT INTO tickets(lottery_id,user_id,number,ticket_type,share_no,amount,status,expires_at) VALUES($1,$2,$3,'full',NULL,$4,'reserved',$5) RETURNING id",lottery,user_id,number,amount,expires)
                    await conn.execute('INSERT INTO allocations(lottery_id,number,share_no,ticket_id) VALUES($1,$2,0,$3)',lottery,number,tid)
                else:
                    if 0 in shares: raise ValueError('ይህ ቁጥር በሙሉ ዕጣ ተይዟል።')
                    share=next((x for x in (1,2) if x not in shares),None)
                    if share is None: raise ValueError('ሁለቱም ግማሾች ተይዘዋል።')
                    tid=await conn.fetchval("INSERT INTO tickets(lottery_id,user_id,number,ticket_type,share_no,amount,status,expires_at) VALUES($1,$2,$3,'half',$4,$5,'reserved',$6) RETURNING id",lottery,user_id,number,share,amount,expires)
                    await conn.execute('INSERT INTO allocations(lottery_id,number,share_no,ticket_id) VALUES($1,$2,$3,$4)',lottery,number,share,tid)
    except ValueError as e: await update.effective_message.reply_text(str(e)); return
    except asyncpg.UniqueViolationError: await update.effective_message.reply_text('ይህ ቁጥር አሁን ተይዟል። ሌላ ይሞክሩ።'); return
    context.user_data['awaiting_number']=False; context.user_data['awaiting_receipt_ticket_id']=int(tid)
    s={k:await get_setting(k,v) for k,v in DEFAULT_SETTINGS.items()}
    await update.effective_message.reply_text(f"✅ ቁጥር {number:04d} ለጊዜው ተይዟል።\nዕጣ #{tid} | {'ሙሉ' if kind=='full' else 'ግማሽ'}"+(f' (ግማሽ {share})' if share else '')+f"\nየሚከፈለው፦ {money(amount)}\nየማስያዣ ጊዜ፦ {mins} ደቂቃ\n\n🏦 {s['bank_name']}\n🔢 {s['bank_account']}\n👤 {s['account_holder']}\n{s['payment_instructions']}\n{s['receipt_instruction']}")
async def my_tickets(update,context):
    if not await ensure_access(update,context): return
    pool=await db()
    async with pool.acquire() as conn: rows=await conn.fetch('SELECT id,number,ticket_type,share_no,amount,status,expires_at FROM tickets WHERE user_id=$1 ORDER BY created_at DESC LIMIT 20',update.effective_user.id)
    if not rows: await update.effective_message.reply_text('እስካሁን ዕጣ አልገዙም።'); return
    lines=['🎫 የዕጣዎችዎ፦']
    for r in rows:
        status=r['status'].upper()
        if status=='RESERVED' and r['expires_at']<datetime.now(timezone.utc): status='EXPIRED'
        half=f" (ግማሽ {r['share_no']})" if r['ticket_type']=='half' else ''
        lines.append(f"#{r['id']} | {r['number']:04d} | {r['ticket_type']}{half} | {money(r['amount'])} | {status}")
    await update.effective_message.reply_text('\n'.join(lines))
async def payment_history(update,context):
    if not await ensure_access(update,context): return
    pool=await db()
    async with pool.acquire() as conn: rows=await conn.fetch('SELECT p.id,p.ticket_id,p.amount,p.status,p.review_note,t.number FROM payments p JOIN tickets t ON t.id=p.ticket_id WHERE p.user_id=$1 ORDER BY p.created_at DESC LIMIT 20',update.effective_user.id)
    if not rows: await update.effective_message.reply_text('የክፍያ ታሪክ የለዎትም።'); return
    await update.effective_message.reply_text('💳 የክፍያ ታሪክ፦\n'+'\n'.join(f"#{r['id']} | ዕጣ #{r['ticket_id']} | ቁጥር {r['number']:04d} | {money(r['amount'])} | {r['status'].upper()} {r['review_note']}" for r in rows))
async def handle_receipt(update,context):
    if not await ensure_access(update,context): return
    tid=context.user_data.get('awaiting_receipt_ticket_id'); msg=update.effective_message
    if not tid: await msg.reply_text('መጀመሪያ ዕጣ ቁጥር ያስይዙ፣ ከዚያ ደረሰኝ ይላኩ።'); return
    fid=msg.photo[-1].file_id if msg.photo else (msg.document.file_id if msg.document and (msg.document.mime_type or '').startswith('image/') else None)
    if not fid: await msg.reply_text('ደረሰኙን እንደ ፎቶ ይላኩ።'); return
    pool=await db()
    async with pool.acquire() as conn:
        async with conn.transaction():
            ticket=await conn.fetchrow('SELECT id,user_id,amount,status,expires_at FROM tickets WHERE id=$1 AND user_id=$2 FOR UPDATE',int(tid),update.effective_user.id)
            if not ticket: context.user_data.pop('awaiting_receipt_ticket_id',None); await msg.reply_text('ዕጣው አልተገኘም።'); return
            if ticket['status']!='reserved' or ticket['expires_at']<datetime.now(timezone.utc):
                await conn.execute("UPDATE tickets SET status='expired' WHERE id=$1 AND status='reserved'",int(tid)); context.user_data.pop('awaiting_receipt_ticket_id',None); await msg.reply_text('የማስያዣ ጊዜው አልቋል። እንደገና ያስይዙ።'); return
            existing=await conn.fetchval("SELECT id FROM payments WHERE ticket_id=$1 AND status='pending' LIMIT 1",int(tid))
            if existing: await msg.reply_text('ደረሰኙ አስቀድሞ ተልኳል።'); return
            pid=await conn.fetchval('INSERT INTO payments(ticket_id,user_id,amount,receipt_file_id,receipt_caption) VALUES($1,$2,$3,$4,$5) RETURNING id',int(tid),update.effective_user.id,ticket['amount'],fid,msg.caption or '')
            info=await conn.fetchrow('SELECT number,ticket_type,share_no FROM tickets WHERE id=$1',int(tid))
    context.user_data.pop('awaiting_receipt_ticket_id',None); await msg.reply_text(f'⏳ ደረሰኙ ተልኳል። ክፍያ #{pid} በAdmin እየተጣራ ነው።')
    kb=InlineKeyboardMarkup([[InlineKeyboardButton('✅ አጽድቅ',callback_data=f'pay:approve:{pid}'),InlineKeyboardButton('❌ ውድቅ አድርግ',callback_data=f'pay:reject:{pid}')]])
    profile=await get_user(update.effective_user.id)
    try:
        await context.bot.send_photo(chat_id=ADMIN_ID,photo=fid,caption=f"📥 አዲስ ደረሰኝ\nክፍያ #{pid} | ዕጣ #{tid}\nስም፦ {profile['display_name'] or profile['first_name']}\nስልክ፦ {profile['phone_number'] or '-'}\nUser ID፦ {update.effective_user.id}\nቁጥር፦ {info['number']:04d}\nአይነት፦ {info['ticket_type']}\nመጠን፦ {money(ticket['amount'])}\nማስታወሻ፦ {msg.caption or '-'}",reply_markup=kb)
    except TelegramError:
        log.exception('Admin notification failed for payment %s',pid); await msg.reply_text('ደረሰኙ ተቀምጧል፣ ግን Adminን ማሳወቅ አልተቻለም።')
async def payment_review_callback(update,context):
    q=update.callback_query
    if not await is_admin(update): await q.answer('Admin ብቻ ነው።',show_alert=True); return
    await q.answer(); _,action,raw=q.data.split(':'); pid=int(raw); pool=await db(); user_id=tid=None; result='rejected'
    async with pool.acquire() as conn:
        async with conn.transaction():
            p=await conn.fetchrow('SELECT * FROM payments WHERE id=$1 FOR UPDATE',pid)
            if not p: await q.edit_message_caption('የክፍያ መዝገቡ አልተገኘም።'); return
            if p['status']!='pending': await q.answer(f"አስቀድሞ {p['status']} ተደርጓል።",show_alert=True); return
            user_id,tid=p['user_id'],p['ticket_id']
            if action=='approve':
                t=await conn.fetchrow('SELECT status FROM tickets WHERE id=$1 FOR UPDATE',tid)
                if not t or t['status']!='reserved':
                    await conn.execute("UPDATE payments SET status='rejected',reviewed_by=$2,reviewed_at=NOW(),review_note='Reservation no longer active' WHERE id=$1",pid,ADMIN_ID)
                    await q.edit_message_caption('ማጽደቅ አልተቻለም፤ ማስያዣው ንቁ አይደለም።'); return
                await conn.execute("UPDATE payments SET status='approved',reviewed_by=$2,reviewed_at=NOW() WHERE id=$1",pid,ADMIN_ID)
                await conn.execute("UPDATE tickets SET status='paid' WHERE id=$1",tid); result='approved'
            else: await conn.execute("UPDATE payments SET status='rejected',reviewed_by=$2,reviewed_at=NOW(),review_note='በAdmin ውድቅ ተደርጓል' WHERE id=$1",pid,ADMIN_ID)
    # Allow a replacement receipt after rejection; handle_receipt still verifies expiry.
    if result == 'rejected' and user_id is not None and tid is not None:
        context.application.user_data[user_id]['awaiting_receipt_ticket_id'] = tid
    try: await context.bot.send_message(chat_id=user_id,text=f"የክፍያ #{pid} ውሳኔ፦ {'ጸድቋል። ዕጣዎ ተረጋግጧል።' if result=='approved' else 'ውድቅ ተደርጓል። ማስያዣው ካልተጠናቀቀ አዲስ ደረሰኝ ይላኩ።'}")
    except TelegramError: log.warning('Could not notify user %s',user_id)
    try: await q.edit_message_caption((q.message.caption or '')+f'\n\nውሳኔ፦ {result.upper()}')
    except TelegramError: pass
async def admin_required(update):
    if not await is_admin(update):
        if update.effective_message: await update.effective_message.reply_text('⛔ Admin ብቻ ነው።')
        return False
    await register_telegram_user(update); return True
async def admin_command(update,context):
    if await admin_required(update): await update.effective_message.reply_text('የAdmin ምናሌ፦',reply_markup=ADMIN_MENU)
async def admin_dashboard(update,context):
    if not await admin_required(update): return
    pool=await db()
    async with pool.acquire() as conn:
        users=await conn.fetchval('SELECT COUNT(*) FROM users'); blocked=await conn.fetchval('SELECT COUNT(*) FROM users WHERE is_blocked=TRUE'); pending=await conn.fetchval("SELECT COUNT(*) FROM payments WHERE status='pending'"); paid=await conn.fetchval("SELECT COUNT(*) FROM tickets WHERE status='paid'"); reserved=await conn.fetchval("SELECT COUNT(*) FROM tickets WHERE status='reserved' AND expires_at>NOW()"); revenue=await conn.fetchval("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='approved'")
    await update.effective_message.reply_text(f"📊 Admin Dashboard\nተጠቃሚዎች፦ {users} (የታገዱ፦ {blocked})\nበመጠባበቅ ላይ ክፍያዎች፦ {pending}\nየተረጋገጡ ዕጣዎች፦ {paid}\nንቁ ማስያዣዎች፦ {reserved}\nገቢ፦ {money(revenue)}",reply_markup=ADMIN_MENU)
async def settings_menu(update,context):
    if not await admin_required(update): return
    values={k:await get_setting(k,v) for k,v in DEFAULT_SETTINGS.items()}
    lines=['⚙️ የሎተሪ ቅንብሮች፦']+[f'{k}: {v}' for k,v in values.items()]
    lines.append('\nቅንብር ለመቀየር /set KEY VALUE ይጠቀሙ። ምሳሌ፦ /set full_price 2500\n/set bank_name CBE\n/set bank_account የሂሳብ_ቁጥር\n/set account_holder የባለቤት_ስም')
    await update.effective_message.reply_text('\n'.join(lines),reply_markup=ADMIN_MENU)
async def set_command(update,context):
    if not await admin_required(update): return
    if len(context.args)<2: await update.effective_message.reply_text('አጠቃቀም፦ /set KEY VALUE'); return
    key=context.args[0].lower(); value=' '.join(context.args[1:]).strip()
    if key not in DEFAULT_SETTINGS: await update.effective_message.reply_text('ያልታወቀ ቅንብር ነው። /settings ይጠቀሙ።'); return
    if key in ('full_price','half_price'):
        try:
            n=Decimal(value)
            if n<0: raise InvalidOperation
            value=str(n.quantize(Decimal('0.01')))
        except (InvalidOperation,ValueError): await update.effective_message.reply_text('ዋጋው ትክክለኛ ቁጥር መሆን አለበት።'); return
    if key in ('reservation_minutes','number_min','number_max'):
        try:
            n=int(value)
            if (key=='reservation_minutes' and not 1<=n<=1440) or (key!='reservation_minutes' and not 0<=n<=9999): raise ValueError
        except ValueError: await update.effective_message.reply_text('ቁጥሩ ከተፈቀደው ክልል ውጭ ነው።'); return
    if key=='sales_open':
        if value.lower() not in ('true','false'): await update.effective_message.reply_text('sales_open true ወይም false መሆን አለበት።'); return
        value=value.lower()
    if key in ('number_min','number_max'):
        low=int(value) if key=='number_min' else int(await get_setting('number_min','1')); high=int(value) if key=='number_max' else int(await get_setting('number_max','6000'))
        if low>high: await update.effective_message.reply_text('number_min ከ number_max መብለጥ አይችልም።'); return
    await set_setting(key,value); await update.effective_message.reply_text(f'✅ {key} ተዘምኗል።')
async def users_command(update,context):
    if not await admin_required(update): return
    pool=await db()
    async with pool.acquire() as conn: rows=await conn.fetch('SELECT user_id,username,first_name,phone_number,is_blocked FROM users ORDER BY created_at DESC LIMIT 30')
    if not rows: await update.effective_message.reply_text('ተጠቃሚዎች የሉም።'); return
    lines=['👥 የቅርብ ጊዜ ተጠቃሚዎች፦']+[f"{r['user_id']} | {r['first_name']} | @{r['username'] or '-'} | {r['phone_number'] or 'ስልክ የለም'} | {'BLOCKED' if r['is_blocked'] else 'active'}" for r in rows]
    lines.append('/blockuser USER_ID ወይም /unblockuser USER_ID'); await update.effective_message.reply_text('\n'.join(lines),reply_markup=ADMIN_MENU)
async def block_user_command(update,context):
    if not await admin_required(update): return
    if len(context.args)!=1 or not context.args[0].isdigit(): await update.effective_message.reply_text('አጠቃቀም፦ /blockuser USER_ID'); return
    target=int(context.args[0])
    if target==ADMIN_ID: await update.effective_message.reply_text('Adminን ማገድ አይቻልም።'); return
    pool=await db()
    async with pool.acquire() as conn: result=await conn.execute('UPDATE users SET is_blocked=TRUE WHERE user_id=$1',target)
    await update.effective_message.reply_text('ተጠቃሚው ታግዷል።' if result.endswith('1') else 'ተጠቃሚው አልተገኘም።')
async def unblock_user_command(update,context):
    if not await admin_required(update): return
    if len(context.args)!=1 or not context.args[0].isdigit(): await update.effective_message.reply_text('አጠቃቀም፦ /unblockuser USER_ID'); return
    pool=await db()
    async with pool.acquire() as conn: result=await conn.execute('UPDATE users SET is_blocked=FALSE WHERE user_id=$1',int(context.args[0]))
    await update.effective_message.reply_text('እገዳው ተነስቷል።' if result.endswith('1') else 'ተጠቃሚው አልተገኘም።')
async def block_number_command(update,context):
    if not await admin_required(update): return
    if len(context.args)!=1 or not context.args[0].isdigit(): await update.effective_message.reply_text('አጠቃቀም፦ /blocknumber NUMBER'); return
    n=int(context.args[0]); low=int(await get_setting('number_min','1')); high=int(await get_setting('number_max','6000'))
    if not low<=n<=high: await update.effective_message.reply_text(f'ቁጥሩ ከ {low} እስከ {high} መሆን አለበት።'); return
    pool=await db()
    async with pool.acquire() as conn:
        lottery=await get_setting('lottery_id',DEFAULT_LOTTERY_ID)
        active=await conn.fetchval("SELECT EXISTS(SELECT 1 FROM allocations a JOIN tickets t ON t.id=a.ticket_id WHERE a.lottery_id=$1 AND a.number=$2 AND t.status IN ('reserved','paid') AND (t.status='paid' OR t.expires_at>NOW()))",lottery,n)
        if active: await update.effective_message.reply_text('ይህ ቁጥር በማስያዣ ወይም በተሸጠ ዕጣ ስር ነው።'); return
        await conn.execute('INSERT INTO blocked_numbers(lottery_id,number,blocked_by) VALUES($1,$2,$3) ON CONFLICT(lottery_id,number) DO NOTHING',lottery,n,ADMIN_ID)
    await update.effective_message.reply_text(f'ቁጥር {n:04d} ታግዷል።')
async def unblock_number_command(update,context):
    if not await admin_required(update): return
    if len(context.args)!=1 or not context.args[0].isdigit(): await update.effective_message.reply_text('አጠቃቀም፦ /unblocknumber NUMBER'); return
    pool=await db()
    async with pool.acquire() as conn:
        lottery=await get_setting('lottery_id',DEFAULT_LOTTERY_ID); res=await conn.execute('DELETE FROM blocked_numbers WHERE lottery_id=$1 AND number=$2',lottery,int(context.args[0]))
    await update.effective_message.reply_text('ቁጥሩ ተከፍቷል።' if res.endswith('1') else 'ቁጥሩ አልታገደም።')
async def blocked_numbers_command(update,context):
    if not await admin_required(update): return
    pool=await db()
    async with pool.acquire() as conn:
        lottery=await get_setting('lottery_id',DEFAULT_LOTTERY_ID); rows=await conn.fetch('SELECT number FROM blocked_numbers WHERE lottery_id=$1 ORDER BY number LIMIT 100',lottery)
    await update.effective_message.reply_text('የታገዱ ቁጥሮች፦ '+(', '.join(f"{r['number']:04d}" for r in rows) if rows else 'የሉም'))
async def pending_payments_command(update,context):
    if not await admin_required(update): return
    pool=await db()
    async with pool.acquire() as conn: rows=await conn.fetch("SELECT p.id,p.ticket_id,p.user_id,p.amount,t.number FROM payments p JOIN tickets t ON t.id=p.ticket_id WHERE p.status='pending' ORDER BY p.created_at LIMIT 20")
    await update.effective_message.reply_text('🧾 በመጠባበቅ ላይ ያሉ ክፍያዎች፦\n'+('\n'.join(f"#{r['id']} | ዕጣ #{r['ticket_id']} | ቁጥር {r['number']:04d} | User {r['user_id']} | {money(r['amount'])}" for r in rows) if rows else 'የሉም'),reply_markup=ADMIN_MENU)
async def expire_reservations(context):
    pool=await db()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE tickets t SET status='expired' WHERE t.status='reserved' AND t.expires_at<NOW() AND NOT EXISTS(SELECT 1 FROM payments p WHERE p.ticket_id=t.id AND p.status='pending')")
        await conn.execute("DELETE FROM allocations a USING tickets t WHERE a.ticket_id=t.id AND t.status IN ('expired','cancelled')")
async def contact_handler(update,context):
    if not await register_telegram_user(update): await update.effective_message.reply_text('🚫 መለያዎ ታግዷል።'); return
    contact=update.effective_message.contact
    if contact.user_id and contact.user_id!=update.effective_user.id: await update.effective_message.reply_text('እባክዎ የራስዎን ስልክ ቁጥር ያጋሩ።'); return
    await save_phone(update,context,contact.phone_number)
async def text_router(update,context):
    text=(update.effective_message.text or '').strip()
    if context.user_data.get('registration_step')=='phone': await save_phone(update,context,text); return
    actions={'🎟 ዕጣ ግዛ':buy_ticket,'🎫 ዕጣዎቼ':my_tickets,'💳 የክፍያ ታሪክ':payment_history,'ℹ️ የክፍያ መረጃ':payment_info,'❓ እገዛ':help_command,'📊 Admin Dashboard':admin_dashboard,'⚙️ Settings':settings_menu,'👥 Users':users_command,'🧾 Pending Payments':pending_payments_command}
    if text in actions: await actions[text](update,context)
    elif text=='🚫 Block Number': await update.effective_message.reply_text('አጠቃቀም፦ /blocknumber NUMBER')
    elif text=='✅ Unblock Number': await update.effective_message.reply_text('አጠቃቀም፦ /unblocknumber NUMBER')
    elif text=='⬅️ Main Menu': await update.effective_message.reply_text('ዋና ምናሌ፦',reply_markup=ADMIN_MENU if await is_admin(update) else MAIN_MENU)
    elif context.user_data.get('awaiting_number'):
        if not text.isdigit(): await update.effective_message.reply_text('ቁጥር ብቻ ያስገቡ።'); return
        if not await ensure_access(update,context): return
        await create_reservation(update,context,int(text))
    else:
        if not await ensure_access(update,context): return
        await update.effective_message.reply_text('ከምናሌው ይምረጡ ወይም /help ይጠቀሙ።',reply_markup=MAIN_MENU)
async def post_init(application):
    await init_db()
    if application.job_queue: application.job_queue.run_repeating(expire_reservations,interval=60,first=60)
    log.info('Bot is ready')
async def post_shutdown(application):
    global POOL
    if POOL: await POOL.close(); POOL=None
async def health_check(request): return web.Response(text='Sheger Lottery Bot is running',status=200)
async def start_web_server():
    app=web.Application(); app.router.add_get('/',health_check); app.router.add_get('/health',health_check)
    runner=web.AppRunner(app); await runner.setup(); port=int(os.getenv('PORT','10000')); site=web.TCPSite(runner,'0.0.0.0',port); await site.start(); log.info('Render health server listening on %s',port)
async def run_bot():
    app=(Application.builder().token(TOKEN).build())
    app.add_handler(CommandHandler('start',start)); app.add_handler(CommandHandler('help',help_command)); app.add_handler(CommandHandler('admin',admin_command)); app.add_handler(CommandHandler('settings',settings_menu)); app.add_handler(CommandHandler('set',set_command)); app.add_handler(CommandHandler('users',users_command)); app.add_handler(CommandHandler('blockuser',block_user_command)); app.add_handler(CommandHandler('unblockuser',unblock_user_command)); app.add_handler(CommandHandler('blocknumber',block_number_command)); app.add_handler(CommandHandler('unblocknumber',unblock_number_command)); app.add_handler(CommandHandler('blockednumbers',blocked_numbers_command)); app.add_handler(CommandHandler('pendingpayments',pending_payments_command))
    app.add_handler(CallbackQueryHandler(buy_type_callback,pattern=r'^buy:(full|half)$')); app.add_handler(CallbackQueryHandler(payment_review_callback,pattern=r'^pay:(approve|reject):\d+$'))
    app.add_handler(MessageHandler(filters.CONTACT,contact_handler)); app.add_handler(MessageHandler(filters.PHOTO|filters.Document.IMAGE,handle_receipt)); app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,text_router))
    try:
        await app.initialize(); await post_init(app); await app.start()
        if not app.updater: raise RuntimeError('Telegram updater unavailable')
        await app.updater.start_polling(allowed_updates=Update.ALL_TYPES); log.info('Telegram polling started'); await asyncio.Event().wait()
    finally:
        if app.updater and app.updater.running: await app.updater.stop()
        if app.running: await app.stop()
        await post_shutdown(app); await app.shutdown()
async def main_async(): await asyncio.gather(start_web_server(),run_bot())
if __name__=='__main__': asyncio.run(main_async())
