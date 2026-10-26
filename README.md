# Telegram Lottery Bot — PostgreSQL + Render

A starter Telegram bot using `python-telegram-bot` and PostgreSQL. It supports:

- User registration and administrator-only controls
- Full tickets (reserve the whole number) and half tickets (two independently allocatable shares)
- Temporary reservations with expiry
- Receipt photo/image upload and administrator approve/reject buttons
- My Tickets and payment history
- Admin dashboard, editable settings, user block/unblock, number block/unblock
- PostgreSQL storage suitable for a Render background worker

## Important before using with real payments

This is a software starter, not a certification that your lottery operation is legal or payment-compliant. Confirm local laws, licensing, age limits, consumer-protection rules, and payment-provider terms in every place where you operate. Never use a receipt image alone as proof of cleared funds; verify payments in your payment provider/bank records. Test the bot with test data before taking real money.

## Files

- `main.py` — bot application and database schema
- `requirements.txt` — Python dependencies
- `render.yaml` — Render Blueprint for a background worker and PostgreSQL database
- `.env.example` — environment variable template

## 1. Create the Telegram bot

1. Open Telegram and talk to `@BotFather`.
2. Use `/newbot`, follow its steps, and copy the token.
3. Get your numeric Telegram user ID from a trusted ID bot or Telegram tooling.
4. Keep both values private. Do not paste the token into GitHub or this chat.

## 2. Deploy to Render using Blueprint

1. Put these files in a GitHub repository.
2. In Render, create a **Blueprint** and connect the repository.
3. Review `render.yaml`. It defines a background worker and a PostgreSQL database.
4. Enter `TELEGRAM_BOT_TOKEN` and `TELEGRAM_ADMIN_ID` when Render asks for secret environment values.
5. Deploy and check the worker logs.

Render plan names and database availability can change. If the Blueprint rejects a plan name, select a currently supported paid worker/database plan in Render and update the YAML to match. Do not rely on an ephemeral filesystem for database storage; this project uses PostgreSQL instead.

## 3. Required environment variables

| Variable | Purpose |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Token from BotFather |
| `TELEGRAM_ADMIN_ID` | Numeric Telegram ID of the sole configured super-admin |
| `DATABASE_URL` | PostgreSQL connection string; Render Blueprint injects it from the database |
| `CURRENCY` | Display currency, default `ETB` |
| `LOTTERY_ID` | Initial lottery ID, default `main` |
| `LOG_LEVEL` | Optional logging level, default `INFO` |

Never commit `.env` or a real bot token. Rotate the token immediately if it leaks.

## 4. Local development

Python 3.11+ recommended.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt
```

Set the environment variables in your shell or a local `.env` loader of your choice. The app intentionally does not load `.env` automatically.

```bash
export TELEGRAM_BOT_TOKEN="your-token"
export TELEGRAM_ADMIN_ID="your-numeric-user-id"
export DATABASE_URL="postgresql://user:password@localhost:5432/telegram_lottery"
python main.py
```

## 5. User workflow

1. User taps **🎟 Buy Ticket**.
2. User chooses full or half ticket.
3. User sends a number in the configured range.
4. Bot reserves the number/share and shows the amount and payment instructions.
5. User uploads a receipt as a photo or image document.
6. The administrator receives the receipt with **Approve** and **Reject** buttons.
7. On approval, payment becomes approved and the ticket becomes paid. On rejection, the user is notified and may submit a corrected receipt while the reservation remains active.

A full ticket blocks both halves of a number. Two half tickets can occupy a number, one for each share. A full ticket cannot be allocated while either half is occupied, and halves cannot be allocated after a full ticket is reserved or sold.

## 6. Admin controls

The configured `TELEGRAM_ADMIN_ID` is the super-admin. Use these commands in Telegram:

- `/settings` — view settings and examples
- `/set full_price 100`
- `/set half_price 50`
- `/set reservation_minutes 30`
- `/set lottery_name My Lottery`
- `/set lottery_id main`
- `/set number_min 0`
- `/set number_max 9999`
- `/set sales_open true` or `/set sales_open false`
- `/set payment_instructions Your payment instructions`
- `/users` — recent users
- `/blockuser USER_ID`
- `/unblockuser USER_ID`
- `/blocknumber NUMBER`
- `/unblocknumber NUMBER`
- `/blockednumbers`
- `/pendingpayments`

**Caution:** changing `lottery_id` changes which lottery's number pool is used. Do not change it mid-draw unless you intentionally want a separate pool.

## 7. Operational notes / limitations

- PostgreSQL transactions and advisory locks protect concurrent number allocation.
- Pending payment receipts keep a reservation from expiring automatically so an admin can review the receipt. Review pending receipts promptly.
- Only one configured super-admin is supported in this version. Additional admin roles and audit logs can be added before production.
- This bot uses Telegram long polling; deploy it as a Render **Background Worker**, not a web service.
- Make database backups and test a restore process.
- Before production, add automated tests for reservation races, payment approval/rejection, expiry, and blocked users/numbers. Test callback handling and receipt delivery with a private test bot first.
