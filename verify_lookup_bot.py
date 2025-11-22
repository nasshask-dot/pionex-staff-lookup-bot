#!/usr/bin/env python3
# verify_lookup_bot.py
"""
Pionex Staff Lookup Bot - Buttons removed, 'another' supported by typing.
Full replacement file - paste into your project and restart the bot.
"""

import os
import csv
import re
import random
import time
from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime, timezone
from difflib import SequenceMatcher

from aiogram import Bot, Dispatcher, types, executor

# ----- CONFIG -----
TG_TOKEN = os.getenv("TG_TOKEN")
if not TG_TOKEN:
    raise RuntimeError("TG_TOKEN environment variable is required")

DATA_PATH = os.getenv("DATA_PATH", "pionex_staff.csv")
NOT_FOUND_LOG = os.getenv("NOT_FOUND_LOG", "not_found_log.csv")
LOOKUP_LOG = os.getenv("LOOKUP_LOG", "lookup_log.csv")
VERIFY_LOG = os.getenv("VERIFY_LOG", "verify_bot.log")
ADMIN_IDS = set(int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip())

RATE_LIMIT_COUNT = int(os.getenv("RATE_LIMIT_COUNT", "6"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))

FUZZY_MATCH_CUTOFF = float(os.getenv("FUZZY_MATCH_CUTOFF", "0.6"))
FUZZY_MAX_SUGGEST = int(os.getenv("FUZZY_MAX_SUGGEST", "5"))

# ----- BOT SETUP -----
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(bot)

DATA: List[Dict[str, str]] = []
LAST_RELOAD: Optional[datetime] = None

_rate_store: Dict[int, List[float]] = {}

# ----- GREETING DETECTION -----
GREETINGS = {
    "hi", "hello", "hey", "hiya", "good morning", "good afternoon", "good evening", "gm", "hey there",
    "مرحبا", "اهلا", "أهلا", "السلام عليكم", "سلام", "صباح الخير", "مساء الخير",
    "salam", "salaam",
}
GREETINGS_RE = re.compile(r"^\s*(?:{})[\s!,.؟?]*$".format("|".join(re.escape(g) for g in GREETINGS)), flags=re.IGNORECASE)

def is_greeting(text: Optional[str]) -> bool:
    if not text:
        return False
    t = text.strip()
    if GREETINGS_RE.match(t):
        return True
    tokens = t.split()
    if tokens and tokens[0].lower() in GREETINGS and len(tokens) <= 3:
        return True
    return False

# ----- UTIL: LOGGING -----
def _append_csv_log(path: str, header: List[str], row: List[Any]) -> None:
    header_needed = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if header_needed:
            writer.writerow(header)
        writer.writerow([str(x) if x is not None else "" for x in row])

def log_not_found(raw_input: str, query_type: str, value: str, user_id: int, user_name: Optional[str]) -> None:
    _append_csv_log(NOT_FOUND_LOG, ["timestamp", "user_id", "user_name", "raw_input", "query_type", "value"],
                    [datetime.now(timezone.utc).isoformat(), user_id, user_name or "", raw_input, query_type, value])

def log_lookup(found: bool, raw_input: str, query_type: str, value: str, user_id: int, user_name: Optional[str], matched_name: str = "") -> None:
    _append_csv_log(LOOKUP_LOG, ["timestamp", "user_id", "user_name", "raw_input", "query_type", "value", "found", "matched_name"],
                    [datetime.now(timezone.utc).isoformat(), user_id, user_name or "", raw_input, query_type, value, "1" if found else "0", matched_name])

# ----- CSV LOADER -----
def load_dataset(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    email_aliases = {"email", "e-mail", "email_address", "emailaddress", "mail", "work_email", "e_mail"}
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                row: Dict[str, str] = {}
                for orig_key, value in r.items():
                    if orig_key is None:
                        continue
                    clean_key = orig_key.strip().lower().lstrip("\ufeff").strip()
                    row[clean_key] = (value or "").strip()
                email_val = ""
                if row.get("email"):
                    email_val = row.get("email")
                else:
                    for alias in email_aliases:
                        if row.get(alias):
                            email_val = row.get(alias)
                            break
                if not email_val:
                    for k, v in row.items():
                        if "email" in k and v:
                            email_val = v
                            break
                row["email_norm"] = (email_val or "").lower().strip()
                row["tg_norm"] = row.get("tg_username", "").lower().lstrip("@").strip()
                row["x_norm"] = row.get("x_username", "").lower().lstrip("@").strip()
                row["full_name_norm"] = row.get("full_name", "").strip()
                rows.append(row)
    except FileNotFoundError:
        print("DATA FILE NOT FOUND:", path)
    except Exception as e:
        print(f"Error loading CSV data from {path}: {e}")
    return rows

def reload_data() -> None:
    global DATA, LAST_RELOAD
    DATA = load_dataset(DATA_PATH)
    LAST_RELOAD = datetime.now(timezone.utc)
    print(f"Loaded {len(DATA)} staff rows from {DATA_PATH} at {LAST_RELOAD.isoformat()}")

# ----- PARSE USER INPUT -----
def parse_query(text: str) -> Tuple[Optional[str], Optional[str]]:
    t = text.strip().strip("`<> ")

    # email
    m = re.search(r"(?:mailto:)?([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})", t, flags=re.IGNORECASE)
    if m:
        return "email_norm", m.group(1).lower()

    # x/twitter
    m = re.search(r"(?:https?://)?(?:www\.)?(?:x\.com|twitter\.com)/@?([A-Za-z0-9_]{1,15})(?:[/?#]|$)", t, flags=re.IGNORECASE)
    if m:
        return "x_norm", m.group(1).lower()

    # telegram
    m = re.search(r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/@?([A-Za-z0-9_]{3,64})(?:[/?#]|$)", t, flags=re.IGNORECASE)
    if m:
        return "tg_norm", m.group(1).lower()

    # plain handle
    m = re.match(r"^@?([A-Za-z0-9_]{1,64})$", t)
    if m:
        return "raw", m.group(1).lower()

    # name-like
    if re.match(r"^[A-Za-z\u00C0-\u024F0-9\-\s\.'`]{2,100}$", t):
        return "name", t.strip()

    # domain
    m = re.search(r"@?([A-Za-z0-9\.-]+\.[A-Za-z]{2,})$", t)
    if m:
        return "email_domain", m.group(1).lower()

    return None, None

# ----- FIND & FUZZY -----
def find_record(field: str, value: str) -> Optional[Dict[str, str]]:
    value = value.strip().lower()
    if field in ("email_norm", "tg_norm", "x_norm"):
        for r in DATA:
            if r.get(field, "").lower() == value:
                return r
        return None

    if field == "raw":
        for r in DATA:
            if r.get("tg_norm", "") == value or r.get("x_norm", "") == value or r.get("email_norm", "") == value:
                return r
        return None

    if field == "name":
        for r in DATA:
            if r.get("full_name_norm", "").lower() == value.lower():
                return r
        return None

    if field == "email_domain":
        for r in DATA:
            em = r.get("email_norm", "")
            if em and em.endswith("@" + value):
                return r
        return None

    return None

def fuzzy_name_suggestions(name_value: str, max_suggest: int = FUZZY_MAX_SUGGEST) -> List[Tuple[str, float]]:
    names = [r.get("full_name_norm", "") for r in DATA if r.get("full_name_norm")]
    scored: List[Tuple[str, float]] = []
    for n in set(names):
        ratio = SequenceMatcher(None, name_value.lower(), n.lower()).ratio()
        if ratio >= FUZZY_MATCH_CUTOFF:
            scored.append((n, ratio))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:max_suggest]

# ----- RATE LIMITING -----
def rate_allow(user_id: int) -> Tuple[bool, int]:
    now = time.time()
    lst = _rate_store.get(user_id, [])
    window_start = now - RATE_LIMIT_WINDOW
    lst = [t for t in lst if t > window_start]
    if len(lst) >= RATE_LIMIT_COUNT:
        retry_after = int(lst[0] + RATE_LIMIT_WINDOW - now) if lst else RATE_LIMIT_WINDOW
        _rate_store[user_id] = lst
        return False, max(1, retry_after)
    lst.append(now)
    _rate_store[user_id] = lst
    return True, 0

# ----- HELPERS -----
def welcome_msg() -> str:
    return ("Welcome to the Pionex Staff Lookup Bot\n\n"
            "Send an email (example@pionex.com), Telegram/X handle or link, or a full name. The bot will check our records and reply.")

def greeting_prompt(first_name: str) -> str:
    fn = first_name or "there"
    return f"Hi {fn}!\n\nCould you please send the username, link, or email you'd like me to verify if they work at Pionex?\n\nExamples:\n• @username\n• t.me/username\n• user@pionex.com"

# ----- HANDLERS -----
@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    await message.answer(welcome_msg())

@dp.message_handler(commands=["reload"])
async def cmd_reload(message: types.Message):
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        await message.answer("You are not authorized to run that command.")
        return
    try:
        reload_data()
        await message.answer(f"Reloaded dataset. {len(DATA)} rows loaded. (UTC {LAST_RELOAD.isoformat()})")
    except Exception as e:
        await message.answer(f"Failed to reload dataset: {e}")

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        await message.answer("You are not authorized to view stats.")
        return
    await message.answer(f"Rows loaded: {len(DATA)}\nData path: {DATA_PATH}\nLast reload (UTC): {LAST_RELOAD.isoformat() if LAST_RELOAD else 'never'}")

@dp.message_handler(commands=["logs"])
async def cmd_logs(message: types.Message):
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        await message.answer("Not authorized.")
        return
    parts = (message.text or "").split()
    n = 100
    if len(parts) > 1 and parts[1].isdigit():
        n = min(2000, int(parts[1]))
    if os.path.exists(VERIFY_LOG):
        with open(VERIFY_LOG, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()[-n:]
            await message.answer("Last %d lines of verify log:\n\n%s" % (len(lines), "".join(lines[-n:])))
    else:
        await message.answer("Log file not found: %s" % VERIFY_LOG)

@dp.message_handler(commands=["recent_notfound"])
async def cmd_recent_notfound(message: types.Message):
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        await message.answer("Not authorized.")
        return
    parts = (message.text or "").split()
    n = 50
    if len(parts) > 1 and parts[1].isdigit():
        n = min(1000, int(parts[1]))
    if os.path.exists(NOT_FOUND_LOG):
        with open(NOT_FOUND_LOG, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()[-n:]
            await message.answer("Last %d lines of not_found_log:\n\n%s" % (len(lines), "".join(lines[-n:])))
    else:
        await message.answer("not_found_log not present.")

@dp.message_handler()
async def handle_query(message: types.Message) -> None:
    raw = (message.text or "").strip()
    # rate limit
    allowed, retry = rate_allow(message.from_user.id)
    if not allowed:
        await message.answer(f"You're sending requests too fast. Try again in {retry} seconds.")
        return

    low = raw.lower().strip()
    # Accept typed "another" as the same action the former button did
    if low in ("another", "verify another", "verifyanother"):
        await message.answer("Sure — send the username, link, or email you'd like me to check.")
        return

    # Greeting
    if is_greeting(raw):
        first = (message.from_user.first_name or "").strip()
        await message.answer(greeting_prompt(first))
        return

    field, value = parse_query(raw)
    if not field:
        if len(raw.split()) <= 2:
            await message.answer(welcome_msg())
            return
        else:
            await message.answer("I couldn't understand that input. Send an email, a Telegram/X handle or link, or a full name.")
            return

    if not DATA:
        await message.answer("Error: Staff data list is empty or could not be loaded. Please ensure the CSV exists and try /reload.")
        return

    rec = find_record(field, value)

    if field == "name" and rec is None:
        suggestions = fuzzy_name_suggestions(value)
        if suggestions:
            suggestions_text = "\n".join(f"- {name}  (score {score:.2f})" for name, score in suggestions)
            log_lookup(False, raw, field, value, message.from_user.id, message.from_user.username or "", matched_name="")
            await message.answer("No exact match found. Close matches:\n\n" + suggestions_text + "\n\nIf one is correct, resend the exact name shown.")
            return

    if not rec:
        log_not_found(raw, field, value, message.from_user.id, message.from_user.username or "")
        log_lookup(False, raw, field, value, message.from_user.id, message.from_user.username or "", matched_name="")
        first = (message.from_user.first_name or message.from_user.username or "").strip() or "there"
        quotes = [
            "Keep going — small wins add up",
            "Progress over perfection — keep moving",
            "One step forward is still progress",
            "Stay focused and trust the process",
            "Do a little today that your future self will thank you for"
        ]
        quote = random.choice(quotes)
        preface = f"Hi {first}! {quote}\n\n"
        friendly_msg = (f"Sorry — I couldn't find {raw} in the official Pionex staff records.\n\n"
                        "Please double-check the username/link or email and try again.\nIf you want, resend the username or link and I'll check again.")
        await message.answer(preface + friendly_msg)
        return

    name = rec.get("full_name", "(no name)")
    job = rec.get("job_title", "(no job title)")
    works = rec.get("works_at_pionex", "").lower()
    status_active = works in ("yes", "y", "true", "1")
    status_header = "STAFF FOUND & ACTIVE" if status_active else "RECORD FOUND, BUT INACTIVE"
    status_text = "Active Pionex Staff" if status_active else "NOT currently active at Pionex"

    extra = []
    if rec.get("department"):
        extra.append(f"Department: {rec.get('department')}")
    if rec.get("location"):
        extra.append(f"Location: {rec.get('location')}")
    if rec.get("email"):
        extra.append(f"Email: {rec.get('email')}")
    if rec.get("tg_username"):
        extra.append(f"Telegram: @{rec.get('tg_username').lstrip('@')}")
    if rec.get("x_username"):
        extra.append(f"X: @{rec.get('x_username').lstrip('@')}")

    extra_text = "\n".join(extra)
    msg = (f"{status_header}\n\nName: {name}\nJob Title: {job}\nStatus: {status_text}\n\n")
    if extra_text:
        msg += extra_text

    first = (message.from_user.first_name or message.from_user.username or "").strip() or "there"
    motivational = [
        "Keep going — small wins add up",
        "Progress over perfection — keep moving",
        "One step forward is still progress",
        "Stay focused and trust the process",
        "Do a little today that your future self will thank you for"
    ]
    preface = f"Hi {first}! {random.choice(motivational)}\n\n"

    log_lookup(True, raw, field, value, message.from_user.id, message.from_user.username or "", matched_name=name)
    await message.answer(preface + msg)

# ----- STARTUP -----
async def on_startup(dp: Dispatcher) -> None:
    reload_data()
    print("Bot started. Admins:", ADMIN_IDS)

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
