import base64
import csv
import hashlib
import io
import json
import os
import re
import secrets
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager

import web_lead_finder
from leadgen_prompts import (
    DEFAULT_LLM_PROMPTS,
    effective_llm_prompts,
    format_llm_prompt,
    normalize_llm_prompts_for_save,
)
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from collections.abc import Set as AbstractSet
from typing import Any

import pyotp
import qrcode
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from flask import Flask, Response, abort, g, jsonify, redirect, render_template, request, send_file
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from itsdangerous import URLSafeTimedSerializer, BadData

BASE_DIR = Path(".")
CONFIG_PATH = BASE_DIR / "config.json"
LOG_PATH = BASE_DIR / "logs" / "bot.log"
BOT_SCRIPT = BASE_DIR / "telegram_leadgen_bot.py"
# Путь к SQLite: можно вынести из папки синхронизации (Яндекс.Диск и т.п.) — WAL там часто портит файл.
_db_path_raw = (os.getenv("DB_PATH", "") or "").strip()
DB_PATH = Path(_db_path_raw) if _db_path_raw else (BASE_DIR / "data.db")
TENANTS_DIR = BASE_DIR / "tenants"
SESSIONS_DIR = BASE_DIR / "sessions"
AVATARS_DIR = BASE_DIR / "data" / "avatars"
MAX_AVATAR_BYTES = 2 * 1024 * 1024

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "5173"))
ENV = str(os.getenv("ENV", "dev") or "dev").strip().lower()
_IS_HTTPS = str(os.getenv("HTTPS", "") or "").strip().lower() in ("1", "true", "yes", "on")
SESSION_TTL_HOURS = int(os.getenv("SESSION_TTL_HOURS", "72"))
# Сколько reverse-proxy «слёг» доверять для X-Forwarded-* (0 = отключено).
# На VPS за одним nginx ставьте 1. Не включайте на голом gunicorn без прокси —
# клиент сможет подделать IP и обходить rate limit.
_trusted_proxy_raw = (os.getenv("LEADGEN_TRUSTED_PROXY_COUNT") or os.getenv("TRUSTED_PROXY_COUNT") or "0").strip()
try:
    TRUSTED_PROXY_COUNT = max(0, min(5, int(_trusted_proxy_raw)))
except ValueError:
    TRUSTED_PROXY_COUNT = 0
# Secure cookie: production или явный HTTPS (за reverse-proxy).
COOKIE_SECURE = ENV == "prod" or _IS_HTTPS
CONSENT_POLICY_VERSION = "privacy_1.0"
MIN_PASSWORD_LEN = 12

# Роли в memberships: доступ к воронке и outreach / поиск.
_ORG_FUNNEL_ROLES: frozenset[str] = frozenset({"admin", "manager", "client", "tester"})
_ORG_STAFF_ROLES: frozenset[str] = frozenset({"admin", "manager", "tester"})
_ORG_DEBUG_LOG_ROLES: frozenset[str] = frozenset({"admin", "tester"})

# Жёсткие потолки поверх plans (защита сервера и Telegram API).
PLAN_ABS_MAX_CHATS = int(os.getenv("PLAN_ABS_MAX_CHATS", "1200"))
PLAN_ABS_MIN_MONITOR_INTERVAL_SEC = int(os.getenv("PLAN_ABS_MIN_MONITOR_INTERVAL_SEC", "8"))
PLAN_ABS_MAX_DM_DAY = int(os.getenv("PLAN_ABS_MAX_DM_DAY", "100"))
PLAN_ABS_MAX_DM_MONTH = int(os.getenv("PLAN_ABS_MAX_DM_MONTH", "3500"))

# Должны совпадать с telegram_leadgen_bot.CSV_FIELDNAMES
LEADS_CSV_FIELDNAMES: list[str] = [
    "timestamp",
    "username",
    "user_id",
    "source_chat",
    "message_id",
    "message",
    "stage",
    "status",
    "matched_keyword",
    "lead_tag",  # lead | junk | in_progress | wrote | partner
    "deleted",  # мягкое удаление: "1" = скрыто в UI, строка остаётся в файле
]

# Идентификатор строки не зависит от lead_tag и deleted (тег можно менять без смены id).
_LEADS_CSV_ID_KEYS: tuple[str, ...] = (
    "timestamp",
    "username",
    "user_id",
    "source_chat",
    "message_id",
    "message",
    "stage",
    "status",
    "matched_keyword",
)

ALLOWED_LEAD_TAGS: frozenset[str] = frozenset(
    {"", "lead", "junk", "in_progress", "wrote", "partner"}
)


def _csv_row_deleted(row: dict[str, Any]) -> bool:
    v = str(row.get("deleted") or "").strip().lower()
    return v in ("1", "true", "yes", "deleted")


def _lead_row_id(row: dict[str, Any]) -> str:
    """Стабильный id строки без учёта поля deleted (после soft-delete id не меняется)."""
    sep = "\u241e"
    parts = [str(row.get(k, "") or "") for k in _LEADS_CSV_ID_KEYS]
    return hashlib.sha256(sep.join(parts).encode("utf-8")).hexdigest()[:32]


def _leads_csv_backup(path: Path) -> Path | None:
    """Копия журнала перед массовыми операциями. Возвращает путь к .bak или None."""
    if not path.is_file():
        return None
    bak = path.parent / f"{path.name}.bak"
    try:
        shutil.copy2(path, bak)
    except OSError:
        return None
    return bak


# Пресеты для UI (OpenAI-совместимый /v1/chat/completions)
LLM_PRESETS: list[dict[str, Any]] = [
    {
        "id": "openai",
        "label": "OpenAI",
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-4o-mini",
    },
    {
        "id": "cerebras",
        "label": "Cerebras",
        "base_url": "https://api.cerebras.ai/v1",
        "model": "llama3.1-8b",
    },
    {
        "id": "groq",
        "label": "Groq",
        "base_url": "https://api.groq.com/openai/v1",
        "model": "llama-3.3-70b-versatile",
    },
    {
        "id": "ollama",
        "label": "Ollama (локально)",
        "base_url": "http://127.0.0.1:11434/v1",
        "model": "llama3.2",
    },
    {
        "id": "vsegpt",
        "label": "VseGPT (примеры URL — сверьте с лк)",
        "base_url": "https://api.vsegpt.ru/v1",
        "model": "gpt-4o-mini",
    },
]

# Подсказки для LLM при генерации групп keywords в настройках org
CONFIG_KEYWORD_GROUP_INSTRUCTIONS: dict[str, str] = {
    "hot_lead": "фразы-триггеры: как формулируют запрос потенциальные клиенты в чатах (ищут услугу, подрядчика, цену)",
    "required_intent_hot_lead": "короткие обязательные маркеры намерения (ищу, нужен, нужна, сколько стоит, кто сделает…)",
    "exclude_hot_lead": "типичные фразы конкурентов, спама и «продавцов в чужой чат» — чтобы не принимать их за лида",
    "negative": "стоп-слова отказа и агрессии в личке (не интересно, спам, не пиши…)",
    "qualification": "слова/фразы, по которым бот поймёт, что диалог можно углублять (бюджет, срок, детали, созвон…)",
    "interested": "короткие сигналы согласия и интереса (да, ок, удобно, давайте созвон…)",
    "bio_block": "фразы в описании профиля Telegram, по которым стоит не писать человеку (бот, реклама, не ЛС…)",
}

# Некоторые API за Cloudflare отбрасывают Python-urllib по умолчанию (403, код 1010). Можно задать свой UA.
# Пример: $env:LLM_USER_AGENT="python-requests/2.32.0" (PowerShell)
_LLM_UA = os.getenv(
    "LLM_USER_AGENT",
    "Mozilla/5.0 (compatible; OpenAI-Compatible-Client/1.0; Leadgen-LLM) Python/urllib",
)

app = Flask(__name__)
# Версия веб-консоли: можно переопределить LEADGEN_WEB_VERSION.
WEB_APP_VERSION = (os.getenv("LEADGEN_WEB_VERSION") or "0.1.0-beta").strip()
_FSK = os.getenv("FLASK_SECRET_KEY", "").strip()
if not _FSK:
    if ENV == "prod":
        # В проде стартовать с эфемерным ключом нельзя: после рестарта инвалидируются все сессии/CSRF,
        # а атакующий получает шанс на race-условия с подписями. Лучше явный отказ старта.
        raise RuntimeError(
            "FLASK_SECRET_KEY обязателен в prod. Сгенерируйте: python -c \"import secrets;print(secrets.token_hex(32))\""
        )
    _FSK = secrets.token_hex(32)
    print(
        "WARNING: FLASK_SECRET_KEY не задан, используется случайный ключ при старте (сессии/CSRF сбросятся при рестарте).",
        file=sys.stderr,
    )
app.config["SECRET_KEY"] = _FSK
app.config["JSON_SORT_KEYS"] = False
app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024  # 8 MiB на запрос (аватарки <= 2 MiB)
_pwd_hasher = PasswordHasher()
limiter = Limiter(app=app, key_func=get_remote_address, default_limits=[])
if TRUSTED_PROXY_COUNT > 0:
    from werkzeug.middleware.proxy_fix import ProxyFix

    app.wsgi_app = ProxyFix(
        app.wsgi_app,
        x_for=TRUSTED_PROXY_COUNT,
        x_proto=TRUSTED_PROXY_COUNT,
        x_host=TRUSTED_PROXY_COUNT,
        x_port=TRUSTED_PROXY_COUNT,
        x_prefix=TRUSTED_PROXY_COUNT,
    )
# Токен для CSRF: выдаётся в GET /api/auth/csrf, в запросы к API — в заголовке X-CSRF-Token.
_csrf_serializer = URLSafeTimedSerializer(_FSK, salt="leadgen-csrf-v1")
# Второй шаг входа 2FA: подписывает user_id на ~5 минут.
_totp_login_serializer = URLSafeTimedSerializer(_FSK, salt="leadgen-totp-step-v1")
bot_processes: dict[int, subprocess.Popen] = {}


@app.context_processor
def _inject_app_version() -> dict[str, Any]:
    return {"app_version": WEB_APP_VERSION}


@app.before_request
def _guard_pages() -> None:
    # Backend guard for HTML pages (not only UI hiding).
    # If not authenticated, allow only /auth, /healthz and static assets.
    p = request.path or "/"
    if p.startswith("/static/") or p in ("/healthz", "/api/auth/login", "/api/auth/register", "/api/auth/me"):
        return None
    if p == "/" or p == "/auth" or p.startswith("/legal/"):
        return None
    if p.startswith("/api/"):
        return None
    if _session_user() is None:
        return redirect("/auth")
    return None


def _api_path_requires_csrf() -> bool:
    m = (request.method or "GET").upper()
    if m not in ("POST", "PUT", "DELETE", "PATCH"):
        return False
    p = request.path or ""
    if not p.startswith("/api/"):
        return False
    if p in ("/api/auth/login", "/api/auth/login/totp", "/api/billing/webhook/yookassa"):
        return False
    return True


@app.before_request
def _csrf_guard() -> None:
    if not _api_path_requires_csrf():
        return None
    raw = (request.headers.get("X-CSRF-Token") or request.headers.get("X-Csrf-Token") or "").strip()
    if not raw:
        return jsonify({"message": "Требуется CSRF: заголовок X-CSRF-Token (получите токен в GET /api/auth/csrf)."}), 403
    try:
        _csrf_serializer.loads(raw, max_age=48 * 3600)
    except BadData:
        return jsonify({"message": "Недействительный или устаревший CSRF токен."}), 403
    return None


@app.errorhandler(429)
def _rate_limited(e: Exception) -> tuple[Any, int]:
    _ = e
    return jsonify({"message": "Слишком много запросов. Повторите позже."}), 429


# Минимальный, но рабочий CSP. Намеренно разрешаем 'unsafe-inline' для script,
# т.к. в base.html есть инлайновые блоки c bootstrap-кодом (window.* константы).
# img-src — data: для логотипов, blob: для аватаров. connect-src — 'self', чтобы
# фронт ходил только на свой backend.
_CSP_VALUE = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline'; "
    "style-src 'self' 'unsafe-inline'; "
    "img-src 'self' data: blob:; "
    "font-src 'self' data:; "
    "connect-src 'self'; "
    "frame-ancestors 'none'; "
    "base-uri 'self'; "
    "form-action 'self';"
)


@app.after_request
def _security_headers(resp: Any) -> Any:
    # CSP применяем только к HTML-ответам — для JSON/CSV он избыточен.
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "html" in ctype:
        resp.headers.setdefault("Content-Security-Policy", _CSP_VALUE)
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    resp.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    return resp


def _db() -> sqlite3.Connection:
    # 5 секунд ожидания блокировки лучше дефолтного 0 — SQLite не падает при коротких конкурентных пиках.
    conn = sqlite3.connect(DB_PATH, timeout=5.0)
    conn.row_factory = sqlite3.Row
    # Per-connection прагмы дешёвые, ускоряют работу с журналом и кешем.
    try:
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-20000")  # ~20 MiB на соединение
        conn.execute("PRAGMA foreign_keys=ON")
    except sqlite3.DatabaseError:
        pass
    return conn


def _sqlite_journal_mode() -> str:
    """WAL быстрее, но с облачной синхронизацией каталога проекта даёт «malformed». Тогда: SQLITE_JOURNAL_MODE=DELETE."""
    m = (os.getenv("SQLITE_JOURNAL_MODE", "WAL") or "WAL").strip().upper()
    if m not in ("WAL", "DELETE", "TRUNCATE", "MEMORY", "OFF"):
        return "WAL"
    return m


def _raise_db_corrupt_hint(exc: BaseException) -> None:
    p = Path(DB_PATH).resolve()
    raise RuntimeError(
        f"База SQLite повреждена или недоступна: {exc}\n"
        f"Файл: {p}\n\n"
        "Частая причина — проект в Яндекс.Диск / Dropbox / OneDrive: режим WAL и синхронизация портят data.db.\n\n"
        "Что сделать:\n"
        "1) Остановите приложение. Скопируйте data.db в безопасное место.\n"
        "2) Если рядом есть data.db-wal и data.db-shm — переименуйте их в .bak и попробуйте снова.\n"
        "3) Или переименуйте data.db в data.db.corrupt и запустите снова — создастся пустая БД "
        "(пользователи и лиды из старого файла не подтянутся без восстановления).\n"
        "4) Для облака: вынесите БД из синхронизируемой папки, например:\n"
        "   set DB_PATH=C:\\\\Users\\\\Вы\\\\AppData\\\\Local\\\\Leadgen\\\\data.db\n"
        "   и задайте SQLITE_JOURNAL_MODE=DELETE\n"
    ) from exc


def init_db() -> None:
    try:
        with _db() as conn:
            conn.execute(f"PRAGMA journal_mode={_sqlite_journal_mode()}")
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                created_at TEXT NOT NULL
            )
            """
            )
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
            """
            )
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orgs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
            )
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS memberships (
                user_id INTEGER NOT NULL,
                org_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(user_id, org_id),
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(org_id) REFERENCES orgs(id)
            )
            """
            )
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS plans (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                price_rub_month INTEGER NOT NULL,
                max_chats INTEGER NOT NULL,
                max_dm_day INTEGER NOT NULL,
                max_dm_month INTEGER NOT NULL,
                monitor_interval_min_sec INTEGER NOT NULL
            )
            """
            )
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                org_id INTEGER PRIMARY KEY,
                plan_id TEXT NOT NULL,
                status TEXT NOT NULL,
                renew_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                current_period_start TEXT,
                current_period_end TEXT,
                last_payment_id INTEGER,
                FOREIGN KEY(org_id) REFERENCES orgs(id),
                FOREIGN KEY(plan_id) REFERENCES plans(id)
            )
            """
            )
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_profiles (
                org_id INTEGER PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY(org_id) REFERENCES orgs(id)
            )
            """
            )

            user_cols = {row[1] for row in conn.execute("PRAGMA table_info(users)")}
            if "avatar_filename" not in user_cols:
                conn.execute("ALTER TABLE users ADD COLUMN avatar_filename TEXT")
            if "totp_secret" not in user_cols:
                conn.execute("ALTER TABLE users ADD COLUMN totp_secret TEXT")
            if "totp_enabled" not in user_cols:
                conn.execute("ALTER TABLE users ADD COLUMN totp_enabled INTEGER NOT NULL DEFAULT 0")
            session_cols = {row[1] for row in conn.execute("PRAGMA table_info(sessions)")}
            if "ip" not in session_cols:
                conn.execute("ALTER TABLE sessions ADD COLUMN ip TEXT")
            if "user_agent" not in session_cols:
                conn.execute("ALTER TABLE sessions ADD COLUMN user_agent TEXT")

            plan_cols = {row[1] for row in conn.execute("PRAGMA table_info(plans)")}
            if "max_telegram_accounts" not in plan_cols:
                conn.execute(
                    "ALTER TABLE plans ADD COLUMN max_telegram_accounts INTEGER NOT NULL DEFAULT 2"
                )
                conn.execute("UPDATE plans SET max_telegram_accounts = 1 WHERE id = 'free'")
                conn.execute("UPDATE plans SET max_telegram_accounts = 3 WHERE id = 'pro'")
                conn.execute("UPDATE plans SET max_telegram_accounts = 5 WHERE id = 'pro_plus'")

            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS consents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                version TEXT NOT NULL,
                kind TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
            """
            )
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS login_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                event TEXT NOT NULL,
                email TEXT,
                ip TEXT,
                user_agent TEXT,
                created_at TEXT NOT NULL
            )
            """
            )
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS outreach_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                source_chat TEXT NOT NULL,
                stage INTEGER NOT NULL,
                draft_text TEXT NOT NULL,
                task_hint TEXT NOT NULL DEFAULT '',
                lead_snippet TEXT NOT NULL DEFAULT '',
                trigger_match TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT,
                approved_at TEXT,
                conversation_id INTEGER
            )
            """
            )
            conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_outreach_org_status ON outreach_queue(org_id, status)"
            )
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id INTEGER NOT NULL,
                lead_username TEXT,
                lead_user_id TEXT,
                scheduled_at TEXT,
                duration_min INTEGER,
                outcome TEXT NOT NULL DEFAULT 'planned',
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT,
                conversation_id INTEGER
            )
            """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_org ON calls(org_id)")
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                plan_id TEXT NOT NULL,
                amount_rub_gross INTEGER NOT NULL,
                fee_rub INTEGER NOT NULL DEFAULT 0,
                amount_rub_net INTEGER NOT NULL,
                currency TEXT NOT NULL DEFAULT 'RUB',
                provider TEXT,
                provider_payment_id TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                paid_at TEXT
            )
            """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_org ON payments(org_id)")

            sub_cols = {row[1] for row in conn.execute("PRAGMA table_info(subscriptions)")}
            if "current_period_start" not in sub_cols:
                conn.execute("ALTER TABLE subscriptions ADD COLUMN current_period_start TEXT")
            if "current_period_end" not in sub_cols:
                conn.execute("ALTER TABLE subscriptions ADD COLUMN current_period_end TEXT")
            if "last_payment_id" not in sub_cols:
                conn.execute("ALTER TABLE subscriptions ADD COLUMN last_payment_id INTEGER")

            oq_cols = {row[1] for row in conn.execute("PRAGMA table_info(outreach_queue)")}
            if "conversation_id" not in oq_cols:
                conn.execute("ALTER TABLE outreach_queue ADD COLUMN conversation_id INTEGER")

            calls_cols = {row[1] for row in conn.execute("PRAGMA table_info(calls)")}
            if "conversation_id" not in calls_cols:
                conn.execute("ALTER TABLE calls ADD COLUMN conversation_id INTEGER")

            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id INTEGER NOT NULL,
                lead_user_id TEXT NOT NULL,
                lead_username TEXT,
                source_chat TEXT,
                status TEXT NOT NULL DEFAULT 'waiting_approval',
                history_json TEXT NOT NULL DEFAULT '[]',
                current_stage INTEGER NOT NULL DEFAULT 1,
                lead_snippet TEXT,
                trigger_match TEXT,
                outreach_queue_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT,
                FOREIGN KEY(org_id) REFERENCES orgs(id)
            )
            """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_conversations_org ON conversations(org_id)")
            conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_conversations_org_lead ON conversations(org_id, lead_user_id)"
            )

            conv_cols = {row[1] for row in conn.execute("PRAGMA table_info(conversations)")}
            if "last_activity_at" not in conv_cols:
                conn.execute("ALTER TABLE conversations ADD COLUMN last_activity_at TEXT")
            if "bot_user_id" not in conv_cols:
                conn.execute("ALTER TABLE conversations ADD COLUMN bot_user_id TEXT")

            # Таблица для мульти-воркер сценария: PID запущенного бота + heartbeat,
            # чтобы любой Gunicorn worker видел реальный статус, а не свой локальный dict.
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_runs (
                org_id INTEGER PRIMARY KEY,
                pid INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                last_heartbeat TEXT,
                phase TEXT,
                host TEXT,
                FOREIGN KEY(org_id) REFERENCES orgs(id)
            )
            """
            )

            # Лидоген по сайтам: найденные домены и контакты (см. web_lead_finder.py).
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS web_leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id INTEGER NOT NULL,
                domain TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'serp',
                query TEXT,
                url TEXT,
                title TEXT,
                http_status INTEGER NOT NULL DEFAULT 0,
                emails_json TEXT NOT NULL DEFAULT '[]',
                phones_json TEXT NOT NULL DEFAULT '[]',
                telegrams_json TEXT NOT NULL DEFAULT '[]',
                whatsapps_json TEXT NOT NULL DEFAULT '[]',
                vks_json TEXT NOT NULL DEFAULT '[]',
                pages_visited_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'new',
                last_error TEXT,
                conversation_id INTEGER,
                last_checked_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT,
                FOREIGN KEY(org_id) REFERENCES orgs(id)
            )
            """
            )
            conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_web_leads_org_domain ON web_leads(org_id, domain)"
            )
            conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_web_leads_org_status_upd ON web_leads(org_id, status, updated_at)"
            )

            # Очередь фоновых задач для парсинга сайтов.
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS web_lead_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT
            )
            """
            )
            conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_web_lead_jobs_status_id ON web_lead_jobs(status, id)"
            )
            conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_web_lead_jobs_org_status ON web_lead_jobs(org_id, status)"
            )

            # Пресеты настроек. kind='system' — глобальные, видны всем org'ам,
            # редактируются только платформенным admin'ом; kind='org' — приватные
            # для конкретной org, редактируются её admin'ом. `data_json` — снимок
            # настроек (части `config.json` + параметры поиска и пр.), который
            # пресет применяет/перезаписывает в config.json текущей org.
            conn.execute(
            """
            CREATE TABLE IF NOT EXISTS config_presets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL CHECK (kind IN ('system', 'org')),
                org_id INTEGER,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                data_json TEXT NOT NULL DEFAULT '{}',
                created_by INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
            )
            conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_config_presets_kind_org ON config_presets(kind, org_id)"
            )
            conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_config_presets_name "
            "ON config_presets(kind, COALESCE(org_id, -1), name)"
            )

            AVATARS_DIR.mkdir(parents=True, exist_ok=True)

            # Seed plans (random prices for now, as requested)
            conn.execute(
            """
            INSERT OR IGNORE INTO plans(id, title, price_rub_month, max_chats, max_dm_day, max_dm_month, monitor_interval_min_sec)
            VALUES
              ('free', 'Free', 0, 80, 8, 150, 45),
              ('pro', 'Pro', 1990, 400, 40, 900, 12),
              ('pro_plus', 'Pro+', 4990, 1200, 80, 3500, 10)
            """
            )

            # Ensure there is at least one org (single-tenant bootstrap)
            org = conn.execute("SELECT id FROM orgs ORDER BY id LIMIT 1").fetchone()
            if not org:
                conn.execute("INSERT INTO orgs(name, created_at) VALUES (?, ?)", ("Default", _now_iso()))
                org = conn.execute("SELECT id FROM orgs ORDER BY id LIMIT 1").fetchone()
            org_id = int(org["id"])

            sub = conn.execute("SELECT org_id FROM subscriptions WHERE org_id = ?", (org_id,)).fetchone()
            if not sub:
                conn.execute(
                    "INSERT INTO subscriptions(org_id, plan_id, status, renew_at, created_at) VALUES (?, ?, ?, ?, ?)",
                    (org_id, "free", "active", _now_iso(), _now_iso()),
                )
            bp = conn.execute("SELECT org_id FROM bot_profiles WHERE org_id = ?", (org_id,)).fetchone()
            if not bp:
                conn.execute(
                    "INSERT INTO bot_profiles(org_id, enabled, created_at) VALUES (?, ?, ?)",
                    (org_id, 0, _now_iso()),
                )

            # Индексы для hot-path. IF NOT EXISTS — идемпотентно при каждом старте.
            for stmt in (
                "CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)",
                "CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)",
                "CREATE INDEX IF NOT EXISTS idx_login_audit_user_ts ON login_audit(user_id, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_memberships_org ON memberships(org_id)",
                "CREATE INDEX IF NOT EXISTS idx_outreach_org_status ON outreach_queue(org_id, status)",
                "CREATE INDEX IF NOT EXISTS idx_calls_org_outcome ON calls(org_id, outcome)",
                "CREATE INDEX IF NOT EXISTS idx_conversations_org_status_upd ON conversations(org_id, status, updated_at)",
                "CREATE INDEX IF NOT EXISTS idx_payments_org_created ON payments(org_id, created_at)",
            ):
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass

            # Чистка просроченных сессий при старте — предотвращает бесконтрольный рост таблицы.
            try:
                conn.execute("DELETE FROM sessions WHERE expires_at < ?", (_now_iso(),))
            except sqlite3.OperationalError:
                pass
    except sqlite3.DatabaseError as exc:
        _raise_db_corrupt_hint(exc)


def _default_org_id() -> int:
    with _db() as conn:
        row = conn.execute("SELECT id FROM orgs ORDER BY id LIMIT 1").fetchone()
        return int(row["id"]) if row else 1


def _org_membership_role_for_user_table(role: str) -> str:
    """Роль в таблице memberships: client вместо user."""
    r = (role or "").strip()
    if r == "user":
        return "client"
    if r in ("admin", "manager", "client", "tester"):
        return r
    return "client"


def _normalize_manager_memberships(conn: sqlite3.Connection, user_id: int) -> int:
    """
    У менеджера в memberships ровно одна строка: (user_id, org_id, 'manager').
    При нескольких строках оставляем org, отличный от default (если есть), иначе default.
    """
    default_id = _default_org_id()
    rows = list(conn.execute("SELECT org_id FROM memberships WHERE user_id = ?", (user_id,)).fetchall())
    if not rows:
        conn.execute(
            "INSERT OR IGNORE INTO memberships(user_id, org_id, role, created_at) VALUES (?, ?, ?, ?)",
            (user_id, default_id, "manager", _now_iso()),
        )
        return default_id
    org_ids = [int(r["org_id"]) for r in rows]
    non_default = [oid for oid in org_ids if oid != default_id]
    keep = min(non_default) if non_default else min(org_ids)
    conn.execute("DELETE FROM memberships WHERE user_id = ? AND org_id != ?", (user_id, keep))
    conn.execute(
        "UPDATE memberships SET role = ? WHERE user_id = ? AND org_id = ?",
        ("manager", user_id, keep),
    )
    return keep


def _ensure_org_for_user(user_id: int) -> int:
    """
    Модель орг/ролей:
    - admin — только default org, роль в memberships: admin
    - manager — одна org (клиентская по «Назначить org» в админке либо default)
    - user (клиент) — своя org, роль в memberships: client
    """
    with _db() as conn:
        u = conn.execute("SELECT id, role, email FROM users WHERE id = ?", (user_id,)).fetchone()
        if not u:
            return _default_org_id()
        table_role = str(u["role"])
        mem_role = _org_membership_role_for_user_table(table_role)
        em = str(u["email"])
        m_count = int(
            conn.execute("SELECT COUNT(*) AS c FROM memberships WHERE user_id = ?", (user_id,)).fetchone()["c"]
        )
        if m_count > 0:
            if table_role == "admin":
                d = _default_org_id()
                conn.execute("DELETE FROM memberships WHERE user_id = ?", (user_id,))
                conn.execute(
                    "INSERT OR IGNORE INTO memberships(user_id, org_id, role, created_at) VALUES (?, ?, ?, ?)",
                    (user_id, d, "admin", _now_iso()),
                )
                return d
            if table_role == "manager":
                return _normalize_manager_memberships(conn, user_id)
            for row in conn.execute("SELECT org_id FROM memberships WHERE user_id = ?", (user_id,)).fetchall():
                oid = int(row["org_id"])
                cur_row = conn.execute(
                    "SELECT role FROM memberships WHERE user_id = ? AND org_id = ?",
                    (user_id, oid),
                ).fetchone()
                cur_role = str(cur_row["role"]) if cur_row else ""
                if cur_role == "tester":
                    continue
                conn.execute(
                    "UPDATE memberships SET role = ? WHERE user_id = ? AND org_id = ?",
                    (mem_role, user_id, oid),
                )
            row = conn.execute(
                "SELECT org_id FROM memberships WHERE user_id = ? LIMIT 1",
                (user_id,),
            ).fetchone()
            return int(row["org_id"]) if row else _default_org_id()
        if table_role == "admin":
            org_id = _default_org_id()
            conn.execute(
                "INSERT OR IGNORE INTO memberships(user_id, org_id, role, created_at) VALUES (?, ?, ?, ?)",
                (user_id, org_id, "admin", _now_iso()),
            )
            return org_id
        if table_role == "manager":
            org_id = _default_org_id()
            conn.execute(
                "INSERT OR IGNORE INTO memberships(user_id, org_id, role, created_at) VALUES (?, ?, ?, ?)",
                (user_id, org_id, "manager", _now_iso()),
            )
            return org_id
        org_name = f"{em.split('@')[0][:40] or 'Client'}"
        conn.execute("INSERT INTO orgs(name, created_at) VALUES (?, ?)", (org_name, _now_iso()))
        org_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        conn.execute(
            "INSERT OR IGNORE INTO memberships(user_id, org_id, role, created_at) VALUES (?, ?, ?, ?)",
            (user_id, org_id, "client", _now_iso()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO subscriptions(org_id, plan_id, status, renew_at, created_at) VALUES (?, ?, ?, ?, ?)",
            (org_id, "free", "active", _now_iso(), _now_iso()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO bot_profiles(org_id, enabled, created_at) VALUES (?, ?, ?)",
            (org_id, 0, _now_iso()),
        )
        return org_id


def _user_org_role(user_id: int) -> tuple[int, str]:
    org_id = _ensure_org_for_user(user_id)
    with _db() as conn:
        m = conn.execute(
            "SELECT role FROM memberships WHERE user_id = ? AND org_id = ?",
            (user_id, org_id),
        ).fetchone()
        if m:
            return org_id, str(m["role"])
        u = conn.execute("SELECT role FROM users WHERE id = ?", (user_id,)).fetchone()
        role = str(u["role"]) if u else "client"
        mapped = "admin" if role == "admin" else "client"
        conn.execute(
            "INSERT OR IGNORE INTO memberships(user_id, org_id, role, created_at) VALUES (?, ?, ?, ?)",
            (user_id, org_id, mapped, _now_iso()),
        )
        return org_id, mapped


def _migrate_sessions_from_root() -> None:
    """Переносит *.session / *.session-journal из корня проекта в каталог sessions/."""
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    for p in BASE_DIR.iterdir():
        if not p.is_file():
            continue
        if p.suffix != ".session" and not p.name.endswith(".session-journal"):
            continue
        dest = SESSIONS_DIR / p.name
        if dest.exists():
            continue
        try:
            shutil.move(str(p), str(dest))
        except OSError:
            pass


def _tenant_paths(org_id: int) -> dict[str, Path]:
    base = TENANTS_DIR / f"org_{org_id}"
    (base / "logs").mkdir(parents=True, exist_ok=True)
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    session_stem = f"leadgen_org_{org_id}_session"
    return {
        "base": base,
        "config": base / "config.json",
        "state": base / "state.json",
        "csv": base / "sent_leads.csv",
        "log": base / "logs" / "bot.log",
        "session_file": SESSIONS_DIR / session_stem,
    }


# Несколько Telegram-аккаунтов на org: `telegram_accounts` + `active_telegram_account` в config.json
# Верхняя граница независимо от тарифа (защита от ошибок в БД).
TELEGRAM_ACCOUNTS_ABS_MAX = 10


def _default_telegram_session_stem(org_id: int) -> str:
    return f"leadgen_org_{int(org_id)}_session"


def _telegram_account_session_stem(org_id: int, account_id: str) -> str:
    aid = (account_id or "default").strip() or "default"
    if aid in ("default", "main", "primary"):
        return _default_telegram_session_stem(org_id)
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", aid).strip("_")
    if not safe:
        safe = "acc"
    if len(safe) > 48:
        safe = safe[:48]
    return f"leadgen_org_{int(org_id)}_acc_{safe}"


def _stem_from_legacy_session_name(session_name: str, org_id: int) -> str:
    s = (session_name or "").strip()
    if not s:
        return _default_telegram_session_stem(org_id)
    p = Path(s)
    name = p.name
    if name.endswith(".session"):
        name = name[: -8]
    if not name or name in (".", ".."):
        return _default_telegram_session_stem(org_id)
    return name


def _in_memory_telegram_accounts_migrate(cfg: dict[str, Any], org_id: int) -> None:
    """Дополняет cfg списком аккаунтов, если в файле ещё только плоские api_id/phone (без записи на диск)."""
    if isinstance(cfg.get("telegram_accounts"), list) and len(cfg["telegram_accounts"]) > 0:
        for a in cfg["telegram_accounts"]:
            if not isinstance(a, dict):
                continue
            if not str(a.get("session_stem") or "").strip():
                aid = str(a.get("id") or "default").strip() or "default"
                a["session_stem"] = _telegram_account_session_stem(org_id, aid)
        if str(cfg.get("active_telegram_account") or "").strip() == "" and cfg["telegram_accounts"]:
            first = cfg["telegram_accounts"][0]
            if isinstance(first, dict) and str(first.get("id") or "").strip():
                cfg["active_telegram_account"] = str(first["id"]).strip()
        return
    stem = _stem_from_legacy_session_name(str(cfg.get("session_name") or ""), org_id)
    cfg["telegram_accounts"] = [
        {
            "id": "default",
            "label": "Основной",
            "api_id": cfg.get("api_id"),
            "api_hash": cfg.get("api_hash", "") or "",
            "phone": str(cfg.get("phone") or ""),
            "session_stem": stem,
        }
    ]
    cfg["active_telegram_account"] = "default"


def _sync_root_telegram_from_active(cfg: dict[str, Any], org_id: int) -> None:
    """Копия полей активного аккаунта в корень (совместимость с ботом и старым UI)."""
    accs = cfg.get("telegram_accounts")
    if not isinstance(accs, list) or not accs:
        return
    active = str(cfg.get("active_telegram_account") or "default").strip() or "default"
    chosen: dict[str, Any] | None = None
    for a in accs:
        if isinstance(a, dict) and str(a.get("id", "")).strip() == active:
            chosen = a
            break
    if chosen is None:
        for a in accs:
            if isinstance(a, dict):
                chosen = a
                cfg["active_telegram_account"] = str(chosen.get("id", "default")).strip() or "default"
                break
    if not isinstance(chosen, dict):
        return
    aid = str(chosen.get("id", "default")).strip() or "default"
    if not str(chosen.get("session_stem") or "").strip():
        chosen["session_stem"] = _telegram_account_session_stem(org_id, aid)
    stem = str(chosen.get("session_stem") or "").strip()
    cfg["api_id"] = chosen.get("api_id")
    cfg["api_hash"] = chosen.get("api_hash", "") or ""
    cfg["phone"] = str(chosen.get("phone") or "")
    cfg["session_name"] = stem


def _normalize_telegram_accounts_on_save(cfg: dict[str, Any], org_id: int) -> str | None:
    _in_memory_telegram_accounts_migrate(cfg, org_id)
    accs_in = cfg.get("telegram_accounts")
    accs: list[dict[str, Any]] = []
    if isinstance(accs_in, list):
        for a in accs_in:
            if isinstance(a, dict) and str(a.get("id", "")).strip():
                accs.append(dict(a))
    if not accs:
        accs = [
            {
                "id": "default",
                "label": "Основной",
                "api_id": cfg.get("api_id"),
                "api_hash": str(cfg.get("api_hash", "") or ""),
                "phone": str(cfg.get("phone") or ""),
            }
        ]
    cap = _effective_max_telegram_accounts(org_id)
    if len(accs) > cap:
        return f"По тарифу разрешено не более {cap} Telegram-аккаунтов (см. блок «Тариф» в настройках)"
    seen: set[str] = set()
    for a in accs:
        tid = str(a.get("id", "")).strip()
        if not tid:
            return "У каждого Telegram-аккаунта должен быть непустой id (латиница, цифры, _-)"
        if tid in seen:
            return f"Дублируется id Telegram-аккаунта: {tid}"
        seen.add(tid)
    active = str(cfg.get("active_telegram_account") or "").strip() or "default"
    if active not in seen:
        active = str(accs[0].get("id", "default")).strip() or "default"
    for a in accs:
        tid = str(a.get("id", "")).strip()
        a["session_stem"] = str(a.get("session_stem") or "").strip() or _telegram_account_session_stem(
            org_id, tid
        )
    cfg["telegram_accounts"] = accs
    cfg["active_telegram_account"] = active
    _sync_root_telegram_from_active(cfg, org_id)
    return None


def _session_path_for_telegram_account(
    org_id: int, cfg: dict[str, Any], account_id: str | None
) -> Path:
    _in_memory_telegram_accounts_migrate(cfg, org_id)
    target = (account_id or cfg.get("active_telegram_account") or "default")
    if not isinstance(target, str):
        target = "default"
    target = str(target).strip() or "default"
    accs = cfg.get("telegram_accounts")
    if isinstance(accs, list):
        for a in accs:
            if not isinstance(a, dict):
                continue
            if str(a.get("id", "")).strip() == target:
                stem = str(a.get("session_stem") or "").strip() or _telegram_account_session_stem(
                    org_id, target
                )
                return SESSIONS_DIR / stem
    return _tenant_paths(org_id)["session_file"]


def _bootstrap_tenant_config_from_root(dest: Path) -> None:
    """Копия шаблона для новой org: без чужих api_id/api_hash и без LLM-ключа (приватность)."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError("Корневой config.json не найден")
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    cfg["api_id"] = ""
    cfg["api_hash"] = ""
    cfg.setdefault(
        "llm",
        {
            "enabled": False,
            "api_key": "",
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-4o-mini",
        },
    )
    if isinstance(cfg.get("llm"), dict):
        cfg["llm"]["api_key"] = ""
    # Бета: по умолчанию не шлём DM, пока не отключат dry_run явительно.
    cfg["dry_run"] = True
    cfg.setdefault("human_approval_for_dm", False)
    cfg.setdefault(
        "human_approval_stages",
        {"stage1": True, "stage2": True, "stage3": True},
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_llm_model_id(base_url: str, model: str) -> str:
    """
    Сопоставление устаревших/ошибочных имён с актуальными id у конкретных провайдеров
    (модельные сады часто меняют список).
    """
    b = (base_url or "").lower()
    m = (model or "").strip()
    if not m:
        return m
    # Groq: нужен полный id с суффиксом, не «llama-3.3-70b».
    if "groq.com" in b:
        groq = {
            "llama-3.3-70b": "llama-3.3-70b-versatile",
            "llama3.3-70b": "llama-3.3-70b-versatile",
        }
        return groq.get(m.lower(), m)
    # Cerebras: публичный API больше не обслуживает llama-3.3-70b; см. models/overview.
    if "cerebras.ai" in b:
        cerebras = {
            "llama-3.3-70b": "gpt-oss-120b",
            "llama-3.3-70b-versatile": "gpt-oss-120b",
        }
        return cerebras.get(m.lower(), m)
    return m


def _llm_http_error_hint(status: int, detail: str) -> str:
    """Короткая подсказка по типичным ответам провайдеров (Cloudflare, регион и т.д.)."""
    s = (detail or "").lower()
    if status == 403 and "1010" in detail:
        return (
            " [Подсказка] Код 1010 обычно означает блокировку Cloudflare (клиент не прошёл проверку). "
            "Перезапустите веб‑приложение после обновления кода (добавлены заголовки User-Agent). "
            "Если 403 остаётся: смените LLM‑провайдера (например Groq, base_url https://api.groq.com/openai/v1) "
            "или задайте переменную LLM_USER_AGENT. Отключите VPN/прокси, если трафик идёт через «серую» сеть."
        )
    if status == 403 and (
        "country" in s
        or "region" in s
        or "forbidden" in s
        or "not supported" in s
        or "permission" in s
    ):
        return (
            " [Подсказка] 403: проверьте API key, регион/страну для провайдера и что ключ выдан в том же "
            "проекте, куда смотрит billing. Для Google Gemini: ai.google.dev — список регионов."
        )
    if status == 401:
        return " [Подсказка] Проверьте API key в «Настройки» → «Подключения» (скопирован без пробелов, ключ не отозван)."
    if status == 404 and "model" in s and ("not_found" in s or "does not exist" in s):
        return (
            " [Подсказка] Модель с таким id у провайдера не найдена. "
            "Для Cerebras (api.cerebras.ai) в 2026 г. в продакшене, например: gpt-oss-120b, llama3.1-8b "
            "(см. inference-docs.cerebras.ai/models/overview). "
            "Для Groq: llama-3.3-70b-versatile. Обновите поле «Модель» в «Настройки» → «Подключения» и сохраните."
        )
    return ""


def _openai_chat_completion(
    base_url: str,
    api_key: str,
    model: str,
    system: str,
    user: str,
    *,
    temperature: float = 0.7,
) -> str:
    model = _normalize_llm_model_id(base_url, model)
    url = base_url.rstrip("/") + "/chat/completions"
    payload = json.dumps(
        {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": float(temperature),
        }
    ).encode("utf-8")
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    if _LLM_UA:
        req.add_header("User-Agent", _LLM_UA)
    req.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        hint = _llm_http_error_hint(int(exc.code), detail)
        raise ValueError(f"LLM HTTP {exc.code}: {detail[:800]}{hint}") from exc
    try:
        return str(raw["choices"][0]["message"]["content"]).strip()
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError(f"Неожиданный ответ LLM: {raw!r}") from exc


def _conversation_history_load(raw: str) -> list[dict[str, Any]]:
    try:
        h = json.loads(raw or "[]")
    except Exception:
        return []
    return h if isinstance(h, list) else []


def _conversation_history_save(h: list[dict[str, Any]]) -> str:
    return json.dumps(h, ensure_ascii=False)


ALLOWED_CONVERSATION_STATUS = frozenset(
    {
        "waiting_approval",
        "active",
        "sent",
        "replied",
        "ignored",
        "qualified",
        "scheduled",
        "won",
        "dead",
    }
)


def _conv_propagate_call_outcome(conn: sqlite3.Connection, *, conversation_id: int, org_id: int, outcome: str) -> None:
    """При сохранении исхода созвона — обновляем статус conversation.

    Маппинг outcome → status:
      planned/scheduled → 'scheduled'
      done             → 'qualified'
      won              → 'won'
      lost/declined    → 'dead'
    Всё остальное игнорируется."""
    if not conversation_id:
        return
    out = (outcome or "").strip().lower()
    mapping = {
        "planned": "scheduled",
        "scheduled": "scheduled",
        "done": "qualified",
        "won": "won",
        "lost": "dead",
        "declined": "dead",
        "rejected": "dead",
    }
    new_status = mapping.get(out)
    if not new_status:
        return
    ts = _now_iso()
    try:
        cur_row = conn.execute(
            "SELECT history_json, status FROM conversations WHERE id = ? AND org_id = ?",
            (int(conversation_id), int(org_id)),
        ).fetchone()
        if not cur_row:
            return
        try:
            hist = json.loads(str(cur_row["history_json"] or "[]"))
        except Exception:  # noqa: BLE001
            hist = []
        if not isinstance(hist, list):
            hist = []
        hist.append(
            {
                "role": "system",
                "source": "call_outcome",
                "text": f"Исход созвона зафиксирован: {out} → статус CRM «{new_status}»",
                "at": ts,
            }
        )
        conn.execute(
            "UPDATE conversations SET status = ?, history_json = ?, updated_at = ?, last_activity_at = ? "
            "WHERE id = ? AND org_id = ?",
            (
                new_status,
                json.dumps(hist, ensure_ascii=False),
                ts,
                ts,
                int(conversation_id),
                int(org_id),
            ),
        )
    except sqlite3.Error as exc:
        print(f"WARNING: conversation propagate failed: {exc}", file=sys.stderr)


def _bot_run_register(org_id: int, pid: int, *, phase: str = "starting") -> None:
    """Сохраняет PID запущенного бота в БД (см. bot_runs).

    Нужен для мульти-воркер сценария: dict в памяти Flask виден только своему worker'у,
    а bot_runs — общий для всех."""
    ts = _now_iso()
    try:
        host = os.uname().nodename if hasattr(os, "uname") else os.getenv("COMPUTERNAME", "")
    except Exception:
        host = ""
    with _db() as conn:
        conn.execute(
            """
            INSERT INTO bot_runs(org_id, pid, started_at, last_heartbeat, phase, host)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(org_id) DO UPDATE SET
                pid = excluded.pid,
                started_at = excluded.started_at,
                last_heartbeat = excluded.last_heartbeat,
                phase = excluded.phase,
                host = excluded.host
            """,
            (int(org_id), int(pid), ts, ts, phase, host),
        )


def _bot_run_clear(org_id: int) -> None:
    with _db() as conn:
        conn.execute("DELETE FROM bot_runs WHERE org_id = ?", (int(org_id),))


def _pid_alive(pid: int) -> bool:
    """Кросс-платформенная проверка живого PID (без сигнала)."""
    if pid <= 0:
        return False
    try:
        if os.name == "nt":
            import ctypes  # noqa: PLC0415

            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
            if not handle:
                return False
            try:
                exit_code = ctypes.c_ulong(0)
                ok = kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
                if not ok:
                    return False
                STILL_ACTIVE = 259
                return int(exit_code.value) == STILL_ACTIVE
            finally:
                kernel32.CloseHandle(handle)
        else:
            os.kill(int(pid), 0)
            return True
    except (OSError, ProcessLookupError):
        return False
    except Exception:
        return False


def _bot_run_status(org_id: int) -> dict[str, Any]:
    """Возвращает {alive, pid, last_heartbeat, phase, stale}.

    stale=True — запись есть, но процесс мёртв или heartbeat старше 5 мин.
    Если stale, запись удаляется."""
    with _db() as conn:
        row = conn.execute(
            "SELECT pid, started_at, last_heartbeat, phase FROM bot_runs WHERE org_id = ?",
            (int(org_id),),
        ).fetchone()
    if not row:
        return {"alive": False, "pid": None, "last_heartbeat": None, "phase": None, "stale": False}
    pid = int(row["pid"] or 0)
    alive_pid = _pid_alive(pid)
    hb_raw = str(row["last_heartbeat"] or "").strip()
    hb_ok = True
    hb_dt = None
    if hb_raw:
        try:
            hb_dt = datetime.fromisoformat(hb_raw)
            if hb_dt.tzinfo is None:
                hb_dt = hb_dt.replace(tzinfo=timezone.utc)
            hb_ok = (datetime.now(timezone.utc) - hb_dt).total_seconds() < 300.0
        except ValueError:
            hb_ok = True
    if not alive_pid:
        _bot_run_clear(int(org_id))
        return {
            "alive": False,
            "pid": pid,
            "last_heartbeat": hb_raw or None,
            "phase": row["phase"],
            "stale": True,
        }
    return {
        "alive": True,
        "pid": pid,
        "last_heartbeat": hb_raw or None,
        "phase": row["phase"],
        "stale": not hb_ok,
    }


def _bot_is_running(org_id: int) -> bool:
    """Истина, если бот действительно запущен — БД (любой worker) ИЛИ локальный proc."""
    proc = bot_processes.get(int(org_id))
    if proc and proc.poll() is None:
        return True
    return bool(_bot_run_status(int(org_id)).get("alive"))


# ── Лидоген по сайтам: helpers ───────────────────────────────────────────────


def _web_get_serpapi_key(org_id: int) -> str:
    """Читает SerpAPI ключ из tenant config (секция web_leadgen). Пусто — не настроен."""
    try:
        paths = _tenant_paths(int(org_id))
        if not paths["config"].exists():
            return ""
        cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ""
    section = cfg.get("web_leadgen") if isinstance(cfg, dict) else None
    if not isinstance(section, dict):
        return ""
    return str(section.get("serpapi_key") or "").strip()


def _web_set_serpapi_key(org_id: int, key: str) -> None:
    paths = _tenant_paths(int(org_id))
    cfg = (
        json.loads(paths["config"].read_text(encoding="utf-8"))
        if paths["config"].exists()
        else {}
    )
    if not isinstance(cfg, dict):
        cfg = {}
    section = cfg.get("web_leadgen")
    if not isinstance(section, dict):
        section = {}
    section["serpapi_key"] = (key or "").strip()
    cfg["web_leadgen"] = section
    paths["base"].mkdir(parents=True, exist_ok=True)
    paths["config"].write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def _web_jobs_enqueue(
    conn: sqlite3.Connection,
    *,
    org_id: int,
    kind: str,
    payload: dict[str, Any],
) -> int:
    """Создаёт строку в web_lead_jobs (status=pending). Возвращает её id."""
    cur = conn.execute(
        """
        INSERT INTO web_lead_jobs(org_id, kind, payload_json, status, created_at)
        VALUES (?, ?, ?, 'pending', ?)
        """,
        (int(org_id), str(kind), json.dumps(payload, ensure_ascii=False), _now_iso()),
    )
    return int(cur.lastrowid or 0)


def _web_lead_upsert(
    conn: sqlite3.Connection,
    *,
    org_id: int,
    domain: str,
    source: str,
    query: str,
    sc: web_lead_finder.SiteContacts | None = None,
) -> int:
    """Upsert строки `web_leads`. Если sc передан — сохраняем найденные контакты.

    Возвращает id записи.
    """
    now = _now_iso()
    row = conn.execute(
        "SELECT id FROM web_leads WHERE org_id = ? AND domain = ?",
        (int(org_id), domain),
    ).fetchone()
    if sc is None:
        if row:
            return int(row["id"])
        cur = conn.execute(
            """
            INSERT INTO web_leads(org_id, domain, source, query, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'queued', ?, ?)
            """,
            (int(org_id), domain, source or "serp", query or "", now, now),
        )
        return int(cur.lastrowid or 0)

    payload = {
        "url": sc.url,
        "title": sc.title,
        "http_status": int(sc.http_status or 0),
        "emails_json": json.dumps(sc.emails, ensure_ascii=False),
        "phones_json": json.dumps(sc.phones, ensure_ascii=False),
        "telegrams_json": json.dumps(sc.telegrams, ensure_ascii=False),
        "whatsapps_json": json.dumps(sc.whatsapps, ensure_ascii=False),
        "vks_json": json.dumps(sc.vks, ensure_ascii=False),
        "pages_visited_json": json.dumps(sc.pages_visited, ensure_ascii=False),
        "last_error": sc.error or None,
        "last_checked_at": now,
        "updated_at": now,
        "status": "ready" if sc.is_useful else ("empty" if not sc.error else "error"),
    }
    if row:
        conn.execute(
            """
            UPDATE web_leads
               SET url=?, title=?, http_status=?,
                   emails_json=?, phones_json=?, telegrams_json=?, whatsapps_json=?, vks_json=?,
                   pages_visited_json=?, last_error=?, last_checked_at=?, updated_at=?, status=?
             WHERE id=?
            """,
            (
                payload["url"],
                payload["title"],
                payload["http_status"],
                payload["emails_json"],
                payload["phones_json"],
                payload["telegrams_json"],
                payload["whatsapps_json"],
                payload["vks_json"],
                payload["pages_visited_json"],
                payload["last_error"],
                payload["last_checked_at"],
                payload["updated_at"],
                payload["status"],
                int(row["id"]),
            ),
        )
        return int(row["id"])
    cur = conn.execute(
        """
        INSERT INTO web_leads(
            org_id, domain, source, query, url, title, http_status,
            emails_json, phones_json, telegrams_json, whatsapps_json, vks_json,
            pages_visited_json, last_error, status, last_checked_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(org_id),
            domain,
            source or "serp",
            query or "",
            payload["url"],
            payload["title"],
            payload["http_status"],
            payload["emails_json"],
            payload["phones_json"],
            payload["telegrams_json"],
            payload["whatsapps_json"],
            payload["vks_json"],
            payload["pages_visited_json"],
            payload["last_error"],
            payload["status"],
            payload["last_checked_at"],
            now,
            now,
        ),
    )
    return int(cur.lastrowid or 0)


def _web_jobs_claim_one(conn: sqlite3.Connection) -> sqlite3.Row | None:
    """Атомарно забирает одну pending-задачу. Возвращает строку или None."""
    row = conn.execute(
        "SELECT id, org_id, kind, payload_json, attempts FROM web_lead_jobs "
        "WHERE status = 'pending' ORDER BY id LIMIT 1"
    ).fetchone()
    if not row:
        return None
    upd = conn.execute(
        "UPDATE web_lead_jobs SET status='running', started_at=?, attempts=attempts+1 "
        "WHERE id=? AND status='pending'",
        (_now_iso(), int(row["id"])),
    )
    if upd.rowcount != 1:
        return None
    return row


def _web_job_finish(conn: sqlite3.Connection, job_id: int, *, error: str = "") -> None:
    conn.execute(
        "UPDATE web_lead_jobs SET status=?, finished_at=?, error=? WHERE id=?",
        ("error" if error else "done", _now_iso(), (error or None), int(job_id)),
    )


def _web_job_run_serp_search(conn: sqlite3.Connection, *, org_id: int, payload: dict[str, Any]) -> None:
    query = str(payload.get("query") or "").strip()
    count = max(1, min(50, int(payload.get("count") or 20)))
    gl = str(payload.get("gl") or "ru")
    hl = str(payload.get("hl") or "ru")
    api_key = _web_get_serpapi_key(org_id)
    if not api_key:
        raise RuntimeError("SerpAPI key не настроен для организации")
    res = web_lead_finder.serpapi_search_domains(
        query, api_key=api_key, num=count, gl=gl, hl=hl
    )
    if not res.get("ok"):
        raise RuntimeError(f"SerpAPI: {res.get('error') or 'unknown'}")
    items = res.get("items") or []
    for it in items:
        domain = str(it.get("domain") or "").strip()
        if not domain:
            continue
        # Создаём заглушку (status=queued) и сразу job на парсинг.
        _web_lead_upsert(conn, org_id=org_id, domain=domain, source="serp", query=query)
        _web_jobs_enqueue(
            conn,
            org_id=org_id,
            kind="parse_domain",
            payload={"domain": domain, "query": query, "source": "serp"},
        )


def _web_job_run_parse_domain(conn: sqlite3.Connection, *, org_id: int, payload: dict[str, Any]) -> None:
    domain = web_lead_finder.normalize_domain(str(payload.get("domain") or ""))
    if not domain:
        raise RuntimeError("invalid_domain")
    sc = web_lead_finder.discover_site_contacts(domain)
    _web_lead_upsert(
        conn,
        org_id=org_id,
        domain=domain,
        source=str(payload.get("source") or "serp"),
        query=str(payload.get("query") or ""),
        sc=sc,
    )


def _web_jobs_tick() -> bool:
    """Один шаг воркера. True если задача обработана (продолжаем без сна)."""
    with _db() as conn:
        row = _web_jobs_claim_one(conn)
    if row is None:
        return False
    job_id = int(row["id"])
    org_id = int(row["org_id"])
    kind = str(row["kind"] or "")
    try:
        payload = json.loads(row["payload_json"] or "{}")
        if not isinstance(payload, dict):
            payload = {}
    except (TypeError, ValueError):
        payload = {}

    err = ""
    try:
        with _db() as conn:
            if kind == "serp_search":
                _web_job_run_serp_search(conn, org_id=org_id, payload=payload)
            elif kind == "parse_domain":
                _web_job_run_parse_domain(conn, org_id=org_id, payload=payload)
            else:
                err = f"unknown_kind:{kind}"
    except Exception as exc:  # noqa: BLE001
        err = str(exc)[:1000]

    with _db() as conn:
        _web_job_finish(conn, job_id, error=err)
    return True


_WEB_JOBS_WORKER_STARTED = False
_WEB_JOBS_WORKER_LOCK = threading.Lock()


def _web_jobs_worker_loop() -> None:
    while True:
        try:
            did = _web_jobs_tick()
        except Exception:  # noqa: BLE001
            did = False
        time.sleep(0.5 if did else 3.0)


def _ensure_web_jobs_worker() -> None:
    """Стартует фоновый daemon-thread для очереди web_lead_jobs (idempotent per process)."""
    global _WEB_JOBS_WORKER_STARTED
    if _WEB_JOBS_WORKER_STARTED:
        return
    with _WEB_JOBS_WORKER_LOCK:
        if _WEB_JOBS_WORKER_STARTED:
            return
        if str(os.getenv("LEADGEN_WEBLEADS_WORKER", "1") or "1").strip().lower() in (
            "0",
            "false",
            "no",
            "off",
        ):
            _WEB_JOBS_WORKER_STARTED = True
            return
        t = threading.Thread(target=_web_jobs_worker_loop, name="webleads-worker", daemon=True)
        t.start()
        _WEB_JOBS_WORKER_STARTED = True


def _web_lead_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    def _list(s: str | None) -> list[Any]:
        try:
            v = json.loads(s or "[]")
        except (TypeError, ValueError):
            return []
        return v if isinstance(v, list) else []

    return {
        "id": int(row["id"]),
        "org_id": int(row["org_id"]),
        "domain": row["domain"],
        "source": row["source"],
        "query": row["query"] or "",
        "url": row["url"] or "",
        "title": row["title"] or "",
        "http_status": int(row["http_status"] or 0),
        "emails": _list(row["emails_json"]),
        "phones": _list(row["phones_json"]),
        "telegrams": _list(row["telegrams_json"]),
        "whatsapps": _list(row["whatsapps_json"]),
        "vks": _list(row["vks_json"]),
        "pages_visited": _list(row["pages_visited_json"]),
        "status": row["status"] or "new",
        "last_error": row["last_error"] or "",
        "conversation_id": (
            int(row["conversation_id"]) if row["conversation_id"] is not None else None
        ),
        "last_checked_at": row["last_checked_at"] or "",
        "created_at": row["created_at"] or "",
        "updated_at": row["updated_at"] or "",
    }


def _outreach_regenerate_draft_text(org_id: int, oq_row: sqlite3.Row) -> str:
    """LLM-черновик для строки outreach по этапу (1/2/3)."""
    paths = _tenant_paths(org_id)
    if not paths["config"].exists():
        raise ValueError("Нет настроек организации")
    cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    llm = cfg.get("llm", {})
    if not isinstance(llm, dict) or not llm.get("enabled"):
        raise ValueError("LLM выключен в настройках")
    api_key = str(llm.get("api_key", "")).strip()
    if not api_key:
        raise ValueError("Не задан llm.api_key")
    base_url = str(llm.get("base_url", "") or "https://api.openai.com/v1").strip()
    model = str(llm.get("model", "") or "gpt-4o-mini").strip()
    partner = str(cfg.get("partner_name", "") or "партнёр").strip()
    tpl = cfg.get("templates") if isinstance(cfg.get("templates"), dict) else {}
    stage = int(oq_row["stage"] or 1)
    stage = max(1, min(3, stage))
    lead_snippet = str(oq_row["lead_snippet"] or "")[:1200]
    task_hint = str(oq_row["task_hint"] or "")[:800]
    chat_key = str(oq_row["source_chat"] or "—")
    un = str(oq_row["username"] or "").strip()
    uid = str(oq_row["user_id"] or "").strip()
    user_ctx = f"@{un.lstrip('@')}" if un else f"user_id {uid}"

    pr = effective_llm_prompts(cfg)

    if stage == 1:
        stage1_hint = str((tpl or {}).get("stage1", "") or "")[:800]
        system = pr["outreach_stage1_system"]
        user_msg = format_llm_prompt(
            pr["outreach_stage1_user"],
            partner=partner,
            user_ctx=user_ctx,
            chat_key=chat_key,
            lead_snippet=lead_snippet or "(не указан)",
            stage1_hint=stage1_hint or "—",
        )
    elif stage == 2:
        s2 = str((tpl or {}).get("stage2", "") or "")[:600]
        system = pr["outreach_stage2_system"]
        user_msg = format_llm_prompt(
            pr["outreach_stage2_user"],
            partner=partner,
            user_ctx=user_ctx,
            chat_key=chat_key,
            task_hint=task_hint or "—",
            lead_snippet=lead_snippet or "—",
            s2=s2 or "—",
        )
    else:
        s3 = str((tpl or {}).get("stage3", "") or "")[:600]
        system = pr["outreach_stage3_system"]
        user_msg = format_llm_prompt(
            pr["outreach_stage3_user"],
            partner=partner,
            user_ctx=user_ctx,
            task_hint=task_hint or "—",
            lead_snippet=lead_snippet or "",
            s3=s3 or "—",
        )
    return _openai_chat_completion(base_url, api_key, model, system, user_msg, temperature=0.65)


def _tenant_csv_tail(path: Path, limit: int = 200) -> list[dict[str, str]]:
    if not path.exists() or limit <= 0:
        return []
    rows: list[dict[str, str]] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row:
                    norm = {k: (v if v is not None else "") for k, v in row.items()}
                    if not _csv_row_deleted(norm):
                        rows.append(norm)
    except Exception:
        return []
    out = rows[-limit:]
    for r in out:
        r["_id"] = _lead_row_id(r)
    return out


def _leads_csv_fieldnames_union(path: Path) -> list[str]:
    out = list(LEADS_CSV_FIELDNAMES)
    if not path.exists():
        return out
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for fn in reader.fieldnames or []:
                if fn and fn not in out and fn != "_id":
                    out.append(fn)
    except Exception:
        return out
    return out


def _leads_csv_delete_ids(path: Path, drop_ids: set[str]) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    _leads_csv_backup(path)
    fieldnames = _leads_csv_fieldnames_union(path)
    rows_out: list[dict[str, str]] = []
    marked = 0
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                norm: dict[str, str] = {k: str(row.get(k) or "") for k in fieldnames}
                rid = _lead_row_id(norm)
                if rid in drop_ids and not _csv_row_deleted(norm):
                    norm["deleted"] = "1"
                    marked += 1
                rows_out.append(norm)
    except Exception:
        return 0, 0
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in rows_out:
                w.writerow(r)
        tmp.replace(path)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise
    visible = sum(1 for r in rows_out if not _csv_row_deleted(r))
    return marked, visible


def _leads_csv_update_lead_tag(path: Path, row_id: str, lead_tag: str) -> bool:
    """Меняет lead_tag у одной активной строки. True, если строка с таким id найдена."""
    tag = (lead_tag or "").strip()
    if tag not in ALLOWED_LEAD_TAGS:
        return False
    if tag == "lead":
        tag = ""
    if not path.is_file() or not row_id.strip():
        return False
    _leads_csv_backup(path)
    fieldnames = _leads_csv_fieldnames_union(path)
    rows_out: list[dict[str, str]] = []
    found = False
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                norm: dict[str, str] = {k: str(row.get(k) or "") for k in fieldnames}
                rid = _lead_row_id(norm)
                if rid == row_id and not _csv_row_deleted(norm):
                    norm["lead_tag"] = tag
                    found = True
                rows_out.append(norm)
    except Exception:
        return False
    if not found:
        return False
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in rows_out:
                w.writerow(r)
        tmp.replace(path)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise
    return True


def _leads_csv_clear(path: Path) -> None:
    """Помечает все строки как удалённые (мягко). Перед этим — копия sent_leads.csv.bak."""
    path.parent.mkdir(parents=True, exist_ok=True)
    _leads_csv_backup(path)
    fieldnames = _leads_csv_fieldnames_union(path)
    if not path.is_file():
        path.write_text(",".join(fieldnames) + "\n", encoding="utf-8")
        return
    rows_out: list[dict[str, str]] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                norm = {k: str(row.get(k) or "") for k in fieldnames}
                norm["deleted"] = "1"
                rows_out.append(norm)
    except OSError:
        path.write_text(",".join(fieldnames) + "\n", encoding="utf-8")
        return
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in rows_out:
                w.writerow(r)
        tmp.replace(path)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise


def _tenant_csv_for_conversations(path: Path, max_rows: int = 5000) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows: list[dict[str, str]] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row:
                    norm = {k: (v if v is not None else "") for k, v in row.items()}
                    if not _csv_row_deleted(norm):
                        rows.append(norm)
    except Exception:
        return []
    if len(rows) > max_rows:
        rows = rows[-max_rows:]
    return rows


_csv_count_cache: dict[str, tuple[float, int, int]] = {}


def _count_csv_rows(path: Path) -> int:
    """Кешируем по (resolved_path, mtime_ns, size). При неизменности файла —
    возвращаем сохранённое значение, не парся CSV целиком."""
    if not path.exists():
        return 0
    try:
        st = path.stat()
        key = str(path.resolve())
        cached = _csv_count_cache.get(key)
        if cached is not None and cached[0] == st.st_mtime_ns and cached[1] == st.st_size:
            return cached[2]
    except OSError:
        st = None  # type: ignore[assignment]
    n = 0
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row and not _csv_row_deleted(row):
                    n += 1
    except Exception:
        return 0
    if st is not None:
        _csv_count_cache[str(path.resolve())] = (st.st_mtime_ns, st.st_size, n)
    return n


def _parse_leads_timestamp_to_utc_date(s: str) -> date | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).date()


def _leads_csv_counts_by_day(path: Path, days: int = 30) -> list[dict[str, Any]]:
    from collections import defaultdict

    days = max(1, min(90, int(days)))
    end_d = datetime.now(timezone.utc).date()
    start_d = end_d - timedelta(days=days - 1)
    counts: dict[str, int] = defaultdict(int)
    if path.is_file():
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not row:
                        continue
                    if _csv_row_deleted(row):
                        continue
                    d = _parse_leads_timestamp_to_utc_date(str(row.get("timestamp") or ""))
                    if d is None or d < start_d or d > end_d:
                        continue
                    counts[d.isoformat()] += 1
        except OSError:
            pass
    out: list[dict[str, Any]] = []
    cur = start_d
    while cur <= end_d:
        k = cur.isoformat()
        out.append({"date": k, "count": int(counts.get(k, 0))})
        cur += timedelta(days=1)
    return out


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _public_app_base_url() -> str:
    base = (os.getenv("PUBLIC_APP_URL") or "").strip().rstrip("/")
    if base:
        return base
    try:
        return (request.host_url or "").rstrip("/")
    except RuntimeError:
        return ""


def _yookassa_credentials() -> tuple[str, str] | None:
    shop = os.getenv("YOOKASSA_SHOP_ID", "").strip()
    secret = os.getenv("YOOKASSA_SECRET_KEY", "").strip()
    if not shop or not secret:
        return None
    return shop, secret


def _yookassa_api_json(
    method: str,
    rel_path: str,
    payload: dict[str, Any] | None = None,
    *,
    idempotence_key: str | None = None,
) -> dict[str, Any]:
    cred = _yookassa_credentials()
    if not cred:
        raise ValueError(
            "YooKassa не настроена: задайте переменные окружения YOOKASSA_SHOP_ID и YOOKASSA_SECRET_KEY"
        )
    shop, secret = cred
    auth = base64.b64encode(f"{shop}:{secret}".encode("utf-8")).decode("ascii")
    url = "https://api.yookassa.ru/v3" + rel_path
    data: bytes | None = None
    m = method.upper()
    if payload is not None and m in ("POST", "PATCH"):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=m)
    req.add_header("Authorization", f"Basic {auth}")
    if data is not None:
        req.add_header("Content-Type", "application/json; charset=utf-8")
    if idempotence_key:
        req.add_header("Idempotence-Key", idempotence_key)
    try:
        with urllib.request.urlopen(req, timeout=75) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise ValueError(f"YooKassa HTTP {exc.code}: {detail[:1200]}") from exc
    if not isinstance(raw, dict):
        raise ValueError("YooKassa: неожиданный формат ответа")
    return raw


def _apply_yookassa_payment_paid(yid: str, pay_remote: dict[str, Any]) -> None:
    """Подтверждает оплату по данным API YooKassa (после webhook — перепроверка через GET)."""
    if str(pay_remote.get("status") or "") != "succeeded":
        return
    ts_now = _now_iso()
    renew = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM payments WHERE provider_payment_id = ?",
            (yid,),
        ).fetchone()
        if row is None:
            return
        if str(row["status"] or "") == "paid":
            return
        org_id = int(row["org_id"])
        plan_id = str(row["plan_id"] or "").strip()
        if not plan_id:
            md = pay_remote.get("metadata") or {}
            plan_id = str(md.get("plan_id") or "").strip()
        gross = int(row["amount_rub_gross"] or 0)
        pay_row_id = int(row["id"])
        conn.execute(
            "UPDATE payments SET status = 'paid', paid_at = ?, fee_rub = 0, amount_rub_net = ? WHERE id = ?",
            (ts_now, gross, pay_row_id),
        )
        conn.execute(
            """
            INSERT INTO subscriptions(
                org_id, plan_id, status, renew_at, created_at,
                current_period_start, current_period_end, last_payment_id
            )
            VALUES (?, ?, 'active', ?, ?, ?, ?, ?)
            ON CONFLICT(org_id) DO UPDATE SET
                plan_id = excluded.plan_id,
                status = 'active',
                renew_at = excluded.renew_at,
                current_period_start = excluded.current_period_start,
                current_period_end = excluded.current_period_end,
                last_payment_id = excluded.last_payment_id
            """,
            (org_id, plan_id, renew, ts_now, ts_now, renew, pay_row_id),
        )
        # F.4: фиксируем факт оплаты в audit-журнале — поможет при разборе спорных ситуаций.
        try:
            conn.execute(
                "INSERT INTO login_audit(user_id, event, email, ip, user_agent, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    int(row["user_id"] or 0) or None,
                    "payment_applied",
                    f"org={org_id};plan={plan_id};yid={yid};amount={gross}",
                    None,
                    "yookassa-webhook",
                    ts_now,
                ),
            )
        except sqlite3.Error:
            pass


def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256(f"{salt}:{password}".encode("utf-8")).hexdigest()


def _is_argon2_hash(stored: str) -> bool:
    return (stored or "").startswith("$argon2")


def _verify_password(stored: str, salt: str, password: str) -> tuple[bool, str | None]:
    """
    (ok, new_argon2_hash_or_none) — new_hash задан, если миграция с legacy SHA256
    и проверка прошла или нужен rehash argon2.
    """
    if _is_argon2_hash(stored):
        try:
            _pwd_hasher.verify(stored, password)
        except VerifyMismatchError:
            return False, None
        if _pwd_hasher.check_needs_rehash(stored):
            return True, _pwd_hasher.hash(password)
        return True, None
    if _hash_password(password, salt) == stored:
        return True, _pwd_hasher.hash(password)
    return False, None


def _set_auth_cookie(resp: Any, token: str) -> None:
    resp.set_cookie(
        "auth_token",
        token,
        max_age=SESSION_TTL_HOURS * 3600,
        httponly=True,
        samesite="Lax",
        secure=bool(COOKIE_SECURE),
    )
    # Non-HttpOnly «маркер присутствия» — фронт читает только его, сам токен не виден JS.
    # Хранит «1»; отсутствует / пуст — значит сессии нет.
    resp.set_cookie(
        "auth_present",
        "1",
        max_age=SESSION_TTL_HOURS * 3600,
        httponly=False,
        samesite="Lax",
        secure=bool(COOKIE_SECURE),
    )


def _clear_auth_cookies(resp: Any) -> None:
    resp.delete_cookie("auth_token", samesite="Lax", secure=bool(COOKIE_SECURE))
    resp.delete_cookie("auth_present", samesite="Lax", secure=bool(COOKIE_SECURE))


def _client_ip_ua() -> tuple[str, str]:
    return (request.remote_addr or "")[:200], (request.headers.get("User-Agent", "") or "")[:500]


def _audit_login(
    user_id: int | None, event: str, email: str | None = None, *, ex_user_agent: str | None = None
) -> None:
    ip, ua = _client_ip_ua()
    if ex_user_agent is not None:
        ua = ex_user_agent[:500]
    em = (email or "").strip()[:200] if email else None
    if not em:
        em = None
    with _db() as conn:
        conn.execute(
            "INSERT INTO login_audit(user_id, event, email, ip, user_agent, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, event, em, ip or None, ua or None, _now_iso()),
        )


def _make_totp_step_token(user_id: int) -> str:
    return str(_totp_login_serializer.dumps(str(user_id)))


def _parse_totp_step_token(raw: str) -> int | None:
    if not (raw or "").strip():
        return None
    try:
        s = str(_totp_login_serializer.loads((raw or "").strip(), max_age=300))
        return int(s)
    except (BadData, TypeError, ValueError):
        return None


def _current_session_token() -> str:
    t = (request.headers.get("X-Auth-Token", "") or "").strip()
    if not t:
        t = (request.cookies.get("auth_token", "") or "").strip()
    return t


def _session_user() -> sqlite3.Row | None:
    # Кешируем user в g на время одного запроса: иначе на каждом @require_auth + сеттере выполняется
    # 2 SELECT (sessions+users), и при ~3.7 запроса/сек на вкладку это десятки лишних SQL/сек.
    cached = getattr(g, "_leadgen_session_user", "_unset")
    if cached != "_unset":
        return cached  # type: ignore[return-value]
    token = _current_session_token()
    if not token:
        try:
            g._leadgen_session_user = None
        except Exception:
            pass
        return None
    with _db() as conn:
        row = conn.execute(
            """
            SELECT u.*, s.expires_at AS _session_expires_at
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token = ?
            """,
            (token,),
        ).fetchone()
        if not row:
            try:
                g._leadgen_session_user = None
            except Exception:
                pass
            return None
        try:
            exp = datetime.fromisoformat(str(row["_session_expires_at"]))
        except (ValueError, TypeError):
            try:
                g._leadgen_session_user = None
            except Exception:
                pass
            return None
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= exp:
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
            try:
                g._leadgen_session_user = None
            except Exception:
                pass
            return None
    try:
        g._leadgen_session_user = row
    except Exception:
        pass
    return row


def _detect_avatar_ext(data: bytes) -> str | None:
    if len(data) < 12:
        return None
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if data.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return ".gif"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return ".webp"
    return None


def _stored_avatar_path(user_id: int) -> Path | None:
    with _db() as conn:
        row = conn.execute("SELECT avatar_filename FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row or not row["avatar_filename"]:
        return None
    fn = str(row["avatar_filename"]).strip()
    if not fn or "/" in fn or "\\" in fn or ".." in fn:
        return None
    try:
        base = AVATARS_DIR.resolve()
        path = (AVATARS_DIR / fn).resolve()
        if path.is_file() and path.parent == base:
            return path
    except OSError:
        return None
    return None


def _viewer_can_access_avatar(viewer: sqlite3.Row, target_user_id: int) -> bool:
    """Доступ к аватару разрешён, если:
    - это сам пользователь;
    - viewer в роли admin (на уровне таблицы users) — доступ ко всем;
    - target входит в ту же организацию, что и viewer.
    """
    if int(viewer["id"]) == target_user_id:
        return True
    if str(viewer["role"] or "") == "admin":
        return True
    viewer_org, _ = _user_org_role(int(viewer["id"]))
    if not viewer_org:
        return False
    with _db() as conn:
        row = conn.execute(
            "SELECT 1 FROM memberships WHERE user_id = ? AND org_id = ? LIMIT 1",
            (int(target_user_id), int(viewer_org)),
        ).fetchone()
    return bool(row)


def require_auth() -> tuple[bool, tuple[dict[str, Any], int] | None]:
    user = _session_user()
    if user is None:
        return False, ({"message": "Unauthorized"}, 401)
    return True, None


def require_role(allowed: set[str]) -> tuple[bool, tuple[dict[str, Any], int] | None]:
    user = _session_user()
    if user is None:
        return False, ({"message": "Unauthorized"}, 401)
    _org_id, role = _user_org_role(int(user["id"]))
    if role not in allowed:
        return False, ({"message": "Forbidden"}, 403)
    return True, None


def require_org_role(allowed: AbstractSet[str]) -> tuple[bool, tuple[dict[str, Any], int] | None]:
    user = _session_user()
    if user is None:
        return False, ({"message": "Unauthorized"}, 401)
    _org_id, org_role = _user_org_role(int(user["id"]))
    if org_role not in allowed:
        return False, ({"message": "Недостаточно прав для этой операции."}, 403)
    return True, None


def _current_user_org_role() -> tuple[int, str] | None:
    user = _session_user()
    if user is None:
        return None
    return _user_org_role(int(user["id"]))


def _get_plan_for_org(org_id: int) -> sqlite3.Row | None:
    with _db() as conn:
        return conn.execute(
            """
            SELECT p.*, s.status AS sub_status, s.renew_at AS renew_at
            FROM subscriptions s
            JOIN plans p ON p.id = s.plan_id
            WHERE s.org_id = ?
            """,
            (org_id,),
        ).fetchone()


def _effective_max_telegram_accounts(org_id: int) -> int:
    """Сколько Telegram-аккаунтов разрешено по тарифу org (1…TELEGRAM_ACCOUNTS_ABS_MAX)."""
    plan = _get_plan_for_org(org_id)
    if plan is None:
        return min(3, TELEGRAM_ACCOUNTS_ABS_MAX)
    try:
        n = int(plan["max_telegram_accounts"])
    except (KeyError, TypeError, ValueError):
        n = 3
    return max(1, min(TELEGRAM_ACCOUNTS_ABS_MAX, n))


def _enforce_plan_on_config(cfg: dict[str, Any], plan: sqlite3.Row) -> dict[str, Any]:
    max_chats = min(int(plan["max_chats"]), PLAN_ABS_MAX_CHATS)
    min_interval = max(int(plan["monitor_interval_min_sec"]), PLAN_ABS_MIN_MONITOR_INTERVAL_SEC)
    max_dm_day = min(int(plan["max_dm_day"]), PLAN_ABS_MAX_DM_DAY)
    max_dm_month = min(int(plan["max_dm_month"]), PLAN_ABS_MAX_DM_MONTH)
    target = cfg.get("target_chats", [])
    if isinstance(target, list) and len(target) > max_chats:
        cfg["target_chats"] = target[:max_chats]
    limits = cfg.setdefault("limits", {})
    try:
        current = int(limits.get("monitor_interval_sec", 10))
    except Exception:
        current = 10
    limits["monitor_interval_sec"] = max(current, min_interval)
    # Hard cap DM budgets for MVP (enforced in bot by max_dm_month, and daily via daily_limit_range)
    limits["daily_limit_range"] = [max_dm_day, max_dm_day]
    limits["max_dm_month"] = max_dm_month
    return cfg


def read_log_tail(max_bytes: int = 150_000, *, max_lines: int | None = None) -> str:
    if not LOG_PATH.exists():
        return ""
    with LOG_PATH.open("rb") as f:
        f.seek(0, 2)
        size = f.tell()
        start = max(0, size - max_bytes)
        f.seek(start)
        text = f.read().decode("utf-8", errors="ignore")
    if max_lines and max_lines > 0:
        lines = text.splitlines()
        if len(lines) > max_lines:
            text = "\n".join(lines[-max_lines:])
    return text


def read_path_tail(path: Path, max_bytes: int = 120_000, *, max_lines: int | None = None) -> str:
    """Хвост текстового файла (UTF-8); для логов организации."""
    if not path.is_file():
        return ""
    cap = max(1024, min(500_000, int(max_bytes)))
    with path.open("rb") as f:
        f.seek(0, 2)
        size = f.tell()
        start = max(0, size - cap)
        f.seek(start)
        text = f.read().decode("utf-8", errors="replace")
    if max_lines and max_lines > 0:
        lines = text.splitlines()
        if len(lines) > max_lines:
            text = "\n".join(lines[-max_lines:])
    return text


def _redact_secrets_in_text(text: str) -> str:
    """Маскирует типичные секреты в текстах, отдаваемых в браузер (логи, stderr подпроцессов)."""
    if not text:
        return ""
    s = text
    s = re.sub(r"(?i)Bearer\s+[A-Za-z0-9_\-\.\~\+/]{12,}", "Bearer <redacted>", s)
    s = re.sub(r"\bsk-[a-zA-Z0-9]{16,}\b", "<redacted>", s)
    s = re.sub(r"(?i)\b(password|passwd|pwd|secret|token|api_key)=([^&\s\"']{4,})", r"\1=<redacted>", s)
    return s


def _subprocess_env_utf8() -> dict[str, str]:
    e = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
    return e


def _bot_argv_paths_and_session(paths: dict[str, Path], session_path: Path) -> list[str]:
    """Общие флаги путей tenant для подпроцесса telegram_leadgen_bot."""
    return [
        "--config-path",
        str(paths["config"]),
        "--state-path",
        str(paths["state"]),
        "--csv-path",
        str(paths["csv"]),
        "--log-path",
        str(paths["log"]),
        "--session-name",
        str(session_path),
    ]


def run_bot_command(args: list[str]) -> tuple[int, str]:
    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_subprocess_env_utf8(),
    )
    output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
    return result.returncode, _redact_secrets_in_text(output.strip())


_BOT_SUBPROC_TIMEOUT_SEC = int(os.getenv("BOT_SUBPROC_TIMEOUT_SEC", "180"))

# Возраст «висячего» lock-файла Telethon-сессии, после которого считаем его остатком
# аварийного завершения предыдущего воркера (и удаляем без участия пользователя).
# По умолчанию: 3× таймаута самого долгого подпроцесса, минимум 10 минут.
_TG_LOCK_STALE_AGE_SEC: float = max(
    float(os.getenv("TG_SESSION_LOCK_STALE_AGE_SEC", "0") or 0.0),
    float(_BOT_SUBPROC_TIMEOUT_SEC) * 3.0,
    600.0,
)

# Уникальный идентификатор инстанса Flask-процесса. Если PID переиспользовался
# после рестарта (Windows охотно даёт тот же PID новому процессу), это поле
# покажет, что lock-файл оставлен прошлым запуском — и его можно снимать.
_PROCESS_INSTANCE_ID: str = secrets.token_hex(8)


def _pid_is_alive(pid: int) -> bool:
    """Проверка, что процесс с PID ещё есть в ОС (для снятия «висячих» *.session.lock)."""
    if pid <= 0:
        return False
    if sys.platform == "win32":
        try:
            import ctypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            k32 = ctypes.windll.kernel32
            h = int(k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid))
            if h:
                k32.CloseHandle(h)
                return True
            err = int(k32.GetLastError())
            # 87 = ERROR_INVALID_PARAMETER — нет такого PID
            if err == 87:
                return False
            # 5 = ACCESS_DENIED — процесс может существовать; не считаем мёртвым
            if err == 5:
                return True
            return False
        except Exception:
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _try_clear_stale_tg_session_lock(lock_p: Path) -> None:
    """Снимает «висячие» lock-файлы Telethon-сессии без участия пользователя.

    Считаем lock устаревшим, если выполняется хотя бы одно условие:
      1. процесс-владелец (`pid=...`) мёртв в ОС;
      2. в lock записан идентификатор `instance=...` другого инстанса
         (наш PID переиспользовался после аварийного рестарта);
      3. возраст файла больше `_TG_LOCK_STALE_AGE_SEC` (по умолчанию ~10 минут):
         за это время любая операция Telethon обязана завершиться или быть прервана.
    """
    try:
        if not lock_p.is_file():
            return
        raw = lock_p.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return

    holder: int | None = None
    m_pid = re.search(r"\bpid=(\d+)", raw)
    if m_pid:
        try:
            holder = int(m_pid.group(1))
        except ValueError:
            holder = None

    m_inst = re.search(r"\binstance=([0-9a-fA-F]+)", raw)
    instance = m_inst.group(1) if m_inst else ""

    age_sec: float = float("inf")
    try:
        age_sec = max(0.0, time.time() - lock_p.stat().st_mtime)
    except OSError:
        age_sec = float("inf")

    is_dead_holder = holder is None or not _pid_is_alive(holder)
    is_other_instance = bool(instance) and instance != _PROCESS_INSTANCE_ID
    is_self_pid_other_instance = (
        holder is not None
        and holder == os.getpid()
        and bool(instance)
        and instance != _PROCESS_INSTANCE_ID
    )
    is_too_old = age_sec >= _TG_LOCK_STALE_AGE_SEC

    if not (is_dead_holder or is_self_pid_other_instance or is_too_old):
        # Текущий процесс действительно держит lock — оставляем.
        return

    # PID совпадает с нашим, но instance отсутствует/совпадает и lock «свежий» —
    # значит lock реально занят текущим процессом (другим обработчиком запроса).
    if (
        holder is not None
        and holder == os.getpid()
        and (not instance or instance == _PROCESS_INSTANCE_ID)
        and not is_too_old
        and not is_dead_holder
    ):
        return

    try:
        lock_p.unlink(missing_ok=True)
    except OSError:
        pass


def _tg_session_sqlite_path(session_path: Path) -> Path:
    """Telethon SQLite session file path for a given session stem/path."""
    return Path(str(session_path) + ".session")


def _sweep_stale_tg_session_locks_at_startup() -> int:
    """Чистит остатки lock-файлов сессий Telethon при старте веб-процесса.

    Любой lock, существующий до того, как новый Flask-инстанс что-либо сделал,
    по определению «висячий»: его создал предыдущий запуск, который завершился
    аварийно (kill -9, отключение питания). Безопасно удаляем такие файлы,
    не дожидаясь таймаута пользовательского запроса.
    Возвращает число снятых локов (для лога).
    """
    removed = 0
    candidates: list[Path] = []
    try:
        candidates.extend(SESSIONS_DIR.glob("*.session.lock"))
    except OSError:
        pass
    try:
        if TENANTS_DIR.exists():
            candidates.extend(TENANTS_DIR.glob("org_*/sessions/*.session.lock"))
    except OSError:
        pass
    for lock_p in candidates:
        try:
            lock_p.unlink(missing_ok=True)
            removed += 1
        except OSError:
            continue
    if removed:
        print(
            f"INFO: cleared {removed} stale Telethon session lock(s) at startup",
            file=sys.stderr,
        )
    return removed


_sweep_stale_tg_session_locks_at_startup()


@contextmanager
def _tg_session_lock(session_path: Path, *, action: str, wait_sec: float = 8.0) -> Any:
    """Mutex for operations using the same Telethon session file.

    Prevents concurrent subprocesses from touching the same SQLite session,
    which otherwise fails with sqlite3.OperationalError: database is locked.
    """
    sess_sqlite = _tg_session_sqlite_path(session_path)
    lock_p = Path(str(sess_sqlite) + ".lock")
    lock_p.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(0.0, float(wait_sec))
    fd: int | None = None
    act = (action or "").strip() or "telegram_operation"
    # Превентивно проверяем lock на «висячесть» — устраняем остаток предыдущего
    # запуска, если PID переиспользовался Windows.
    _try_clear_stale_tg_session_lock(lock_p)
    while True:
        try:
            fd = os.open(str(lock_p), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(
                    fd,
                    (
                        f"pid={os.getpid()} instance={_PROCESS_INSTANCE_ID} "
                        f"ts={datetime.now(timezone.utc).isoformat()} action={act}\n"
                    ).encode("utf-8"),
                )
            except OSError:
                pass
            break
        except FileExistsError:
            _try_clear_stale_tg_session_lock(lock_p)
            if time.monotonic() >= deadline:
                # Последний шанс: если lock всё ещё на месте — попробуем сразу
                # ещё раз снять остаток (изменился ли возраст / pid) и захватить.
                _try_clear_stale_tg_session_lock(lock_p)
                try:
                    fd = os.open(str(lock_p), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                    try:
                        os.write(
                            fd,
                            (
                                f"pid={os.getpid()} instance={_PROCESS_INSTANCE_ID} "
                                f"ts={datetime.now(timezone.utc).isoformat()} action={act}\n"
                            ).encode("utf-8"),
                        )
                    except OSError:
                        pass
                    break
                except FileExistsError:
                    detail = ""
                    try:
                        detail = lock_p.read_text(encoding="utf-8", errors="replace").strip()
                    except OSError:
                        detail = ""
                    raise TimeoutError(f"telegram_session_locked: {lock_p} {detail}".strip())
            time.sleep(0.15)
    try:
        yield
    finally:
        try:
            if fd is not None:
                os.close(fd)
        except OSError:
            pass
        try:
            lock_p.unlink(missing_ok=True)
        except OSError:
            pass


_TG_LOCK_ACTION_RU: dict[str, str] = {
    "search_channels": "поиск каналов",
    "sync_dialogs": "синхронизация диалогов",
    "enroll_monitoring": "добавление/вступление в мониторинг",
    "telegram_operation": "операция Telegram",
}


def _humanize_tg_session_lock_error(exc: Exception) -> str:
    """Преобразует TimeoutError('telegram_session_locked: ... action=... ts=...') в читаемое сообщение."""
    s = str(exc)
    action = ""
    ts = ""
    pid = ""
    m = re.search(r"\baction=([^\s]+)", s)
    if m:
        action = m.group(1).strip()
    m = re.search(r"\bts=([0-9TZ:+.-]+)", s)
    if m:
        ts = m.group(1).strip()
    m = re.search(r"\bpid=(\d+)", s)
    if m:
        pid = m.group(1).strip()
    act_ru = _TG_LOCK_ACTION_RU.get(action, action or "операция Telegram")
    parts = [f"Сессия Telegram занята: идёт {act_ru}."]
    if ts:
        parts.append(f"Запущено: {ts}.")
    if pid:
        parts.append(f"pid: {pid}.")
    parts.append("Подождите 5–10 секунд и повторите.")
    parts.append(
        "Если сообщение не исчезает минутами — остановите веб-процесс и удалите рядом с сессией файл «имя.session.lock» "
        "(остаток после аварийного завершения воркера)."
    )
    return " ".join(parts)[:1200]


# ─── Реестр запущенных подпроцессов поиска по org_id ───────────────────────
#
# Нужен, чтобы пользователь мог нажать «Остановить» в UI и реально прервать
# зависший Telethon-подпроцесс, а не ждать timeout. Реестр потокобезопасный,
# но не доделан до уровня job_id — ключ org_id обычно достаточен, потому что
# параллельных поисков от одной org не должно быть много.

_RUNNING_SEARCH_SUBPROCESSES: dict[int, list[subprocess.Popen[Any]]] = {}
_RUNNING_SEARCH_SUBPROCESSES_LOCK = threading.Lock()


def _register_search_subprocess(org_id: int, proc: subprocess.Popen[Any]) -> None:
    if org_id <= 0:
        return
    with _RUNNING_SEARCH_SUBPROCESSES_LOCK:
        _RUNNING_SEARCH_SUBPROCESSES.setdefault(int(org_id), []).append(proc)


def _unregister_search_subprocess(org_id: int, proc: subprocess.Popen[Any]) -> None:
    if org_id <= 0:
        return
    with _RUNNING_SEARCH_SUBPROCESSES_LOCK:
        lst = _RUNNING_SEARCH_SUBPROCESSES.get(int(org_id))
        if not lst:
            return
        try:
            lst.remove(proc)
        except ValueError:
            pass
        if not lst:
            _RUNNING_SEARCH_SUBPROCESSES.pop(int(org_id), None)


def _stop_search_subprocesses_for_org(org_id: int) -> int:
    """Аккуратно прервать все активные подпроцессы поиска для организации.

    Возвращает число процессов, которым отправлен сигнал терминации. Сначала
    `terminate()` (SIGTERM на POSIX / CTRL_BREAK на Windows), через 1.5 секунды
    — `kill()` если ещё живы. Реестр чистится в `run_bot_json_stdout_cancellable`.
    """
    with _RUNNING_SEARCH_SUBPROCESSES_LOCK:
        procs = list(_RUNNING_SEARCH_SUBPROCESSES.get(int(org_id), []))
    if not procs:
        return 0
    for p in procs:
        try:
            if p.poll() is None:
                p.terminate()
        except Exception:  # noqa: BLE001
            pass
    # подождём чуть и добьём kill'ом
    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline:
        alive = [p for p in procs if p.poll() is None]
        if not alive:
            break
        time.sleep(0.1)
    for p in procs:
        try:
            if p.poll() is None:
                p.kill()
        except Exception:  # noqa: BLE001
            pass
    return len(procs)


def run_bot_json_stdout_cancellable(
    args: list[str],
    *,
    org_id: int,
    timeout_sec: int | None = None,
) -> dict[str, Any]:
    """Как `run_bot_json_stdout`, но регистрирует Popen в реестре, чтобы кнопка
    «Стоп» в UI могла его прервать. Используется в endpoints поиска каналов.
    """
    timeout = timeout_sec if timeout_sec is not None else _BOT_SUBPROC_TIMEOUT_SEC
    creationflags = 0
    if sys.platform == "win32":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    try:
        proc = subprocess.Popen(  # noqa: S603
            args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=_subprocess_env_utf8(),
            creationflags=creationflags,
        )
    except OSError as exc:
        return {"ok": False, "error": f"Не удалось запустить подпроцесс: {exc}"}
    _register_search_subprocess(org_id, proc)
    cancelled = False
    try:
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except Exception:  # noqa: BLE001
                pass
            try:
                stdout, stderr = proc.communicate(timeout=5)
            except Exception:  # noqa: BLE001
                stdout, stderr = "", ""
            return {
                "ok": False,
                "error": (
                    f"Подпроцесс не ответил за {timeout}с и был прерван. "
                    "Увеличьте таймаут (режим комментариев/качества может быть медленным)."
                ),
                "stdout_tail": _redact_secrets_in_text((stdout or "")[-800:]),
            }
        if proc.returncode is not None and proc.returncode < 0:
            cancelled = True
    finally:
        _unregister_search_subprocess(org_id, proc)
    if cancelled:
        return {"ok": False, "error": "search_cancelled", "cancelled": True}
    raw = (stdout or "").strip()
    if not raw:
        err = (stderr or "").strip() or f"exit={proc.returncode}"
        return {"ok": False, "error": _redact_secrets_in_text(err[:800])}
    line = raw.splitlines()[-1]
    try:
        data = json.loads(line)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass
    return {"ok": False, "error": _redact_secrets_in_text(raw[-1500:])}


def run_bot_json_stdout(args: list[str], *, timeout_sec: int | None = None) -> dict[str, Any]:
    """Ожидает одну строку JSON в stdout (как у --search-channels).

    Таймаут защищает Flask-воркер от зависшего Telethon (FloodWait/сетевые проблемы).
    Длительные операции (поиск в комментариях, enroll сотен каналов) могут увеличить таймаут.
    """
    timeout = timeout_sec if timeout_sec is not None else _BOT_SUBPROC_TIMEOUT_SEC
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=_subprocess_env_utf8(),
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        partial = ""
        if exc.stdout:
            try:
                partial = exc.stdout.decode("utf-8", errors="replace") if isinstance(exc.stdout, bytes) else str(exc.stdout)
            except Exception:
                partial = ""
        return {
            "ok": False,
            "error": f"Подпроцесс не ответил за {timeout}с и был прерван. Увеличьте таймаут (режим комментариев/качества может быть медленным).",
            "stdout_tail": _redact_secrets_in_text(partial[-800:]),
        }
    raw = (result.stdout or "").strip()
    if not raw:
        err = (result.stderr or "").strip() or f"exit={result.returncode}"
        return {"ok": False, "error": _redact_secrets_in_text(err[:800])}
    line = raw.splitlines()[-1]
    try:
        data = json.loads(line)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass
    return {"ok": False, "error": _redact_secrets_in_text(raw[-1500:])}


def _page_search_discover():
    ctx = _current_user_org_role()
    if ctx is None:
        return redirect("/auth")
    _org_id, org_role = ctx
    if org_role not in _ORG_FUNNEL_ROLES:
        return redirect("/analytics", code=302)
    # Раньше только admin/manager; у «client» кнопка «Сгенерировать» молча не работала. API ниже тоже открыт для client.
    can_channel_search = True
    return render_template("search.html", active="discover", can_channel_search=can_channel_search)


def _page_contacts_schedule():
    ctx = _current_user_org_role()
    if ctx is None:
        return redirect("/auth")
    _org_id, org_role = ctx
    if org_role not in _ORG_FUNNEL_ROLES:
        return redirect("/analytics", code=302)
    return render_template("config.html", active="contacts_schedule")


@app.get("/")
def index():
    return render_template("home.html", active="home")


@app.get("/auth")
def page_auth():
    return render_template("auth.html", active="auth")


# ——— Воронка (инструменты) ———


@app.get("/tools/discover")
def page_tools_discover():
    return _page_search_discover()


@app.get("/tools/contacts")
def page_tools_contacts():
    return redirect("/tools/contacts/bot", code=302)


@app.get("/tools/contacts/bot")
def page_tools_contacts_bot():
    return render_template("bot.html", active="contacts_bot")


@app.get("/tools/contacts/schedule")
def page_tools_contacts_schedule():
    return _page_contacts_schedule()


@app.get("/tools/contacts/leads")
def page_tools_contacts_leads():
    ctx = _current_user_org_role()
    if ctx is None:
        return redirect("/auth")
    _org_id, org_role = ctx
    if org_role not in _ORG_FUNNEL_ROLES:
        return redirect("/analytics", code=302)
    return render_template("leads.html", active="contacts_leads")


@app.get("/tools/offers")
def page_tools_offers():
    return render_template("offers.html", active="offers", page_id="offers")


@app.get("/tools/conversations")
def page_tools_conversations():
    return render_template("conversations.html", active="conversations", page_id="conversations")


@app.get("/tools/inbox")
def page_tools_inbox():
    return render_template("chats.html", active="inbox")


@app.get("/tools/calls")
def page_tools_calls():
    return render_template("calls.html", active="calls", page_id="calls")


@app.get("/tools/web-leads")
def page_tools_web_leads():
    return render_template("web_leads.html", active="web_leads", page_id="web_leads")


@app.get("/docs/bot")
def page_docs_bot():
    ctx = _current_user_org_role()
    if ctx is None:
        return redirect("/auth")
    _org_id, org_role = ctx
    if org_role not in _ORG_FUNNEL_ROLES:
        return redirect("/analytics", code=302)
    return render_template("docs_bot.html", active="docs_bot", page_id="docs_bot")


# ——— Совместимость: старые URL ———


@app.get("/automation/bot")
def page_automation_bot():
    return redirect("/tools/contacts/bot", code=301)


@app.get("/automation/schedule")
def page_automation_schedule():
    return redirect("/tools/contacts/schedule", code=301)


# ——— Аналитика и аккаунт ———


@app.get("/analytics")
def page_analytics():
    return render_template("stats.html", active="analytics")


@app.get("/account/billing")
def page_account_billing():
    return render_template("billing.html", active="billing")


@app.get("/account/admin")
def page_account_admin():
    return render_template("admin.html", active="admin")


@app.get("/account/security")
def page_account_security():
    return render_template("security.html", active="security")


@app.get("/legal/privacy")
def legal_privacy():
    return render_template(
        "legal_page.html",
        active="legal",
        legal_title="Политика конфиденциальности",
        legal_sub="Краткое юридическое описание (заглушка). Версия документа: " + CONSENT_POLICY_VERSION,
        legal_html=(
            "<p>Эта страница — заглушка для публичного размещения политики. "
            "Согласуем текст и реквизиты оператора по отдельной задаче.</p>"
        ),
    )


@app.get("/legal/terms")
def legal_terms():
    return render_template(
        "legal_page.html",
        active="legal",
        legal_title="Пользовательское соглашение",
        legal_sub="Версия 1.0 (заглушка).",
        legal_html="<p>Условия использования сервиса будут опубликованы здесь.</p>",
    )


@app.get("/legal/consent")
def legal_consent():
    return render_template(
        "legal_page.html",
        active="legal",
        legal_title="Согласие на обработку персональных данных",
        legal_sub="Версия " + CONSENT_POLICY_VERSION,
        legal_html="<p>Описание целей, состава и срока обработки (заглушка для соответствия требованиям к согласию).</p>",
    )


@app.get("/legal/cookies")
def legal_cookies():
    return render_template(
        "legal_page.html",
        active="legal",
        legal_title="Файлы cookie",
        legal_sub="Прозрачность по трекингу (заглушка).",
        legal_html="<p>Какие cookie используются в веб-интерфейсе, зачем, как отключить.</p>",
    )


@app.get("/legal/contacts")
def legal_contacts():
    return render_template(
        "legal_page.html",
        active="legal",
        legal_title="Контакты",
        legal_sub="Связь по вопросам обработки данных (заглушка).",
        legal_html="<p>Email и юридический адрес оператора будут указаны на этой странице.</p>",
    )


# ——— Редиректы со старых URL (301) ———


@app.get("/stats")
def page_stats_legacy():
    return redirect("/analytics", code=301)


@app.get("/search")
def page_search_legacy():
    return redirect("/tools/discover", code=301)


@app.get("/bot")
def page_bot_legacy():
    return redirect("/tools/contacts/bot", code=301)


@app.get("/config")
def page_config_legacy():
    return redirect("/tools/contacts/schedule", code=301)


@app.get("/chats")
def page_chats_legacy():
    return redirect("/tools/inbox", code=301)


@app.get("/billing")
def page_billing_legacy():
    return redirect("/account/billing", code=301)


@app.get("/admin")
def page_admin_legacy():
    return redirect("/account/admin", code=301)


@app.get("/folders")
def page_folders():
    """Совместимость: старая ссылка → поиск и блок папок."""
    ctx = _current_user_org_role()
    if ctx is None:
        return redirect("/auth")
    _org_id, org_role = ctx
    if org_role not in _ORG_FUNNEL_ROLES:
        return redirect("/analytics", code=302)
    return redirect("/tools/discover#tg-folders", code=301)


@app.get("/healthz")
def health():
    return jsonify(
        {
            "status": "ok",
            "time": _now_iso(),
            "env": ENV,
            "app_version": WEB_APP_VERSION,
            "build": {
                "git_sha": (os.getenv("LEADGEN_GIT_SHA") or os.getenv("GIT_SHA") or os.getenv("COMMIT_SHA") or "").strip()
                or None,
                "build_id": (os.getenv("LEADGEN_BUILD_ID") or os.getenv("BUILD_ID") or "").strip() or None,
            },
        }
    )


@app.get("/api/auth/csrf")
@limiter.limit("60 per minute", methods=["GET"])
def auth_csrf():
    t = str(_csrf_serializer.dumps(_now_iso()))
    return jsonify({"csrf_token": t})


@app.post("/api/auth/register")
@limiter.limit("5 per hour", methods=["POST"])
def register():
    payload = request.get_json(force=True, silent=True) or {}
    email = str(payload.get("email", "")).strip().lower()
    password = str(payload.get("password", "")).strip()
    if not bool(payload.get("consent", False)):
        return jsonify({"message": "Нужно согласие с политикой конфиденциальности и условиями (где это указано)."}), 400
    if "@" not in email or len(password) < MIN_PASSWORD_LEN:
        return jsonify(
            {
                "message": f"Введите валидный email и пароль (минимум {MIN_PASSWORD_LEN} символов, Argon2).",
            }
        ), 400

    pwd_hash = _pwd_hasher.hash(password)
    try:
        with _db() as conn:
            users_count = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
            role = "admin" if users_count == 0 else "user"
            cur = conn.execute(
                "INSERT INTO users(email, password_hash, salt, role, created_at) VALUES (?, ?, ?, ?, ?)",
                (email, pwd_hash, "-", role, _now_iso()),
            )
            uid = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO consents(user_id, version, kind, created_at) VALUES (?, ?, ?, ?)",
                (uid, CONSENT_POLICY_VERSION, "register", _now_iso()),
            )
        return jsonify({"message": "Пользователь зарегистрирован"})
    except sqlite3.IntegrityError:
        return jsonify({"message": "Пользователь уже существует"}), 409


def _json_session_response(row: sqlite3.Row, token: str) -> Any:
    _ensure_org_for_user(int(row["id"]))
    totp_en = int(row["totp_enabled"] or 0) if "totp_enabled" in row.keys() else 0
    resp = jsonify(
        {
            "token": token,
            "email": row["email"],
            "role": row["role"],
            "totp_enabled": bool(totp_en),
        }
    )
    _set_auth_cookie(resp, token)
    return resp


def _open_session_for_user(
    user_row: sqlite3.Row, *, event: str = "login", email_hint: str | None = None
) -> Any:
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.now(timezone.utc) + timedelta(hours=SESSION_TTL_HOURS)).isoformat()
    ip, ua = _client_ip_ua()
    with _db() as conn:
        conn.execute(
            """
            INSERT INTO sessions(token, user_id, expires_at, created_at, ip, user_agent)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (token, user_row["id"], expires_at, _now_iso(), ip or None, ua or None),
        )
    _audit_login(int(user_row["id"]), event, email_hint or str(user_row["email"]))
    return _json_session_response(user_row, token)


@app.post("/api/auth/login")
@limiter.limit("20 per minute", methods=["POST"])
def login():
    payload = request.get_json(force=True, silent=True) or {}
    email = str(payload.get("email", "")).strip().lower()
    password = str(payload.get("password", "")).strip()
    with _db() as conn:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not row:
            _audit_login(None, "login_failed", email=email)
            return jsonify({"message": "Неверные логин или пароль"}), 401
        ok, rehash = _verify_password(
            str(row["password_hash"]), str(row["salt"] or ""), password
        )
        if not ok:
            _audit_login(int(row["id"]), "login_failed", email=email)
            return jsonify({"message": "Неверные логин или пароль"}), 401
        if rehash:
            conn.execute(
                "UPDATE users SET password_hash = ?, salt = ? WHERE id = ?",
                (rehash, "-", int(row["id"])),
            )
            row = conn.execute("SELECT * FROM users WHERE id = ?", (int(row["id"]),)).fetchone() or row
        totp_en = int(row["totp_enabled"] or 0) if "totp_enabled" in row.keys() else 0
        if totp_en:
            s = str(row["totp_secret"] or "").strip() if "totp_secret" in row.keys() else ""
            if not s:
                return jsonify({"message": "2FA включён, но секрет не найден. Обратитесь к администратору."}), 500
            step = _make_totp_step_token(int(row["id"]))
            _audit_login(int(row["id"]), "totp_step", email=email)
            return jsonify(
                {
                    "needs_totp": True,
                    "totp_token": step,
                    "message": "Введите одноразовый код из приложения-аутентификатора.",
                }
            )
    return _open_session_for_user(row, event="login", email_hint=email)


@app.post("/api/auth/login/totp")
@limiter.limit("20 per minute", methods=["POST"])
def login_totp():
    payload = request.get_json(force=True, silent=True) or {}
    raw_t = str(payload.get("totp_token", "")).strip()
    code = str(payload.get("code", "")).replace(" ", "").strip()
    uid = _parse_totp_step_token(raw_t)
    if not uid or not code:
        return jsonify({"message": "Некорректные данные (totp_token, code)."}), 400
    with _db() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
        if not row:
            return jsonify({"message": "Сессия входа устарела. Войдите снова."}), 401
        totp_en = int(row["totp_enabled"] or 0) if "totp_enabled" in row.keys() else 0
        if not totp_en:
            return jsonify({"message": "2FA не включена для этой учётной записи."}), 400
        sec = str(row["totp_secret"] or "").strip() if "totp_secret" in row.keys() else ""
        if not sec:
            return jsonify({"message": "Секрет 2FA не настроен."}), 500
        totp = pyotp.TOTP(sec)
        if not totp.verify(code, valid_window=1):
            _audit_login(uid, "totp_failed", email=str(row["email"] or ""))
            return jsonify({"message": "Неверный код 2FA."}), 401
    return _open_session_for_user(row, event="login_totp_ok", email_hint=str(row["email"] or ""))


@app.post("/api/auth/logout")
def logout():
    tok = _current_session_token()
    user = _session_user()
    if user:
        _audit_login(int(user["id"]), "logout", str(user["email"] or ""))
    if tok:
        with _db() as conn:
            conn.execute("DELETE FROM sessions WHERE token = ?", (tok,))
    resp = jsonify({"message": "OK"})
    _clear_auth_cookies(resp)
    return resp


@app.get("/api/auth/me")
def me():
    user = _session_user()
    if user is None:
        return jsonify({"authenticated": False})
    org_id, org_role = _user_org_role(int(user["id"]))
    default_oid = _default_org_id()
    org_name: str | None = None
    with _db() as conn:
        on = conn.execute("SELECT name FROM orgs WHERE id = ?", (org_id,)).fetchone()
        if on and on["name"] is not None:
            org_name = str(on["name"])
    plan = _get_plan_for_org(org_id)
    plan_id = str(plan["id"]) if plan else "free"
    sub_status = str(plan["sub_status"]) if plan and "sub_status" in plan.keys() else "unknown"
    uid = int(user["id"])
    totp_en = int(user["totp_enabled"] or 0) if "totp_enabled" in user.keys() else 0
    return jsonify(
        {
            "authenticated": True,
            "user_id": uid,
            "email": user["email"],
            "role": user["role"],
            "org_id": org_id,
            "org_name": org_name,
            "org_role": org_role,
            "plan_id": plan_id,
            "subscription_status": sub_status,
            "totp_enabled": bool(totp_en),
            "manager_on_default_org": bool(str(org_role) == "manager" and int(org_id) == int(default_oid)),
            "avatar_url": f"/api/users/{uid}/avatar",
        }
    )


@app.get("/api/account/sessions")
def account_list_sessions():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    cur_tok = _current_session_token()
    uid = int(user["id"])
    with _db() as conn:
        rows = conn.execute(
            """
            SELECT token, created_at, expires_at, ip, user_agent
            FROM sessions
            WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            (uid,),
        ).fetchall()
    out = []
    for r in rows:
        t = str(r["token"] or "")
        out.append(
            {
                "token_suffix": t[-8:] if len(t) > 8 else t,
                "is_current": bool(t and t == cur_tok),
                "created_at": r["created_at"],
                "expires_at": r["expires_at"],
                "ip": r["ip"] or None,
                "user_agent": (str(r["user_agent"])[:200] if r["user_agent"] else None),
            }
        )
    return jsonify({"sessions": out})


@app.post("/api/account/sessions/revoke-others")
def account_revoke_other_sessions():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    cur = _current_session_token()
    if not cur:
        return jsonify({"message": "Нет текущей сессии"}), 400
    uid = int(user["id"])
    with _db() as conn:
        conn.execute("DELETE FROM sessions WHERE user_id = ? AND token != ?", (uid, cur))
    return jsonify({"message": "Другие сессии завершены"})


@app.get("/api/account/login-audit")
def account_login_audit():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    uid = int(user["id"])
    lim = int(request.args.get("limit", 50) or 50)
    lim = max(1, min(lim, 200))
    with _db() as conn:
        rows = conn.execute(
            """
            SELECT event, email, ip, user_agent, created_at
            FROM login_audit
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (uid, lim),
        ).fetchall()
    items = [
        {
            "event": r["event"],
            "email": r["email"],
            "ip": r["ip"],
            "user_agent": (str(r["user_agent"])[:200] if r["user_agent"] else None),
            "created_at": r["created_at"],
        }
        for r in rows
    ]
    return jsonify({"items": items})


@app.post("/api/account/password")
def account_change_password():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    body = request.get_json(force=True, silent=True) or {}
    current = str(body.get("current_password", "")).strip()
    new_pw = str(body.get("new_password", "")).strip()
    if len(new_pw) < MIN_PASSWORD_LEN:
        return jsonify({"message": f"Новый пароль: минимум {MIN_PASSWORD_LEN} символов."}), 400
    ok_p, _ = _verify_password(str(user["password_hash"]), str(user["salt"] or ""), current)
    if not ok_p:
        return jsonify({"message": "Неверный текущий пароль."}), 400
    nhash = _pwd_hasher.hash(new_pw)
    with _db() as conn:
        conn.execute("UPDATE users SET password_hash = ?, salt = ? WHERE id = ?", (nhash, "-", int(user["id"])))
    return jsonify({"message": "Пароль обновлён"})


@app.post("/api/auth/2fa/setup")
def auth_2fa_setup():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    if int(user["totp_enabled"] or 0) if "totp_enabled" in user.keys() else 0:
        return jsonify({"message": "Сначала отключите 2FA."}), 400
    secret = pyotp.random_base32()
    with _db() as conn:
        conn.execute("UPDATE users SET totp_secret = ?, totp_enabled = 0 WHERE id = ?", (secret, int(user["id"])))
    email = str(user["email"] or "user")
    uri = pyotp.TOTP(secret).provisioning_uri(name=email, issuer_name="Leadgen")
    img = qrcode.make(uri)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return jsonify({"secret": secret, "otpauth_uri": uri, "qr_base64": b64})


@app.post("/api/auth/2fa/confirm")
def auth_2fa_confirm():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    body = request.get_json(force=True, silent=True) or {}
    code = str(body.get("code", "")).replace(" ", "").strip()
    sec = str(user["totp_secret"] or "").strip() if "totp_secret" in user.keys() else ""
    if not sec:
        return jsonify({"message": "Сначала вызовите настройку 2FA (setup)."}), 400
    if not pyotp.TOTP(sec).verify(code, valid_window=1):
        return jsonify({"message": "Неверный код."}), 400
    with _db() as conn:
        conn.execute("UPDATE users SET totp_enabled = 1 WHERE id = ?", (int(user["id"]),))
    return jsonify({"message": "Двухфакторная аутентификация включена."})


@app.post("/api/auth/2fa/disable")
def auth_2fa_disable():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    body = request.get_json(force=True, silent=True) or {}
    password = str(body.get("password", "")).strip()
    ok_p, rehash = _verify_password(str(user["password_hash"]), str(user["salt"] or ""), password)
    if not ok_p:
        return jsonify({"message": "Неверный пароль."}), 400
    uid = int(user["id"])
    with _db() as conn:
        if rehash:
            conn.execute("UPDATE users SET password_hash = ?, salt = ? WHERE id = ?", (rehash, "-", uid))
        conn.execute("UPDATE users SET totp_secret = NULL, totp_enabled = 0 WHERE id = ?", (uid,))
    return jsonify({"message": "2FA отключена."})


@app.get("/api/users/<int:user_id>/avatar")
def user_avatar(user_id: int):
    viewer = _session_user()
    if viewer is None or not _viewer_can_access_avatar(viewer, user_id):
        abort(404)
    path = _stored_avatar_path(user_id)
    if path is None:
        return redirect("/static/default-avatar.svg")
    return send_file(path, max_age=86400, conditional=True)


@app.post("/api/me/avatar")
def upload_my_avatar():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    uid = int(user["id"])
    up = request.files.get("file")
    if not up or not up.filename:
        return jsonify({"message": "Нужен файл (поле file)"}), 400
    data = up.read()
    if len(data) > MAX_AVATAR_BYTES:
        return jsonify({"message": "Файл слишком большой (макс. 2 МБ)"}), 400
    ext = _detect_avatar_ext(data)
    if not ext:
        return jsonify({"message": "Допустимы PNG, JPEG, GIF, WebP"}), 400
    new_name = f"{uid}{ext}"
    AVATARS_DIR.mkdir(parents=True, exist_ok=True)
    target = AVATARS_DIR / new_name
    old_name: str | None = None
    with _db() as conn:
        row = conn.execute("SELECT avatar_filename FROM users WHERE id = ?", (uid,)).fetchone()
        if row and row["avatar_filename"]:
            old_name = str(row["avatar_filename"])
    target.write_bytes(data)
    with _db() as conn:
        conn.execute("UPDATE users SET avatar_filename = ? WHERE id = ?", (new_name, uid))
    if old_name and old_name != new_name:
        old_p = AVATARS_DIR / old_name
        if old_p.is_file():
            try:
                old_p.unlink()
            except OSError:
                pass
    return jsonify({"message": "Аватар обновлён", "avatar_url": f"/api/users/{uid}/avatar"})


@app.delete("/api/me/avatar")
def delete_my_avatar():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    uid = int(user["id"])
    old_name: str | None = None
    with _db() as conn:
        row = conn.execute("SELECT avatar_filename FROM users WHERE id = ?", (uid,)).fetchone()
        if row and row["avatar_filename"]:
            old_name = str(row["avatar_filename"])
        conn.execute("UPDATE users SET avatar_filename = NULL WHERE id = ?", (uid,))
    if old_name:
        old_p = AVATARS_DIR / old_name
        if old_p.is_file():
            try:
                old_p.unlink()
            except OSError:
                pass
    return jsonify({"message": "Аватар сброшен"})


@app.get("/api/me/export")
def api_me_export():
    """Выгрузка данных профиля (JSON) для прозрачности / 152-ФЗ."""
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    uid = int(user["id"])
    org_id, org_role = _user_org_role(uid)
    plan = _get_plan_for_org(org_id)
    created_at = str(user["created_at"]) if user["created_at"] is not None else None
    payload: dict[str, Any] = {
        "export_version": 1,
        "generated_at": _now_iso(),
        "user": {
            "id": uid,
            "email": str(user["email"] or ""),
            "role": str(user["role"] or ""),
            "created_at": created_at,
            "org_id": org_id,
            "org_role": org_role,
            "totp_enabled": bool(int(user["totp_enabled"] or 0) if "totp_enabled" in user.keys() else 0),
        },
    }
    with _db() as conn:
        mem = conn.execute(
            "SELECT org_id, role, created_at FROM memberships WHERE user_id = ?",
            (uid,),
        ).fetchall()
        payload["memberships"] = [dict(r) for r in mem]
        cons = conn.execute(
            "SELECT version, kind, created_at FROM consents WHERE user_id = ? ORDER BY id",
            (uid,),
        ).fetchall()
        payload["consents"] = [dict(r) for r in cons]
        la = conn.execute(
            """
            SELECT event, email, ip, user_agent, created_at
            FROM login_audit
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT 200
            """,
            (uid,),
        ).fetchall()
        payload["login_audit_recent"] = [dict(r) for r in la]
    if plan:
        payload["plan_summary"] = {
            "id": str(plan["id"]),
            "sub_status": str(plan["sub_status"]) if "sub_status" in plan.keys() else "",
        }
    raw = json.dumps(payload, ensure_ascii=False, indent=2)
    return Response(
        raw,
        mimetype="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="leadgen_user_{uid}_export.json"'},
    )


def _purge_org_rows(conn: sqlite3.Connection, oid: int) -> None:
    """
    Полное удаление данных организации перед DELETE orgs.
    Иначе при FOREIGN KEY на orgs (conversations, web_leads, bot_runs и т.д.)
    транзакция удаления пользователя откатывается.
    """
    conn.execute("DELETE FROM web_lead_jobs WHERE org_id = ?", (oid,))
    conn.execute("DELETE FROM outreach_queue WHERE org_id = ?", (oid,))
    conn.execute("DELETE FROM calls WHERE org_id = ?", (oid,))
    conn.execute("DELETE FROM payments WHERE org_id = ?", (oid,))
    conn.execute("DELETE FROM bot_runs WHERE org_id = ?", (oid,))
    conn.execute("DELETE FROM web_leads WHERE org_id = ?", (oid,))
    conn.execute("DELETE FROM conversations WHERE org_id = ?", (oid,))
    conn.execute("DELETE FROM subscriptions WHERE org_id = ?", (oid,))
    conn.execute("DELETE FROM bot_profiles WHERE org_id = ?", (oid,))
    conn.execute("DELETE FROM orgs WHERE id = ?", (oid,))


@app.post("/api/me/delete")
def api_me_delete():
    """Удаление своего аккаунта и при пустой организации — данных org на диске."""
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    body = request.get_json(force=True, silent=True) or {}
    password = str(body.get("password", "")).strip()
    if not password:
        return jsonify({"message": "Укажите текущий пароль."}), 400
    uid = int(user["id"])
    ok_p, _rehash = _verify_password(str(user["password_hash"]), str(user["salt"] or ""), password)
    if not ok_p:
        return jsonify({"message": "Неверный пароль."}), 400

    default_oid = _default_org_id()
    avatar_to_delete: str | None = None

    with _db() as conn:
        row = conn.execute("SELECT id FROM users WHERE id = ?", (uid,)).fetchone()
        if not row:
            return jsonify({"message": "Пользователь не найден"}), 404
        org_rows = conn.execute("SELECT org_id FROM memberships WHERE user_id = ?", (uid,)).fetchall()
        org_ids = [int(r["org_id"]) for r in org_rows]
        av_row = conn.execute("SELECT avatar_filename FROM users WHERE id = ?", (uid,)).fetchone()
        if av_row and av_row["avatar_filename"]:
            avatar_to_delete = str(av_row["avatar_filename"])
        conn.execute("DELETE FROM consents WHERE user_id = ?", (uid,))
        conn.execute("DELETE FROM login_audit WHERE user_id = ?", (uid,))
        conn.execute("DELETE FROM sessions WHERE user_id = ?", (uid,))
        conn.execute("DELETE FROM memberships WHERE user_id = ?", (uid,))
        conn.execute("DELETE FROM users WHERE id = ?", (uid,))

        for oid in org_ids:
            left_row = conn.execute(
                "SELECT COUNT(*) AS c FROM memberships WHERE org_id = ?", (oid,)
            ).fetchone()
            left = int(left_row["c"]) if left_row else 0
            if left == 0 and oid != default_oid:
                _purge_org_rows(conn, oid)
                tenant = TENANTS_DIR / f"org_{oid}"
                if tenant.exists():
                    shutil.rmtree(tenant, ignore_errors=True)
                proc = bot_processes.pop(oid, None)
                if proc and proc.poll() is None:
                    proc.terminate()

    if avatar_to_delete:
        ap = AVATARS_DIR / avatar_to_delete
        try:
            ap.unlink(missing_ok=True)
        except OSError:
            pass
    resp = jsonify({"message": "Аккаунт удалён"})
    _clear_auth_cookies(resp)
    return resp


@app.get("/api/plans")
def list_plans():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    with _db() as conn:
        rows = conn.execute(
            """
            SELECT id, title, price_rub_month, max_chats, max_dm_day, max_dm_month, monitor_interval_min_sec,
                   max_telegram_accounts
            FROM plans
            ORDER BY price_rub_month ASC
            """
        ).fetchall()
    return jsonify({"plans": [dict(r) for r in rows]})


@app.patch("/api/admin/plans/<plan_id>")
def admin_plans_patch(plan_id: str):
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    plan_id = str(plan_id or "").strip()
    if not plan_id:
        return jsonify({"message": "Некорректный plan_id"}), 400
    body = request.get_json(force=True, silent=True)
    if not isinstance(body, dict) or not body:
        return jsonify({"message": "Передайте JSON с полями для обновления"}), 400
    int_fields = {
        "price_rub_month",
        "max_chats",
        "max_dm_day",
        "max_dm_month",
        "monitor_interval_min_sec",
        "max_telegram_accounts",
    }
    sets: list[str] = []
    params: list[Any] = []
    for k in int_fields:
        if k not in body:
            continue
        try:
            v = int(body[k])
        except (TypeError, ValueError):
            return jsonify({"message": f"Некорректное число: {k}"}), 400
        if v < 0:
            return jsonify({"message": f"Отрицательные значения недопустимы: {k}"}), 400
        if k == "max_chats" and v < 1:
            return jsonify({"message": "max_chats: минимум 1"}), 400
        if k == "max_telegram_accounts" and v < 1:
            return jsonify({"message": "max_telegram_accounts: минимум 1"}), 400
        if k == "max_telegram_accounts" and v > TELEGRAM_ACCOUNTS_ABS_MAX:
            return jsonify({"message": f"max_telegram_accounts: не больше {TELEGRAM_ACCOUNTS_ABS_MAX}"}), 400
        if k == "monitor_interval_min_sec" and v < 1:
            return jsonify({"message": "monitor_interval_min_sec: минимум 1"}), 400
        if k == "max_chats":
            v = min(v, PLAN_ABS_MAX_CHATS)
        elif k == "monitor_interval_min_sec":
            v = max(v, PLAN_ABS_MIN_MONITOR_INTERVAL_SEC)
        elif k == "max_dm_day":
            v = min(v, PLAN_ABS_MAX_DM_DAY)
        elif k == "max_dm_month":
            v = min(v, PLAN_ABS_MAX_DM_MONTH)
        sets.append(f"{k} = ?")
        params.append(v)
    if "title" in body:
        t = str(body.get("title") or "").strip()
        if not t:
            return jsonify({"message": "title не может быть пустым"}), 400
        sets.append("title = ?")
        params.append(t)
    if not sets:
        return jsonify({"message": "Нет полей для обновления (title, price_rub_month, max_chats, …)"}), 400
    with _db() as conn:
        row = conn.execute("SELECT id FROM plans WHERE id = ?", (plan_id,)).fetchone()
        if not row:
            return jsonify({"message": "План не найден"}), 404
        q = f"UPDATE plans SET {', '.join(sets)} WHERE id = ?"
        conn.execute(q, (*params, plan_id))
    return jsonify({"message": "План обновлён", "id": plan_id})


@app.get("/api/admin/users")
def admin_users():
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    with _db() as conn:
        rows = conn.execute(
            """
            SELECT
              u.id,
              u.email,
              u.role,
              u.created_at,
              u.avatar_filename,
              (
                SELECT m2.org_id FROM memberships m2
                WHERE m2.user_id = u.id
                ORDER BY
                  CASE WHEN m2.org_id = (SELECT id FROM orgs ORDER BY id LIMIT 1) THEN 1 ELSE 0 END,
                  m2.org_id
                LIMIT 1
              ) AS org_id,
              (
                SELECT m2.role FROM memberships m2
                WHERE m2.user_id = u.id
                ORDER BY
                  CASE WHEN m2.org_id = (SELECT id FROM orgs ORDER BY id LIMIT 1) THEN 1 ELSE 0 END,
                  m2.org_id
                LIMIT 1
              ) AS org_membership_role,
              s.plan_id AS sub_plan_id,
              s.status AS sub_status
            FROM users u
            LEFT JOIN subscriptions s ON s.org_id = (
                SELECT m3.org_id FROM memberships m3
                WHERE m3.user_id = u.id
                ORDER BY
                  CASE WHEN m3.org_id = (SELECT id FROM orgs ORDER BY id LIMIT 1) THEN 1 ELSE 0 END,
                  m3.org_id
                LIMIT 1
            )
            ORDER BY u.id DESC
            LIMIT 500
            """
        ).fetchall()
    return jsonify({"users": [dict(r) for r in rows]})


@app.get("/api/admin/orgs")
@limiter.limit("60 per minute", methods=["GET"])
def admin_orgs_list():
    """Список организаций для привязки менеджера к клиенту."""
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    with _db() as conn:
        rows = conn.execute(
            """
            SELECT o.id, o.name, o.created_at,
              (SELECT COUNT(*) FROM memberships m WHERE m.org_id = o.id) AS members_count
            FROM orgs o
            ORDER BY o.id ASC
            """
        ).fetchall()
    return jsonify({"orgs": [dict(r) for r in rows]})


# ─── Перезапуск веб-приложения (только для платформенного admin) ───────────
#
# Стратегия выбирается через env, чтобы работать с любым процесс-менеджером:
#   LEADGEN_RESTART_COMMAND="systemctl restart leadgen.service"
#       — выполнить произвольную команду (рекомендуется на проде с systemd /
#       pm2 / supervisor). Должна быть выполнима юзером Flask.
#   LEADGEN_RESTART_TOUCH_FILE="/var/run/leadgen/reload.flag"
#       — touch указанного файла, перезапуск делает внешний watcher
#       (gunicorn --reload-extra-file, uvicorn --reload-include и т.п.).
#   LEADGEN_RESTART_PARENT_SIGNAL="HUP"
#       — отправить сигнал родительскому процессу (обычно master gunicorn
#       реагирует на SIGHUP как «graceful reload»). POSIX-only.
#   LEADGEN_ALLOW_SELF_EXEC="1"
#       — fallback: текущий процесс заменит сам себя через os.execv
#       (только если запущен напрямую `python web_app.py`, не workerом).
#
# Если ничего не задано, отдадим понятную ошибку — без угадывания, чтобы
# не уронить прод неожиданным механизмом.


def _restart_method_summary() -> dict[str, Any]:
    cmd = (os.getenv("LEADGEN_RESTART_COMMAND") or "").strip()
    touch = (os.getenv("LEADGEN_RESTART_TOUCH_FILE") or "").strip()
    sig = (os.getenv("LEADGEN_RESTART_PARENT_SIGNAL") or "").strip()
    allow_exec = (os.getenv("LEADGEN_ALLOW_SELF_EXEC") or "").strip() in (
        "1",
        "true",
        "yes",
        "on",
    )
    method = "none"
    if cmd:
        method = "command"
    elif touch:
        method = "touch"
    elif sig:
        method = "parent_signal"
    elif allow_exec:
        method = "self_exec"
    return {
        "method": method,
        "command": cmd if cmd else None,
        "touch_file": touch if touch else None,
        "parent_signal": sig if sig else None,
        "self_exec_allowed": allow_exec,
    }


def _do_restart_command(cmd: str) -> None:
    # Detached subprocess: пусть переживёт текущий веб-процесс.
    try:
        if sys.platform == "win32":
            subprocess.Popen(
                cmd,
                shell=True,
                creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                | getattr(subprocess, "DETACHED_PROCESS", 0),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
        else:
            subprocess.Popen(
                cmd,
                shell=True,
                start_new_session=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: restart command failed: {exc}", file=sys.stderr)


def _do_restart_touch(path: str) -> None:
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        # touch с обновлением mtime
        with p.open("a", encoding="utf-8"):
            os.utime(p, None)
    except OSError as exc:
        print(f"WARNING: restart touch failed for {path}: {exc}", file=sys.stderr)


def _do_restart_parent_signal(name: str) -> None:
    if sys.platform == "win32":
        print("WARNING: parent signal restart is POSIX-only", file=sys.stderr)
        return
    raw = name.upper().lstrip("S")
    if raw.startswith("IG"):
        raw = raw[2:]
    sig_name = "SIG" + raw if not raw.startswith("SIG") else raw
    sig = getattr(signal, sig_name, None)
    if sig is None:
        print(f"WARNING: unknown signal '{name}'", file=sys.stderr)
        return
    try:
        os.kill(os.getppid(), int(sig))
    except OSError as exc:
        print(f"WARNING: parent signal {sig_name} failed: {exc}", file=sys.stderr)


def _do_restart_self_exec() -> None:
    try:
        os.execv(sys.executable, [sys.executable, *sys.argv])
    except OSError as exc:
        print(f"WARNING: self exec failed: {exc}", file=sys.stderr)


def _schedule_restart_async(method: dict[str, Any], delay_sec: float = 1.2) -> None:
    """Откладываем перезапуск, чтобы HTTP-ответ успел уйти клиенту."""

    def _runner() -> None:
        try:
            time.sleep(max(0.1, float(delay_sec)))
        except Exception:  # noqa: BLE001
            pass
        m = method.get("method")
        if m == "command":
            _do_restart_command(str(method.get("command") or ""))
        elif m == "touch":
            _do_restart_touch(str(method.get("touch_file") or ""))
        elif m == "parent_signal":
            _do_restart_parent_signal(str(method.get("parent_signal") or "HUP"))
        elif m == "self_exec":
            _do_restart_self_exec()

    threading.Thread(target=_runner, name="leadgen-restart", daemon=True).start()


@app.get("/api/admin/restart-info")
def api_admin_restart_info():
    """Подсказывает админу, какой механизм перезапуска сейчас активен."""
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    info = _restart_method_summary()
    info["available"] = info["method"] != "none"
    return jsonify(info)


@app.post("/api/admin/restart")
@limiter.limit("3 per minute", methods=["POST"])
def api_admin_restart():
    """Запланировать перезапуск процесса веб-приложения.

    Доступ строго ролью admin (платформенный администратор). HTTP-ответ
    отправляется немедленно, фактический перезапуск выполняется фоновой
    нитью через 1–2 секунды, чтобы клиент успел получить подтверждение.
    """
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    info = _restart_method_summary()
    if info["method"] == "none":
        return (
            jsonify(
                {
                    "message": (
                        "Перезапуск не настроен. Задайте в окружении одну из переменных: "
                        "LEADGEN_RESTART_COMMAND (например, 'systemctl restart leadgen.service'), "
                        "LEADGEN_RESTART_TOUCH_FILE, LEADGEN_RESTART_PARENT_SIGNAL или "
                        "LEADGEN_ALLOW_SELF_EXEC=1 для одиночного процесса."
                    ),
                    "method": "none",
                }
            ),
            409,
        )
    user = _session_user()
    if user is not None:
        try:
            _audit_login(int(user["id"]), "admin_restart_requested", email=str(user["email"] or ""))
        except Exception:  # noqa: BLE001
            pass
    _schedule_restart_async(info)
    return jsonify(
        {
            "message": (
                f"Перезапуск запланирован: метод «{info['method']}». "
                "Сайт станет недоступен на несколько секунд — это нормально."
            ),
            "method": info["method"],
        }
    )


@app.post("/api/admin/manager/assign-org")
@limiter.limit("30 per minute", methods=["POST"])
def admin_manager_assign_org():
    """Назначить менеджера в организацию клиента (одна org на менеджера)."""
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    body = request.get_json(force=True, silent=True) or {}
    try:
        user_id = int(body.get("user_id", 0) or 0)
        org_id = int(body.get("org_id", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"message": "Некорректные user_id / org_id"}), 400
    if user_id <= 0 or org_id <= 0:
        return jsonify({"message": "Нужны user_id и org_id > 0"}), 400
    with _db() as conn:
        u = conn.execute("SELECT id, role, email FROM users WHERE id = ?", (user_id,)).fetchone()
        if not u:
            return jsonify({"message": "Пользователь не найден"}), 404
        if str(u["role"]) != "manager":
            return jsonify({"message": "Назначать org можно только пользователю с ролью manager"}), 400
        o = conn.execute("SELECT id FROM orgs WHERE id = ?", (org_id,)).fetchone()
        if not o:
            return jsonify({"message": "Организация не найдена"}), 404
        conn.execute("DELETE FROM memberships WHERE user_id = ?", (user_id,))
        conn.execute(
            "INSERT INTO memberships(user_id, org_id, role, created_at) VALUES (?, ?, ?, ?)",
            (user_id, org_id, "manager", _now_iso()),
        )
    return jsonify({"message": f"Менеджер #{user_id} назначен в org {org_id}", "org_id": org_id})


@app.post("/api/admin/membership/set")
@limiter.limit("30 per minute", methods=["POST"])
def admin_membership_set():
    """Задать единственную membership для пользователя: org_id + роль (admin/manager/client/tester)."""
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    body = request.get_json(force=True, silent=True) or {}
    try:
        user_id = int(body.get("user_id", 0) or 0)
        org_id = int(body.get("org_id", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"message": "Некорректные user_id / org_id"}), 400
    role = str(body.get("role", "")).strip()
    if role not in ("admin", "manager", "client", "tester"):
        return jsonify({"message": "role должна быть admin, manager, client или tester"}), 400
    if user_id <= 0 or org_id <= 0:
        return jsonify({"message": "Нужны user_id и org_id > 0"}), 400
    with _db() as conn:
        u = conn.execute("SELECT id FROM users WHERE id = ?", (user_id,)).fetchone()
        if not u:
            return jsonify({"message": "Пользователь не найден"}), 404
        o = conn.execute("SELECT id FROM orgs WHERE id = ?", (org_id,)).fetchone()
        if not o:
            return jsonify({"message": "Организация не найдена"}), 404
        conn.execute("DELETE FROM memberships WHERE user_id = ?", (user_id,))
        conn.execute(
            "INSERT INTO memberships(user_id, org_id, role, created_at) VALUES (?, ?, ?, ?)",
            (user_id, org_id, role, _now_iso()),
        )
    return jsonify({"message": f"Пользователь #{user_id} → org {org_id}, роль {role}", "org_id": org_id, "role": role})


@app.post("/api/admin/user/role")
def admin_set_user_role():
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    payload = request.get_json(force=True, silent=True) or {}
    user_id = int(payload.get("user_id", 0) or 0)
    role = str(payload.get("role", "")).strip()
    if role not in ("admin", "manager", "user"):
        return jsonify({"message": "Invalid role"}), 400
    if user_id <= 0:
        return jsonify({"message": "Invalid user_id"}), 400
    with _db() as conn:
        conn.execute("UPDATE users SET role = ? WHERE id = ?", (role, user_id))
    _ensure_org_for_user(user_id)
    return jsonify({"message": "Role updated"})


@app.post("/api/admin/user/delete")
def admin_delete_user():
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    payload = request.get_json(force=True, silent=True) or {}
    target_id = int(payload.get("user_id", 0) or 0)
    if target_id <= 0:
        return jsonify({"message": "Некорректный user_id"}), 400
    actor = _session_user()
    assert actor is not None
    if target_id == int(actor["id"]):
        return jsonify({"message": "Нельзя удалить собственный аккаунт"}), 400

    default_oid = _default_org_id()
    avatar_to_delete: str | None = None

    with _db() as conn:
        row = conn.execute("SELECT id FROM users WHERE id = ?", (target_id,)).fetchone()
        if not row:
            return jsonify({"message": "Пользователь не найден"}), 404

        org_rows = conn.execute("SELECT org_id FROM memberships WHERE user_id = ?", (target_id,)).fetchall()
        org_ids = [int(r["org_id"]) for r in org_rows]

        av_row = conn.execute("SELECT avatar_filename FROM users WHERE id = ?", (target_id,)).fetchone()
        if av_row and av_row["avatar_filename"]:
            avatar_to_delete = str(av_row["avatar_filename"])

        conn.execute("DELETE FROM consents WHERE user_id = ?", (target_id,))
        conn.execute("DELETE FROM login_audit WHERE user_id = ?", (target_id,))
        conn.execute("DELETE FROM sessions WHERE user_id = ?", (target_id,))
        conn.execute("DELETE FROM memberships WHERE user_id = ?", (target_id,))
        conn.execute("DELETE FROM users WHERE id = ?", (target_id,))

        for oid in org_ids:
            left_row = conn.execute(
                "SELECT COUNT(*) AS c FROM memberships WHERE org_id = ?", (oid,)
            ).fetchone()
            left = int(left_row["c"]) if left_row else 0
            if left == 0 and oid != default_oid:
                _purge_org_rows(conn, oid)
                tenant = TENANTS_DIR / f"org_{oid}"
                if tenant.exists():
                    shutil.rmtree(tenant, ignore_errors=True)
                proc = bot_processes.pop(oid, None)
                if proc and proc.poll() is None:
                    proc.terminate()

    if avatar_to_delete:
        ap = AVATARS_DIR / avatar_to_delete
        try:
            ap.unlink(missing_ok=True)
        except OSError:
            pass

    return jsonify({"message": "Пользователь удалён"})


@app.post("/api/admin/user/block")
def admin_block_user():
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    payload = request.get_json(force=True, silent=True) or {}
    user_id = int(payload.get("user_id", 0) or 0)
    status = str(payload.get("status", "")).strip()
    if status not in ("active", "banned"):
        return jsonify({"message": "Invalid status"}), 400
    if user_id <= 0:
        return jsonify({"message": "Invalid user_id"}), 400

    org_id, _r = _user_org_role(user_id)
    with _db() as conn:
        conn.execute(
            """
            INSERT INTO subscriptions(org_id, plan_id, status, renew_at, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(org_id) DO UPDATE SET status=excluded.status
            """,
            (org_id, "free", status, _now_iso(), _now_iso()),
        )

    proc = bot_processes.get(org_id)
    if proc and proc.poll() is None and status == "banned":
        proc.terminate()
    return jsonify({"message": "Updated", "org_id": org_id, "status": status})


@app.get("/api/admin/subscription")
def admin_subscription_get():
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    org_id = _default_org_id()
    plan = _get_plan_for_org(org_id)
    if not plan:
        return jsonify({"message": "No subscription"}), 404
    return jsonify({"org_id": org_id, "plan": dict(plan)})


@app.post("/api/admin/subscription")
def admin_subscription_set():
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    payload = request.get_json(force=True, silent=True) or {}
    plan_id = str(payload.get("plan_id", "")).strip()
    status = str(payload.get("status", "")).strip() or "active"
    if plan_id not in ("free", "pro", "pro_plus"):
        return jsonify({"message": "Invalid plan_id"}), 400
    if status not in ("active", "trial", "paused", "expired", "banned"):
        return jsonify({"message": "Invalid status"}), 400
    org_id = _default_org_id()
    with _db() as conn:
        conn.execute(
            """
            INSERT INTO subscriptions(org_id, plan_id, status, renew_at, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(org_id) DO UPDATE SET plan_id=excluded.plan_id, status=excluded.status, renew_at=excluded.renew_at
            """,
            (org_id, plan_id, status, _now_iso(), _now_iso()),
        )
    return jsonify({"message": "Subscription updated", "org_id": org_id, "plan_id": plan_id, "status": status})


@app.post("/api/admin/user/subscription")
def admin_user_subscription_set():
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    payload = request.get_json(force=True, silent=True) or {}
    user_id = int(payload.get("user_id", 0) or 0)
    plan_id = str(payload.get("plan_id", "")).strip()
    status = str(payload.get("status", "")).strip() or "active"
    if user_id <= 0:
        return jsonify({"message": "Invalid user_id"}), 400
    if plan_id not in ("free", "pro", "pro_plus"):
        return jsonify({"message": "Invalid plan_id"}), 400
    if status not in ("active", "trial", "paused", "expired", "banned"):
        return jsonify({"message": "Invalid status"}), 400
    org_id, _role = _user_org_role(user_id)
    with _db() as conn:
        conn.execute(
            """
            INSERT INTO subscriptions(org_id, plan_id, status, renew_at, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(org_id) DO UPDATE SET plan_id=excluded.plan_id, status=excluded.status, renew_at=excluded.renew_at
            """,
            (org_id, plan_id, status, _now_iso(), _now_iso()),
        )
    proc = bot_processes.get(org_id)
    if proc and proc.poll() is None and status == "banned":
        proc.terminate()
    return jsonify(
        {"message": "Подписка обновлена", "org_id": org_id, "plan_id": plan_id, "status": status}
    )


@app.get("/api/admin/overview")
def admin_overview():
    ok, err = require_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code

    with _db() as conn:
        users_total = int(conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"])
        subs = conn.execute(
            """
            SELECT s.org_id, s.plan_id, s.status, s.renew_at,
                   p.title, p.price_rub_month, p.max_chats, p.max_dm_day, p.max_dm_month, p.monitor_interval_min_sec
            FROM subscriptions s
            JOIN plans p ON p.id = s.plan_id
            ORDER BY s.org_id
            """
        ).fetchall()

    subs_by_status: dict[str, int] = {}
    revenue_rub_month = 0
    active_orgs = 0
    for s in subs:
        st = str(s["status"])
        subs_by_status[st] = subs_by_status.get(st, 0) + 1
        if st in ("active", "trial"):
            active_orgs += 1
            revenue_rub_month += int(s["price_rub_month"])

    # Aggregate processed events from tenant CSVs
    leads_rows_total = 0
    tenants = []
    if TENANTS_DIR.exists():
        for org_dir in sorted(TENANTS_DIR.glob("org_*")):
            org_id_raw = org_dir.name.replace("org_", "")
            if not org_id_raw.isdigit():
                continue
            org_id = int(org_id_raw)
            csv_path = org_dir / "sent_leads.csv"
            rows = _count_csv_rows(csv_path)
            leads_rows_total += rows
            tenants.append({"org_id": org_id, "csv_rows": rows})

    return jsonify(
        {
            "users_total": users_total,
            "orgs_with_subscription": len(subs),
            "active_orgs": active_orgs,
            "revenue_rub_month": revenue_rub_month,
            "subscriptions_by_status": subs_by_status,
            "events_total_csv_rows": leads_rows_total,
            "tenants": tenants[:200],
        }
    )


@app.get("/api/config")
def get_config():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    paths = _tenant_paths(org_id)
    if not paths["config"].exists():
        if CONFIG_PATH.exists():
            _bootstrap_tenant_config_from_root(paths["config"])
        else:
            return jsonify({"error": "config.json не найден"}), 404
    cfg_raw = json.loads(paths["config"].read_text(encoding="utf-8"))
    if not isinstance(cfg_raw, dict):
        return jsonify({"error": "config повреждён"}), 500
    _in_memory_telegram_accounts_migrate(cfg_raw, org_id)
    cfg_out = _redact_config_for_client(cfg_raw, _role)
    if _role == "admin":
        cfg_out["llm_prompts"] = effective_llm_prompts(cfg_raw)
    return jsonify(cfg_out)


@app.post("/api/config")
def save_config():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, org_role = _user_org_role(int(user["id"]))
    payload = request.get_json(force=True, silent=True)
    if not isinstance(payload, dict):
        return jsonify({"message": "Неверный JSON"}), 400
    _merge_non_admin_llm_from_disk(org_id, payload, org_role)
    if org_role == "tester":
        _merge_telegram_credentials_from_disk(org_id, payload)
    paths = _tenant_paths(org_id)
    if org_role == "admin":
        prev_prompts: dict[str, Any] = {}
        if paths["config"].exists():
            try:
                prev_cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
                if isinstance(prev_cfg.get("llm_prompts"), dict):
                    prev_prompts = dict(prev_cfg["llm_prompts"])
            except Exception:
                prev_prompts = {}
        submitted = payload.get("llm_prompts")
        if isinstance(submitted, dict):
            payload["llm_prompts"] = normalize_llm_prompts_for_save(submitted, DEFAULT_LLM_PROMPTS)
        else:
            payload["llm_prompts"] = prev_prompts
    tg_err = _normalize_telegram_accounts_on_save(payload, org_id)
    if tg_err:
        return jsonify({"message": tg_err}), 400
    plan = _get_plan_for_org(org_id)
    if plan:
        payload = _enforce_plan_on_config(payload, plan)
    paths["config"].write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return jsonify({"message": "config.json сохранен"})


@app.get("/api/chats/overview")
def chats_overview():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    paths = _tenant_paths(org_id)
    cfg: dict[str, Any] = {}
    if paths["config"].exists():
        try:
            cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
        except Exception:
            cfg = {}
    st: dict[str, Any] = {}
    if paths["state"].exists():
        try:
            st = json.loads(paths["state"].read_text(encoding="utf-8"))
        except Exception:
            st = {}
    last_seen = st.get("last_seen_msg_id", {})
    if not isinstance(last_seen, dict):
        last_seen = {}
    chats_raw = cfg.get("target_chats", [])
    chats = []
    if isinstance(chats_raw, list):
        for c in chats_raw:
            key = str(c)
            chats.append(
                {
                    "chat": c,
                    "chat_key": key,
                    "last_seen_msg_id": int(last_seen.get(key, 0) or 0),
                }
            )
    llm = cfg.get("llm", {})
    # Как в эндпоинтах генерации: model может быть пустым — тогда подставляется gpt-4o-mini.
    llm_on = bool(
        isinstance(llm, dict)
        and llm.get("enabled")
        and str(llm.get("api_key", "")).strip()
    )
    return jsonify({"org_id": org_id, "chats": chats, "llm_ready": llm_on})


@app.post("/api/chats/dialogs-compare")
@limiter.limit("8 per minute", methods=["POST"])
def api_chats_dialogs_compare():
    """Диалоги по TG-аккаунтам; при ≥2 аккаунтах — пересечение (чат есть у каждого). Склейка @username и -100… по merge_key из бота."""
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))

    proc = bot_processes.get(org_id)
    if proc and proc.poll() is None:
        return (
            jsonify(
                {"message": "Остановите бота — запрос использует ту же сессию Telegram."},
            ),
            409,
        )

    body = request.get_json(force=True, silent=True) or {}
    try:
        dialog_limit = int(body.get("limit", 500) or 500)
    except (TypeError, ValueError):
        dialog_limit = 500
    dialog_limit = max(10, min(2000, dialog_limit))

    paths = _tenant_paths(org_id)
    if not paths["config"].exists() and CONFIG_PATH.exists():
        _bootstrap_tenant_config_from_root(paths["config"])
    if not paths["config"].exists():
        return jsonify({"message": "Нет config для организации"}), 400
    try:
        cfg_for_sess = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        return jsonify({"message": "config повреждён"}), 400
    if isinstance(cfg_for_sess, dict):
        _in_memory_telegram_accounts_migrate(cfg_for_sess, org_id)

    raw_aid = body.get("account_ids")
    account_ids: list[str] = []
    if isinstance(raw_aid, list):
        for x in raw_aid:
            s = str(x).strip()
            if s and s not in account_ids:
                account_ids.append(s)

    login_code_file = paths["base"] / ".telegram_login_code"
    errors: list[str] = []
    by_aid: dict[str, list[dict[str, Any]]] = {}
    account_order: list[str] = []

    def run_list_chats_one(session_path: Path) -> dict[str, Any]:
        return run_bot_json_stdout(
            [
                sys.executable,
                str(BOT_SCRIPT),
                "--force-unlock",
                *_bot_argv_paths_and_session(paths, session_path),
                "--login-code-file",
                str(login_code_file),
                "--list-chats-json",
                "--list-chats-limit",
                str(dialog_limit),
            ]
        )

    if len(account_ids) > 1:
        cap = _effective_max_telegram_accounts(org_id)
        use_ids = account_ids[:cap]

        def run_one(aid: str) -> tuple[str, dict[str, Any]]:
            sp = _session_path_for_telegram_account(org_id, cfg_for_sess, aid)
            return aid, run_list_chats_one(sp)

        # Подряд по аккаунтам: один общий login_code_file — параллельный запуск смешивает коды входа.
        for aid in use_ids:
            try:
                _r, payload = run_one(aid)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{aid}: {exc}")
                continue
            if not payload.get("ok"):
                errors.append(f"{aid}: {payload.get('error', 'ошибка')}")
                continue
            raw_items = payload.get("items")
            if not isinstance(raw_items, list):
                raw_items = []
            clean = [it for it in raw_items if isinstance(it, dict)]
            by_aid[aid] = clean
        account_order = [a for a in use_ids if a in by_aid]
    else:
        if len(account_ids) == 1:
            acc_for_session: str | None = account_ids[0]
        else:
            acc_raw = body.get("telegram_account_id")
            acc_for_session = (
                str(acc_raw).strip() if acc_raw is not None and str(acc_raw).strip() != "" else None
            )
        aid_key = str(acc_for_session) if acc_for_session else "active"
        session_path = _session_path_for_telegram_account(org_id, cfg_for_sess, acc_for_session)
        payload = run_list_chats_one(session_path)
        if not payload.get("ok"):
            return jsonify({"message": str(payload.get("error", "Ошибка"))[:800]}), 502
        raw_items = payload.get("items")
        if not isinstance(raw_items, list):
            raw_items = []
        by_aid[aid_key] = [it for it in raw_items if isinstance(it, dict)]
        account_order = [aid_key]

    merged: dict[str, dict[str, Any]] = {}
    for aid in account_order:
        for it in by_aid.get(aid, []):
            if not isinstance(it, dict):
                continue
            ref_raw = it.get("ref")
            if not ref_raw:
                continue
            bk = _dialog_merge_bucket_key(it)
            if not bk:
                continue
            if bk not in merged:
                merged[bk] = {
                    "key": bk,
                    "ref": str(ref_raw),
                    "title": str(it.get("title") or ""),
                    "is_broadcast": bool(it.get("is_broadcast")),
                    "is_megagroup": bool(it.get("is_megagroup")),
                    "_in": set(),
                }
            merged[bk]["_in"].add(aid)
            new_ref = str(ref_raw).strip()
            old_ref = merged[bk]["ref"]
            if new_ref.startswith("@"):
                merged[bk]["ref"] = new_ref
            elif not old_ref.startswith("@") and new_ref:
                merged[bk]["ref"] = new_ref
            if not merged[bk]["title"] and it.get("title"):
                merged[bk]["title"] = str(it.get("title"))

    items_out: list[dict[str, Any]] = []
    multi = len(account_order) > 1
    n_acc = len(account_order)
    for k in sorted(merged.keys(), key=lambda x: (merged[x]["title"].lower(), x)):
        row = merged[k]
        present = row["_in"]
        if multi and len(present) != n_acc:
            continue
        accounts_map = {a: (a in present) for a in account_order}
        kind = "channel" if row["is_broadcast"] else ("supergroup" if row["is_megagroup"] else "group")
        items_out.append(
            {
                "key": row["key"],
                "ref": row["ref"],
                "title": row["title"],
                "kind": kind,
                "accounts": accounts_map,
            }
        )

    if not items_out and errors and len(account_order) <= len(errors):
        return jsonify({"message": "; ".join(errors)[:800]}), 502

    return jsonify({"items": items_out, "account_ids": account_order, "errors": errors or None})


@app.post("/api/chats/leave")
@limiter.limit("6 per minute", methods=["POST"])
def api_chats_leave():
    """Выйти из выбранных групп/каналов на указанном TG-аккаунте (delete_dialog в Telethon)."""
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))

    proc = bot_processes.get(org_id)
    if proc and proc.poll() is None:
        return (
            jsonify(
                {"message": "Остановите бота — отписка использует ту же сессию Telegram."},
            ),
            409,
        )

    body = request.get_json(force=True, silent=True) or {}
    raw_refs = body.get("refs")
    if not isinstance(raw_refs, list) or not raw_refs:
        return jsonify({"message": "Передайте refs: непустой массив (@channel или -100…)"}), 400
    refs = [str(x).strip() for x in raw_refs if str(x).strip()]
    if not refs:
        return jsonify({"message": "Передайте refs: непустой массив"}), 400
    if len(refs) > 50:
        return jsonify({"message": "Не более 50 чатов за один запрос"}), 400

    acc_raw = body.get("telegram_account_id")
    acc_for_session = str(acc_raw).strip() if acc_raw is not None and str(acc_raw).strip() != "" else None
    remove_from_monitoring = bool(body.get("remove_from_monitoring", False))

    paths = _tenant_paths(org_id)
    if not paths["config"].exists():
        return jsonify({"message": "Нет config для организации"}), 400
    try:
        cfg_for_sess = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        return jsonify({"message": "config повреждён"}), 400
    if isinstance(cfg_for_sess, dict):
        _in_memory_telegram_accounts_migrate(cfg_for_sess, org_id)

    login_code_file = paths["base"] / ".telegram_login_code"
    session_path = _session_path_for_telegram_account(org_id, cfg_for_sess, acc_for_session)
    payload = run_bot_json_stdout(
        [
            sys.executable,
            str(BOT_SCRIPT),
            "--force-unlock",
            *_bot_argv_paths_and_session(paths, session_path),
            "--login-code-file",
            str(login_code_file),
            "--leave-chats-json",
            json.dumps(refs, ensure_ascii=False, separators=(",", ":")),
        ]
    )
    if not payload.get("ok"):
        return jsonify({"message": str(payload.get("error", "Ошибка отписки"))[:800]}), 502

    removed_m = 0
    if remove_from_monitoring:
        try:
            cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
        except Exception:
            cfg = {}
        rm_keys = {_dialog_compare_key(r) for r in refs if _dialog_compare_key(r)}
        if isinstance(cfg.get("target_chats"), list) and rm_keys:
            new_tc: list[Any] = []
            for x in cfg["target_chats"]:
                if _dialog_compare_key(str(x)) not in rm_keys:
                    new_tc.append(x)
                else:
                    removed_m += 1
            cfg["target_chats"] = new_tc
            paths["config"].write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    return jsonify(
        {
            "message": "Готово",
            "results": payload.get("results"),
            "ok_count": payload.get("ok_count"),
            "removed_from_monitoring": removed_m if remove_from_monitoring else None,
        }
    )


@app.post("/api/chats/sync-dialogs")
@limiter.limit("8 per minute", methods=["POST"])
def api_chats_sync_dialogs():
    """Добавить в target_chats все каналы/группы из диалогов аккаунта (Telethon). Несколько account_ids — параллельно, слияние без дубликатов."""
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))

    proc = bot_processes.get(org_id)
    if proc and proc.poll() is None:
        return (
            jsonify(
                {
                    "message": "Остановите бота — синхронизация чатов использует ту же сессию Telegram.",
                }
            ),
            409,
        )

    body = request.get_json(force=True, silent=True) or {}
    mode = str(body.get("mode", "merge") or "merge").strip().lower()
    if mode not in ("merge", "replace"):
        mode = "merge"
    try:
        dialog_limit = int(body.get("limit", 500) or 500)
    except (TypeError, ValueError):
        dialog_limit = 500
    dialog_limit = max(10, min(2000, dialog_limit))

    paths = _tenant_paths(org_id)
    if not paths["config"].exists() and CONFIG_PATH.exists():
        _bootstrap_tenant_config_from_root(paths["config"])
    if not paths["config"].exists():
        return jsonify({"message": "Нет config для организации"}), 400

    try:
        cfg_for_sess = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        cfg_for_sess = {}
    if isinstance(cfg_for_sess, dict):
        _in_memory_telegram_accounts_migrate(cfg_for_sess, org_id)

    raw_aid = body.get("account_ids")
    account_ids: list[str] = []
    if isinstance(raw_aid, list):
        for x in raw_aid:
            s = str(x).strip()
            if s and s not in account_ids:
                account_ids.append(s)

    login_code_file = paths["base"] / ".telegram_login_code"
    by_account_in_run: dict[str, int] = {}
    errors: list[str] = []
    ordered_unique: list[Any] = []
    sync_n_accounts = 1
    account_ids_capped = False

    if len(account_ids) > 1:
        cap = _effective_max_telegram_accounts(org_id)
        use_ids = account_ids[:cap]
        account_ids_capped = len(account_ids) > cap
        sync_n_accounts = len(use_ids)
        seen_keys: set[str] = set()

        def run_list_chats_one(aid: str) -> tuple[str, dict[str, Any]]:
            sp = _session_path_for_telegram_account(org_id, cfg_for_sess, aid)
            try:
                with _tg_session_lock(sp, action="sync_dialogs"):
                    out = run_bot_json_stdout(
                        [
                            sys.executable,
                            str(BOT_SCRIPT),
                            "--force-unlock",
                            *_bot_argv_paths_and_session(paths, sp),
                            "--login-code-file",
                            str(login_code_file),
                            "--list-chats-json",
                            "--list-chats-limit",
                            str(dialog_limit),
                        ],
                        timeout_sec=900,
                    )
            except TimeoutError as exc:
                out = {"ok": False, "error": _humanize_tg_session_lock_error(exc)}
            return aid, out

        for aid in use_ids:
            try:
                _rid, payload = run_list_chats_one(aid)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{aid}: {exc}")
                continue
            if not payload.get("ok"):
                errors.append(f"{aid}: {payload.get('error', 'ошибка')}")
                continue
            raw_items = payload.get("items")
            if not isinstance(raw_items, list):
                raw_items = []
            per = _list_chats_json_items_to_unique_norms(raw_items)
            by_account_in_run[aid] = len(per)
            for n in per:
                k = str(n)
                if k in seen_keys:
                    continue
                seen_keys.add(k)
                ordered_unique.append(n)
        if not ordered_unique and len(errors) >= len(use_ids):
            return jsonify({"message": "; ".join(errors)[:800]}), 502
    else:
        if len(account_ids) == 1:
            acc_for_session: str | None = account_ids[0]
        else:
            acc_raw = body.get("telegram_account_id")
            acc_for_session = (
                str(acc_raw).strip() if acc_raw is not None and str(acc_raw).strip() != "" else None
            )
        session_path = _session_path_for_telegram_account(org_id, cfg_for_sess, acc_for_session)
        try:
            with _tg_session_lock(session_path, action="sync_dialogs"):
                payload = run_bot_json_stdout(
                    [
                        sys.executable,
                        str(BOT_SCRIPT),
                        "--force-unlock",
                        *_bot_argv_paths_and_session(paths, session_path),
                        "--login-code-file",
                        str(login_code_file),
                        "--list-chats-json",
                        "--list-chats-limit",
                        str(dialog_limit),
                    ],
                    timeout_sec=900,
                )
        except TimeoutError as exc:
            return jsonify({"message": _humanize_tg_session_lock_error(exc)}), 409
        if not payload.get("ok"):
            return jsonify({"message": str(payload.get("error", "Ошибка синхронизации"))[:800]}), 502
        raw_items = payload.get("items")
        if not isinstance(raw_items, list):
            raw_items = []
        ordered_unique = _list_chats_json_items_to_unique_norms(raw_items)
        acc_label = str(acc_for_session or "active")
        by_account_in_run[acc_label] = len(ordered_unique)

    try:
        cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        return jsonify({"message": "config повреждён"}), 400
    if isinstance(cfg, dict):
        _in_memory_telegram_accounts_migrate(cfg, org_id)

    plan = _get_plan_for_org(org_id)
    if mode == "replace":
        merged: list[Any] = list(ordered_unique)
    else:
        existing = cfg.get("target_chats", [])
        if not isinstance(existing, list):
            existing = []
        ex_seen: set[str] = {str(x) for x in existing}
        merged = list(existing)
        for n in ordered_unique:
            k = str(n)
            if k in ex_seen:
                continue
            ex_seen.add(k)
            merged.append(n)

    cfg["target_chats"] = merged
    before_trim = len(merged) if isinstance(merged, list) else 0
    if plan:
        cfg = _enforce_plan_on_config(cfg, plan)
    final_list = cfg.get("target_chats", [])
    if not isinstance(final_list, list):
        final_list = []
    trimmed = before_trim - len(final_list)
    paths["config"].write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    if len(account_ids) > 1:
        parts = [f"{k}: {v}" for k, v in sorted(by_account_in_run.items(), key=lambda x: str(x[0]))]
        acct_h = f" ({'; '.join(parts)})" if parts else ""
        if account_ids_capped:
            cap = _effective_max_telegram_accounts(org_id)
            base_msg = (
                f"Синхронизация: по тарифу обработано {sync_n_accounts} из {len(account_ids)} акк. (лимит {cap}); "
                f"уникально после слияния {len(ordered_unique)} чат(ов){acct_h} · "
            )
        else:
            base_msg = (
                f"Синхронизация {sync_n_accounts} акк.: уникально после слияния {len(ordered_unique)} чат(ов){acct_h} · "
            )
    else:
        base_msg = f"Синхронизация: найдено в диалогах {len(ordered_unique)} чат(ов) · "

    msg = base_msg + f"в списке мониторинга сейчас {len(final_list)}. Режим: {mode}."
    if errors:
        msg += f" Предупреждения: {'; '.join(errors)[:400]}"
    if trimmed > 0:
        msg += f" По лимиту тарифа обрезано: {trimmed}."
    return jsonify(
        {
            "message": msg,
            "dialog_chats_found": len(ordered_unique),
            "by_account": by_account_in_run or None,
            "errors": errors or None,
            "target_chats_count": len(final_list),
            "trimmed_due_to_plan": trimmed,
            "mode": mode,
        }
    )


@app.post("/api/chats/suggest-offer")
@limiter.limit("20 per minute", methods=["POST"])
def chats_suggest_offer():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(force=True, silent=True) or {}
    chat_key = str(body.get("chat_key", "")).strip() or "—"
    snippet = str(body.get("lead_snippet", "")).strip()
    paths = _tenant_paths(org_id)
    if not paths["config"].exists():
        return jsonify({"message": "Нет config для организации"}), 400
    try:
        cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        return jsonify({"message": "config повреждён"}), 400
    llm = cfg.get("llm", {})
    if not isinstance(llm, dict) or not llm.get("enabled"):
        return jsonify({"message": "LLM выключен в настройках"}), 400
    api_key = str(llm.get("api_key", "")).strip()
    if not api_key:
        return jsonify({"message": "Не задан llm.api_key"}), 400
    base_url = str(llm.get("base_url", "") or "https://api.openai.com/v1").strip()
    model = str(llm.get("model", "") or "gpt-4o-mini").strip()
    partner = str(cfg.get("partner_name", "") or "партнёр").strip()
    stage1_hint = str((cfg.get("templates") or {}).get("stage1", "") or "")[:800]
    pr = effective_llm_prompts(cfg)
    system = pr["chats_suggest_offer_system"]
    user_msg = format_llm_prompt(
        pr["chats_suggest_offer_user"],
        partner=partner,
        chat_key=chat_key,
        snippet=snippet
        or "(не указан — предложи нейтральное вежливое первое касание)",
        stage1_hint=stage1_hint or "—",
    )
    try:
        text = _openai_chat_completion(base_url, api_key, model, system, user_msg)
    except ValueError as exc:
        return jsonify({"message": str(exc)}), 502
    return jsonify({"text": text})


@app.post("/api/assistant/ask")
@limiter.limit("30 per minute", methods=["POST"])
def api_assistant_ask():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(force=True, silent=True) or {}
    question = str(body.get("question", "")).strip()
    page = str(body.get("page", "") or "").strip() or "—"
    if len(question) < 2:
        return jsonify({"message": "Введите вопрос (не короче 2 символов)."}), 400
    paths = _tenant_paths(org_id)
    cfg: dict[str, Any] = {}
    if paths["config"].exists():
        try:
            raw_cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
            if isinstance(raw_cfg, dict):
                cfg = raw_cfg
        except Exception:
            cfg = {}
    llm = cfg.get("llm", {})
    llm_on = (
        isinstance(llm, dict)
        and bool(llm.get("enabled"))
        and str(llm.get("api_key", "")).strip()
    )
    if not llm_on:
        fb = (
            "LLM в настройках организации не подключён. Откройте «Настройки и лимиты» → «Подключения», "
            "включите LLM и укажите ключ. Пока могу кратко: это панель Leadgen — воронка (поиск каналов, бот, лиды, "
            "офферы с согласованием, переписка, созвоны), аналитика и подписка. Точный ответ по вашему вопросу "
            "появится после настройки модели."
        )
        return jsonify({"answer": fb, "fallback": True})

    api_key = str(llm.get("api_key", "")).strip()
    base_url = str(llm.get("base_url", "") or "https://api.openai.com/v1").strip()
    model = str(llm.get("model", "") or "gpt-4o-mini").strip()
    pr = effective_llm_prompts(cfg)
    system = pr["assistant_system"]
    user_msg = format_llm_prompt(pr["assistant_user"], page=page, question=question)
    try:
        answer = _openai_chat_completion(base_url, api_key, model, system, user_msg, temperature=0.5)
    except ValueError as exc:
        return jsonify({"message": str(exc), "fallback": True}), 502
    return jsonify({"answer": answer, "fallback": False})


@app.get("/api/logs")
def get_logs():
    # Поддержка ?tail=1000 — отдаём только последние N строк, чтобы не возить мегабайты по сети.
    try:
        tail = int(request.args.get("tail") or 0)
    except (TypeError, ValueError):
        tail = 0
    tail = max(0, min(5000, tail))
    max_bytes = 150_000 if tail == 0 else 1_500_000
    user = _session_user()
    if user is None:
        return jsonify({"log": _redact_secrets_in_text(read_log_tail(max_bytes=max_bytes, max_lines=tail or None))})
    org_id, _role = _user_org_role(int(user["id"]))
    paths = _tenant_paths(org_id)
    if paths["log"].exists():
        global LOG_PATH
        old = LOG_PATH
        try:
            LOG_PATH = paths["log"]
            return jsonify({"log": _redact_secrets_in_text(read_log_tail(max_bytes=max_bytes, max_lines=tail or None))})
        finally:
            LOG_PATH = old
    return jsonify({"log": _redact_secrets_in_text(read_log_tail(max_bytes=max_bytes, max_lines=tail or None))})


@app.get("/api/stats/summary")
def stats_summary():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    paths = _tenant_paths(org_id)

    cfg = {}
    if paths["config"].exists():
        try:
            cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
        except Exception:
            cfg = {}
    st = {}
    if paths["state"].exists():
        try:
            st = json.loads(paths["state"].read_text(encoding="utf-8"))
        except Exception:
            st = {}

    target_chats = cfg.get("target_chats", [])
    contacted = st.get("contacted_users", [])
    blacklist = st.get("blacklist_users", [])
    daily_sent = int(st.get("daily_sent_count", 0) or 0)
    daily_limit = int(st.get("daily_limit", 0) or 0)
    monthly_sent = int(st.get("monthly_sent_count", 0) or 0)
    current_month = str(st.get("current_month", "") or "")
    csv_rows = _count_csv_rows(paths["csv"])

    calls_summary = {"scheduled": 0, "done": 0, "won": 0}
    with _db() as conn:
        for key, outcome in (("scheduled", "planned"), ("done", "done"), ("won", "won")):
            crow = conn.execute(
                "SELECT COUNT(*) AS c FROM calls WHERE org_id = ? AND COALESCE(outcome, '') = ?",
                (org_id, outcome),
            ).fetchone()
            calls_summary[key] = int(crow["c"] if crow else 0)

    limits = cfg.get("limits", {}) if isinstance(cfg.get("limits", {}), dict) else {}
    dm_day = limits.get("daily_limit_range", [])
    max_dm_month = limits.get("max_dm_month", None)
    monitor_interval = limits.get("monitor_interval_sec", None)

    plan = _get_plan_for_org(org_id)
    plan_payload: dict[str, Any] | None = None
    if plan:
        plan_payload = {
            "id": str(plan["id"]),
            "max_chats": int(plan["max_chats"]),
            "max_dm_day": int(plan["max_dm_day"]),
            "max_dm_month": int(plan["max_dm_month"]),
            "monitor_interval_min_sec": int(plan["monitor_interval_min_sec"]),
            "max_telegram_accounts": _effective_max_telegram_accounts(org_id),
        }

    return jsonify(
        {
            "org_id": org_id,
            "target_chats": len(target_chats) if isinstance(target_chats, list) else 0,
            "contacted_users": len(contacted) if isinstance(contacted, list) else 0,
            "blacklist_users": len(blacklist) if isinstance(blacklist, list) else 0,
            "daily_sent_count": daily_sent,
            "daily_limit": daily_limit,
            "monthly_sent_count": monthly_sent,
            "current_month": current_month,
            "leads_rows_total": csv_rows,
            "calls_summary": calls_summary,
            "limits": {
                "monitor_interval_sec": monitor_interval,
                "daily_limit_range": dm_day,
                "max_dm_month": max_dm_month,
            },
            "plan": plan_payload,
        }
    )


@app.get("/api/stats/leads-timeline")
def stats_leads_timeline():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    try:
        days = int(request.args.get("days", "30") or 30)
    except (TypeError, ValueError):
        days = 30
    days = max(1, min(90, days))
    paths = _tenant_paths(org_id)
    series = _leads_csv_counts_by_day(paths["csv"], days)
    return jsonify({"days": days, "series": series})


@app.get("/api/llm/presets")
def api_llm_presets():
    """Публичный список пресетов для формы LLM (без секретов)."""
    return jsonify({"presets": LLM_PRESETS})


def _leads_csv_export_bytes(path: Path, *, include_deleted: bool) -> bytes:
    fieldnames = _leads_csv_fieldnames_union(path)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    if not path.is_file():
        return buf.getvalue().encode("utf-8")
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                norm = {k: str(row.get(k) or "") for k in fieldnames}
                if not include_deleted and _csv_row_deleted(norm):
                    continue
                w.writerow(norm)
    except OSError:
        pass
    return buf.getvalue().encode("utf-8")


@app.get("/api/leads/export")
def export_leads_csv():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    paths = _tenant_paths(org_id)
    csv_path = paths["csv"]
    fn = f"sent_leads_org{org_id}.csv"
    inc = str(request.args.get("include_deleted") or "").strip().lower() in ("1", "true", "yes")
    data = _leads_csv_export_bytes(csv_path, include_deleted=inc)
    return Response(
        data,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fn}"'},
    )


@app.get("/api/bot/status")
def bot_status():
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"status": "Остановлен"})
    org_id, _role = ctx
    info = _bot_run_status(int(org_id))
    proc = bot_processes.get(org_id)
    if proc and proc.poll() is None:
        return jsonify({"status": f"Запущен (org={org_id}, pid={proc.pid})"})
    if info.get("alive"):
        if info.get("stale"):
            return jsonify({"status": f"Не отвечает (org={org_id}, pid={info.get('pid')})"})
        return jsonify({"status": f"Запущен (org={org_id}, pid={info.get('pid')})"})
    if proc and proc.poll() is not None:
        return jsonify({"status": f"Остановлен (org={org_id}, exit={proc.poll()})"})
    return jsonify({"status": f"Остановлен (org={org_id})"})


@app.post("/api/bot/start")
def start_bot():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, role = _user_org_role(int(user["id"]))

    plan = _get_plan_for_org(org_id)
    if plan and str(plan["sub_status"]) not in ("active", "trial"):
        return jsonify({"message": "Подписка не активна"}), 402

    paths = _tenant_paths(org_id)
    if not paths["config"].exists() and CONFIG_PATH.exists():
        _bootstrap_tenant_config_from_root(paths["config"])

    if plan and paths["config"].exists():
        cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
        cfg = _enforce_plan_on_config(cfg, plan)
        paths["config"].write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    existing = bot_processes.get(org_id)
    if existing and existing.poll() is None:
        return jsonify({"message": "Бот уже запущен"})
    # Проверка кросс-воркер: возможно, бот стартанул в другом Gunicorn worker.
    if _bot_run_status(int(org_id)).get("alive"):
        return jsonify({"message": "Бот уже запущен"})
    try:
        cfg_run = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        cfg_run = {}
    session_path = _session_path_for_telegram_account(org_id, cfg_run, None)
    login_code_file = paths["base"] / ".telegram_login_code"
    login_code_file.unlink(missing_ok=True)
    proc = subprocess.Popen(
        [
            sys.executable,
            str(BOT_SCRIPT),
            "--run-now",
            "--force-unlock",
            "--org-id",
            str(org_id),
            "--data-db",
            str(DB_PATH.resolve()),
            *_bot_argv_paths_and_session(paths, session_path),
            "--login-code-file",
            str(login_code_file),
        ]
    )
    bot_processes[org_id] = proc
    try:
        _bot_run_register(int(org_id), int(proc.pid), phase="starting")
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: bot_runs INSERT failed: {exc}", file=sys.stderr)
    return jsonify({"message": f"Бот запущен (org={org_id}, pid={proc.pid})"})


@app.post("/api/bot/telegram-code")
def bot_submit_telegram_code():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    body = request.get_json(force=True, silent=True) or {}
    code = str(body.get("code", "")).strip()
    if not code:
        return jsonify({"message": "Введите код из Telegram"}), 400
    paths = _tenant_paths(org_id)
    code_path = paths["base"] / ".telegram_login_code"
    try:
        code_path.write_text(code, encoding="utf-8")
    except OSError as exc:
        return jsonify({"message": f"Не удалось записать код: {exc}"}), 500
    return jsonify({"message": "Код передан процессу бота"})


@app.get("/api/org-admin/debug-log")
@limiter.limit("40 per minute", methods=["GET"])
def api_org_admin_debug_log():
    """Хвост bot.log организации — admin или tester организации."""
    ok, err = require_org_role(_ORG_DEBUG_LOG_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    raw_b = request.args.get("bytes", default="120000")
    try:
        nb = max(4096, min(500_000, int(raw_b)))
    except ValueError:
        nb = 120_000
    paths = _tenant_paths(org_id)
    log_p = paths["log"]
    text = read_path_tail(log_p, max_bytes=nb, max_lines=12_000)
    hint = f"tenants/org_{org_id}/logs/bot.log"
    return jsonify({"text": _redact_secrets_in_text(text), "log_hint": hint, "org_id": org_id})


@app.get("/api/bot/scan-log")
def bot_scan_log():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    paths = _tenant_paths(org_id)
    info = _bot_run_status(int(org_id))
    proc = bot_processes.get(org_id)
    running = bool((proc and proc.poll() is None) or info.get("alive"))
    base: dict[str, Any] = {
        "running": running,
        "stale": bool(info.get("stale")),
        "last_heartbeat": info.get("last_heartbeat"),
        "scan_progress": None,
        "scan_audit_log": [],
        "org_id": org_id,
    }
    if not paths["state"].exists():
        return jsonify(base)
    try:
        st = json.loads(paths["state"].read_text(encoding="utf-8"))
    except Exception:
        return jsonify(base)
    if not isinstance(st, dict):
        return jsonify(base)
    sp = st.get("scan_progress")
    sal = st.get("scan_audit_log")
    log_list = sal if isinstance(sal, list) else []
    log_tail = ""
    log_p = paths["log"]
    if log_p.exists():
        try:
            log_tail = _redact_secrets_in_text(
                read_path_tail(log_p, max_bytes=96_000, max_lines=320)
            )
        except OSError:
            log_tail = ""
    out: dict[str, Any] = {
        **base,
        "scan_progress": sp if isinstance(sp, dict) else None,
        "scan_audit_log": log_list[-160:],
        "log_tail": log_tail,
    }
    return jsonify(out)


@app.get("/api/bot/leads")
def bot_leads():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    paths = _tenant_paths(org_id)
    raw_limit = request.args.get("limit", default="150")
    try:
        lim = max(1, min(500, int(raw_limit)))
    except ValueError:
        lim = 150
    rows = _tenant_csv_tail(paths["csv"], lim)
    return jsonify({"org_id": org_id, "rows": rows})


@app.get("/api/leads/conversations")
def api_leads_conversations():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    paths = _tenant_paths(org_id)
    rows = _tenant_csv_for_conversations(paths["csv"], max_rows=5000)
    by_uid: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        uid = str(row.get("user_id") or "").strip()
        if not uid:
            continue
        by_uid.setdefault(uid, []).append(row)
    # Подмешиваем CRM-карточку (если есть) — UI получит conversation_id, status, stage.
    conv_by_uid: dict[str, dict[str, Any]] = {}
    if by_uid:
        with _db() as conn:
            placeholders = ",".join("?" * len(by_uid))
            params: list[Any] = [org_id, *list(by_uid.keys())]
            try:
                conv_rows = conn.execute(
                    f"SELECT id, lead_user_id, status, current_stage, last_activity_at "
                    f"FROM conversations WHERE org_id = ? AND lead_user_id IN ({placeholders})",
                    tuple(params),
                ).fetchall()
            except sqlite3.Error:
                conv_rows = []
        for cr in conv_rows:
            conv_by_uid[str(cr["lead_user_id"])] = {
                "conversation_id": int(cr["id"]),
                "crm_status": str(cr["status"] or ""),
                "crm_stage": int(cr["current_stage"] or 1),
                "crm_last_activity": str(cr["last_activity_at"] or "") or None,
            }
    items: list[dict[str, Any]] = []
    for uid, lst in by_uid.items():
        lst.sort(key=lambda r: str(r.get("timestamp") or ""))
        last = lst[-1]
        msg = str(last.get("message") or "")
        item = {
            "user_id": uid,
            "username": last.get("username") or "",
            "last_timestamp": last.get("timestamp") or "",
            "last_stage": last.get("stage") or "",
            "last_status": last.get("status") or "",
            "source_chat": last.get("source_chat") or "",
            "last_message_preview": msg[:160],
            "rows_count": len(lst),
            "last_lead_tag": str(last.get("lead_tag") or "").strip(),
            "conversation_id": None,
            "crm_status": None,
            "crm_stage": None,
            "crm_last_activity": None,
        }
        item.update(conv_by_uid.get(uid, {}))
        items.append(item)
    items.sort(key=lambda x: x.get("last_timestamp") or "", reverse=True)
    return jsonify({"org_id": org_id, "items": items[:300]})


@app.post("/api/leads/delete")
@limiter.limit("30 per minute", methods=["POST"])
def api_leads_delete():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    paths = _tenant_paths(org_id)
    body = request.get_json(force=True, silent=True) or {}
    if bool(body.get("all")):
        _leads_csv_clear(paths["csv"])
        return jsonify(
            {
                "message": "Все записи помечены как скрытые (мягкое удаление). Резервная копия: sent_leads.csv.bak",
                "removed": "all",
            }
        )
    raw_ids = body.get("ids")
    if not isinstance(raw_ids, list) or not raw_ids:
        return jsonify({"message": "Передайте ids: [\"…\"] или all: true"}), 400
    drop = {str(x).strip() for x in raw_ids if str(x).strip()}
    if not drop:
        return jsonify({"message": "Пустой список ids"}), 400
    removed, kept = _leads_csv_delete_ids(paths["csv"], drop)
    return jsonify(
        {
            "message": f"Скрыто записей: {removed}. Активных в журнале: {kept}. Перед операцией создан sent_leads.csv.bak",
            "removed": removed,
            "kept": kept,
        }
    )


@app.patch("/api/leads/tag")
@limiter.limit("120 per minute", methods=["PATCH"])
def api_leads_tag_patch():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    paths = _tenant_paths(org_id)
    body = request.get_json(force=True, silent=True) or {}
    rid = str(body.get("id") or "").strip()
    raw_tag = body.get("lead_tag")
    if not isinstance(raw_tag, str):
        return jsonify({"message": "lead_tag должен быть строкой"}), 400
    tag = raw_tag.strip()
    if tag not in ALLOWED_LEAD_TAGS:
        return jsonify(
            {
                "message": "Недопустимый lead_tag. Допустимо: пусто/lead, junk, in_progress, wrote, partner.",
            }
        ), 400
    if not _leads_csv_update_lead_tag(paths["csv"], rid, tag):
        return jsonify({"message": "Строка не найдена или ошибка записи CSV"}), 404
    return jsonify({"message": "Метка сохранена", "id": rid, "lead_tag": "" if tag == "lead" else tag})


@app.get("/api/outreach")
def api_outreach_list():
    ok, err = require_org_role(_ORG_STAFF_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    st = str(request.args.get("status", "") or "").strip()
    with _db() as conn:
        if st:
            rows = conn.execute(
                "SELECT * FROM outreach_queue WHERE org_id = ? AND status = ? ORDER BY id DESC LIMIT 250",
                (org_id, st),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM outreach_queue WHERE org_id = ? ORDER BY id DESC LIMIT 250",
                (org_id,),
            ).fetchall()
    return jsonify({"org_id": org_id, "items": [dict(r) for r in rows]})


@app.get("/api/outreach/pending-count")
def api_outreach_pending_count():
    ok, err = require_org_role(_ORG_STAFF_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    with _db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM outreach_queue WHERE org_id = ? AND status = 'pending'",
            (org_id,),
        ).fetchone()
    n = int(row["c"] if row else 0)
    return jsonify({"org_id": org_id, "pending": n})


@app.patch("/api/outreach/<int:item_id>")
@limiter.limit("60 per minute", methods=["PATCH"])
def api_outreach_patch(item_id: int):
    ok, err = require_org_role(_ORG_STAFF_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(force=True, silent=True) or {}
    draft = str(body.get("draft_text", "")).strip()
    if len(draft) < 2:
        return jsonify({"message": "Текст слишком короткий"}), 400
    ts = _now_iso()
    with _db() as conn:
        cur = conn.execute(
            "UPDATE outreach_queue SET draft_text = ?, updated_at = ? WHERE id = ? AND org_id = ? AND status = 'pending'",
            (draft, ts, item_id, org_id),
        )
        if cur.rowcount == 0:
            return jsonify({"message": "Не найдено или уже не в статусе ожидания"}), 404
    return jsonify({"message": "Черновик обновлён"})


@app.post("/api/outreach/<int:item_id>/regenerate")
@limiter.limit("20 per minute", methods=["POST"])
def api_outreach_regenerate(item_id: int):
    ok, err = require_org_role(_ORG_STAFF_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    ts = _now_iso()
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM outreach_queue WHERE id = ? AND org_id = ?",
            (item_id, org_id),
        ).fetchone()
        if not row:
            return jsonify({"message": "Запись не найдена"}), 404
        if str(row["status"] or "") != "pending":
            return jsonify({"message": "Регенерация доступна только для статуса «ожидает»"}), 409
    try:
        text = _outreach_regenerate_draft_text(org_id, row)
    except ValueError as exc:
        return jsonify({"message": str(exc)}), 400
    if len(text.strip()) < 2:
        return jsonify({"message": "Модель вернула пустой текст"}), 422
    with _db() as conn:
        conn.execute(
            "UPDATE outreach_queue SET draft_text = ?, updated_at = ? WHERE id = ? AND org_id = ? AND status = 'pending'",
            (text.strip(), ts, item_id, org_id),
        )
    return jsonify({"message": "Черновик перегенерирован", "draft_text": text.strip()})


@app.post("/api/outreach/<int:item_id>/approve")
@limiter.limit("60 per minute", methods=["POST"])
def api_outreach_approve(item_id: int):
    ok, err = require_org_role(_ORG_STAFF_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(force=True, silent=True) or {}
    new_draft = str(body.get("draft_text", "")).strip()
    ts = _now_iso()
    with _db() as conn:
        row = conn.execute(
            "SELECT id, status, conversation_id, stage FROM outreach_queue WHERE id = ? AND org_id = ?",
            (item_id, org_id),
        ).fetchone()
        if not row:
            return jsonify({"message": "Запись не найдена"}), 404
        prev = str(row["status"] or "")
        if prev not in ("pending", "approved"):
            return jsonify({"message": f"Нельзя одобрить в статусе «{prev}»"}), 409
        if new_draft:
            conn.execute(
                "UPDATE outreach_queue SET status = 'approved', draft_text = ?, approved_at = ?, updated_at = ? "
                "WHERE id = ? AND org_id = ?",
                (new_draft, ts, ts, item_id, org_id),
            )
        else:
            conn.execute(
                "UPDATE outreach_queue SET status = 'approved', approved_at = ?, updated_at = ? WHERE id = ? AND org_id = ?",
                (ts, ts, item_id, org_id),
            )
        # CRM: если есть связанная conversation — отметим, что оператор одобрил черновик.
        cid = row["conversation_id"]
        if cid:
            try:
                stage_val = int(row["stage"] or 1)
            except (TypeError, ValueError):
                stage_val = 1
            existing = conn.execute(
                "SELECT history_json FROM conversations WHERE id = ? AND org_id = ?",
                (int(cid), int(org_id)),
            ).fetchone()
            if existing:
                try:
                    hist = json.loads(str(existing["history_json"] or "[]"))
                except Exception:  # noqa: BLE001
                    hist = []
                if not isinstance(hist, list):
                    hist = []
                hist.append(
                    {
                        "role": "system",
                        "source": "approved_in_ui",
                        "stage": stage_val,
                        "text": (
                            f"Оператор одобрил черновик stage{stage_val}. "
                            "Бот подхватит сообщение из очереди и отправит."
                        ),
                        "at": ts,
                    }
                )
                conn.execute(
                    "UPDATE conversations SET history_json = ?, updated_at = ?, last_activity_at = ?, status = 'active' "
                    "WHERE id = ? AND org_id = ?",
                    (json.dumps(hist, ensure_ascii=False), ts, ts, int(cid), int(org_id)),
                )
    return jsonify({"message": "Одобрено. Запущенный бот подхватит сообщение в очередь отправки."})


@app.post("/api/outreach/<int:item_id>/reject")
@limiter.limit("60 per minute", methods=["POST"])
def api_outreach_reject(item_id: int):
    ok, err = require_org_role(_ORG_STAFF_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    ts = _now_iso()
    with _db() as conn:
        row = conn.execute(
            "SELECT id, conversation_id, stage FROM outreach_queue WHERE id = ? AND org_id = ? AND status = 'pending'",
            (item_id, org_id),
        ).fetchone()
        if not row:
            return jsonify({"message": "Не найдено или уже обработано"}), 404
        conn.execute(
            "UPDATE outreach_queue SET status = 'rejected', updated_at = ? WHERE id = ? AND org_id = ?",
            (ts, item_id, org_id),
        )
        cid = row["conversation_id"]
        if cid:
            try:
                stage_val = int(row["stage"] or 1)
            except (TypeError, ValueError):
                stage_val = 1
            existing = conn.execute(
                "SELECT history_json FROM conversations WHERE id = ? AND org_id = ?",
                (int(cid), int(org_id)),
            ).fetchone()
            if existing:
                try:
                    hist = json.loads(str(existing["history_json"] or "[]"))
                except Exception:  # noqa: BLE001
                    hist = []
                if not isinstance(hist, list):
                    hist = []
                hist.append(
                    {
                        "role": "system",
                        "source": "rejected_in_ui",
                        "stage": stage_val,
                        "text": f"Оператор отклонил черновик stage{stage_val}.",
                        "at": ts,
                    }
                )
                conn.execute(
                    "UPDATE conversations SET history_json = ?, updated_at = ?, last_activity_at = ?, status = 'ignored' "
                    "WHERE id = ? AND org_id = ?",
                    (json.dumps(hist, ensure_ascii=False), ts, ts, int(cid), int(org_id)),
                )
    return jsonify({"message": "Отклонено"})


def _conversation_llm_next_message(
    org_id: int,
    *,
    stage: int,
    history: list[dict[str, Any]],
    lead_snippet: str,
    source_chat: str,
    lead_label: str,
) -> str:
    paths = _tenant_paths(org_id)
    if not paths["config"].exists():
        raise ValueError("Нет настроек организации")
    cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    llm = cfg.get("llm", {})
    if not isinstance(llm, dict) or not llm.get("enabled"):
        raise ValueError("LLM выключен в настройках")
    api_key = str(llm.get("api_key", "")).strip()
    if not api_key:
        raise ValueError("Не задан llm.api_key")
    base_url = str(llm.get("base_url", "") or "https://api.openai.com/v1").strip()
    model = str(llm.get("model", "") or "gpt-4o-mini").strip()
    partner = str(cfg.get("partner_name", "") or "партнёр").strip()
    tpl = cfg.get("templates") if isinstance(cfg.get("templates"), dict) else {}
    stage = max(1, min(3, int(stage)))
    hist_lines: list[str] = []
    for entry in history[-24:]:
        if not isinstance(entry, dict):
            continue
        role = str(entry.get("role") or "note")
        txt = str(entry.get("text") or "").strip()
        if not txt:
            continue
        hist_lines.append(f"{role}: {txt[:500]}")
    hist_block = "\n".join(hist_lines) if hist_lines else "(история пуста)"

    pr = effective_llm_prompts(cfg)

    if stage == 1:
        stage1_hint = str((tpl or {}).get("stage1", "") or "")[:800]
        system = pr["conversation_stage1_system"]
        user_msg = format_llm_prompt(
            pr["conversation_stage1_user"],
            partner=partner,
            lead_label=lead_label,
            source_chat=source_chat,
            lead_snippet=lead_snippet or "—",
            hist_block=hist_block,
            stage1_hint=stage1_hint or "—",
        )
    elif stage == 2:
        s2 = str((tpl or {}).get("stage2", "") or "")[:600]
        system = pr["conversation_stage2_system"]
        user_msg = format_llm_prompt(
            pr["conversation_stage2_user"],
            partner=partner,
            lead_label=lead_label,
            source_chat=source_chat,
            hist_block=hist_block,
            s2=s2 or "—",
        )
    else:
        s3 = str((tpl or {}).get("stage3", "") or "")[:600]
        system = pr["conversation_stage3_system"]
        user_msg = format_llm_prompt(
            pr["conversation_stage3_user"],
            partner=partner,
            lead_label=lead_label,
            hist_block=hist_block,
            s3=s3 or "—",
        )
    return _openai_chat_completion(base_url, api_key, model, system, user_msg, temperature=0.65)


@app.get("/api/conversations")
def api_conversations_list():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    with _db() as conn:
        rows = conn.execute(
            """
            SELECT * FROM conversations
            WHERE org_id = ?
            ORDER BY COALESCE(updated_at, created_at) DESC, id DESC
            LIMIT 200
            """,
            (org_id,),
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["history"] = _conversation_history_load(str(d.get("history_json") or "[]"))
        del d["history_json"]
        out.append(d)
    return jsonify({"org_id": org_id, "items": out})


@app.get("/api/conversations/<int:conv_id>")
def api_conversations_get(conv_id: int):
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM conversations WHERE id = ? AND org_id = ?",
            (conv_id, org_id),
        ).fetchone()
    if not row:
        return jsonify({"message": "Не найдено"}), 404
    d = dict(row)
    d["history"] = _conversation_history_load(str(d.get("history_json") or "[]"))
    del d["history_json"]
    return jsonify(d)


@app.post("/api/conversations")
@limiter.limit("40 per minute", methods=["POST"])
def api_conversations_create():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(force=True, silent=True) or {}
    lead_user_id = str(body.get("lead_user_id", "") or "").strip()
    if not lead_user_id:
        return jsonify({"message": "Нужен lead_user_id"}), 400
    lead_username = str(body.get("lead_username", "") or "").strip() or None
    source_chat = str(body.get("source_chat", "") or "").strip() or None
    lead_snippet = str(body.get("lead_snippet", "") or "").strip() or None
    trigger_match = str(body.get("trigger_match", "") or "").strip() or None
    status = str(body.get("status", "") or "waiting_approval").strip()
    if status not in ALLOWED_CONVERSATION_STATUS:
        status = "waiting_approval"
    try:
        stage = int(body.get("current_stage", 1) or 1)
    except (TypeError, ValueError):
        stage = 1
    stage = max(1, min(3, stage))
    ts = _now_iso()
    with _db() as conn:
        cur = conn.execute(
            """
            INSERT INTO conversations (
                org_id, lead_user_id, lead_username, source_chat, status,
                history_json, current_stage, lead_snippet, trigger_match, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, '[]', ?, ?, ?, ?, ?)
            """,
            (
                org_id,
                lead_user_id,
                lead_username,
                source_chat,
                status,
                stage,
                lead_snippet,
                trigger_match,
                ts,
                ts,
            ),
        )
        new_id = int(cur.lastrowid)
    return jsonify({"message": "Создано", "id": new_id})


@app.post("/api/conversations/<int:conv_id>/generate-next")
@limiter.limit("20 per minute", methods=["POST"])
def api_conversations_generate_next(conv_id: int):
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(force=True, silent=True) or {}
    advance = bool(body.get("advance_stage"))
    ts = _now_iso()
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM conversations WHERE id = ? AND org_id = ?",
            (conv_id, org_id),
        ).fetchone()
        if not row:
            return jsonify({"message": "Не найдено"}), 404
        hist = _conversation_history_load(str(row["history_json"] or "[]"))
        stage = int(row["current_stage"] or 1)
        stage = max(1, min(3, stage))
        lead_snippet = str(row["lead_snippet"] or "")
        source_chat = str(row["source_chat"] or "—")
        un = str(row["lead_username"] or "").strip()
        uid = str(row["lead_user_id"] or "").strip()
        lead_label = f"@{un.lstrip('@')}" if un else f"user_id {uid}"
        try:
            text = _conversation_llm_next_message(
                org_id,
                stage=stage,
                history=hist,
                lead_snippet=lead_snippet,
                source_chat=source_chat,
                lead_label=lead_label,
            )
        except ValueError as exc:
            return jsonify({"message": str(exc)}), 400
        text = text.strip()
        if len(text) < 2:
            return jsonify({"message": "Пустой ответ модели"}), 422
        hist.append(
            {
                "role": "assistant",
                "source": "llm_suggestion",
                "text": text,
                "at": ts,
                "stage": stage,
            }
        )
        new_stage = min(3, stage + 1) if advance else stage
        conn.execute(
            """
            UPDATE conversations
            SET history_json = ?, current_stage = ?, updated_at = ?
            WHERE id = ? AND org_id = ?
            """,
            (_conversation_history_save(hist), new_stage, ts, conv_id, org_id),
        )
    return jsonify({"message": "Сгенерировано", "suggestion": text, "current_stage": new_stage})


@app.post("/api/conversations/<int:conv_id>/edit")
@limiter.limit("60 per minute", methods=["POST"])
def api_conversations_edit(conv_id: int):
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(force=True, silent=True) or {}
    text = str(body.get("text", "") or "").strip()
    if len(text) < 1:
        return jsonify({"message": "Пустой текст"}), 400
    role = str(body.get("role", "staff") or "staff").strip()
    if role not in ("staff", "user", "note"):
        role = "staff"
    ts = _now_iso()
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM conversations WHERE id = ? AND org_id = ?",
            (conv_id, org_id),
        ).fetchone()
        if not row:
            return jsonify({"message": "Не найдено"}), 404
        hist = _conversation_history_load(str(row["history_json"] or "[]"))
        hist.append({"role": role, "text": text, "at": ts})
        conn.execute(
            "UPDATE conversations SET history_json = ?, updated_at = ? WHERE id = ? AND org_id = ?",
            (_conversation_history_save(hist), ts, conv_id, org_id),
        )
    return jsonify({"message": "Добавлено в историю"})


@app.post("/api/conversations/<int:conv_id>/skip")
@limiter.limit("40 per minute", methods=["POST"])
def api_conversations_skip(conv_id: int):
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    ts = _now_iso()
    with _db() as conn:
        cur = conn.execute(
            "UPDATE conversations SET status = 'ignored', updated_at = ? WHERE id = ? AND org_id = ?",
            (ts, conv_id, org_id),
        )
        if cur.rowcount == 0:
            return jsonify({"message": "Не найдено"}), 404
    return jsonify({"message": "Помечено как пропущено (ignored)"})


@app.post("/api/conversations/<int:conv_id>/dead")
@limiter.limit("40 per minute", methods=["POST"])
def api_conversations_dead(conv_id: int):
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    ts = _now_iso()
    with _db() as conn:
        cur = conn.execute(
            "UPDATE conversations SET status = 'dead', updated_at = ? WHERE id = ? AND org_id = ?",
            (ts, conv_id, org_id),
        )
        if cur.rowcount == 0:
            return jsonify({"message": "Не найдено"}), 404
    return jsonify({"message": "Помечено как dead"})


@app.get("/api/calls")
def api_calls_list():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM calls WHERE org_id = ? ORDER BY COALESCE(scheduled_at, created_at) DESC, id DESC LIMIT 200",
            (org_id,),
        ).fetchall()
    return jsonify({"org_id": org_id, "items": [dict(r) for r in rows]})


@app.post("/api/calls")
@limiter.limit("30 per minute", methods=["POST"])
def api_calls_create():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(force=True, silent=True) or {}
    lead_username = str(body.get("lead_username", "") or "").strip()
    lead_user_id = str(body.get("lead_user_id", "") or "").strip()
    scheduled_at = str(body.get("scheduled_at", "") or "").strip()
    notes = str(body.get("notes", "") or "").strip()
    outcome = str(body.get("outcome", "planned") or "planned").strip() or "planned"
    conversation_id: int | None = None
    if body.get("conversation_id") is not None and str(body.get("conversation_id")).strip() != "":
        try:
            conversation_id = int(body.get("conversation_id"))
        except (TypeError, ValueError):
            conversation_id = None
    try:
        duration_min = int(body.get("duration_min", 30) or 30)
    except (TypeError, ValueError):
        duration_min = 30
    duration_min = max(5, min(240, duration_min))
    ts = _now_iso()
    with _db() as conn:
        if conversation_id is not None:
            cex = conn.execute(
                "SELECT id FROM conversations WHERE id = ? AND org_id = ?",
                (conversation_id, org_id),
            ).fetchone()
            if not cex:
                return jsonify({"message": "conversation_id не найден в организации"}), 400
        cur = conn.execute(
            """
            INSERT INTO calls (
                org_id, lead_username, lead_user_id, scheduled_at, duration_min,
                outcome, notes, created_at, conversation_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                org_id,
                lead_username,
                lead_user_id,
                scheduled_at or None,
                duration_min,
                outcome,
                notes,
                ts,
                conversation_id,
            ),
        )
        new_id = int(cur.lastrowid)
        if conversation_id is not None:
            _conv_propagate_call_outcome(conn, conversation_id=conversation_id, org_id=org_id, outcome=outcome)
    return jsonify({"message": "Созвон сохранён", "id": new_id})


@app.patch("/api/calls/<int:call_id>")
@limiter.limit("30 per minute", methods=["PATCH"])
def api_calls_patch(call_id: int):
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(force=True, silent=True) or {}
    fields: list[str] = []
    vals: list[Any] = []
    for key, col in (
        ("lead_username", "lead_username"),
        ("lead_user_id", "lead_user_id"),
        ("scheduled_at", "scheduled_at"),
        ("notes", "notes"),
        ("outcome", "outcome"),
        ("conversation_id", "conversation_id"),
    ):
        if key in body:
            if col == "conversation_id":
                raw_c = body.get(key)
                if raw_c is None or str(raw_c).strip() == "":
                    fields.append("conversation_id = ?")
                    vals.append(None)
                else:
                    try:
                        cid = int(raw_c)
                    except (TypeError, ValueError):
                        return jsonify({"message": "conversation_id должен быть числом"}), 400
                    with _db() as cx:
                        cex = cx.execute(
                            "SELECT id FROM conversations WHERE id = ? AND org_id = ?",
                            (cid, org_id),
                        ).fetchone()
                    if not cex:
                        return jsonify({"message": "conversation_id не найден"}), 400
                    fields.append("conversation_id = ?")
                    vals.append(cid)
            else:
                fields.append(f"{col} = ?")
                vals.append(str(body.get(key) or "").strip() or None)
    if "duration_min" in body:
        try:
            dm = int(body.get("duration_min") or 30)
        except (TypeError, ValueError):
            dm = 30
        fields.append("duration_min = ?")
        vals.append(max(5, min(240, dm)))
    if not fields:
        return jsonify({"message": "Нет полей для обновления"}), 400
    fields.append("updated_at = ?")
    vals.append(_now_iso())
    vals.extend([call_id, org_id])
    sql = "UPDATE calls SET " + ", ".join(fields) + " WHERE id = ? AND org_id = ?"
    with _db() as conn:
        cur = conn.execute(sql, tuple(vals))
        if cur.rowcount == 0:
            return jsonify({"message": "Не найдено"}), 404
        # Авто-перевод conversation.status, если меняется outcome созвона.
        if "outcome" in body:
            row = conn.execute(
                "SELECT conversation_id, outcome FROM calls WHERE id = ? AND org_id = ?",
                (call_id, org_id),
            ).fetchone()
            if row and row["conversation_id"]:
                _conv_propagate_call_outcome(
                    conn,
                    conversation_id=int(row["conversation_id"]),
                    org_id=int(org_id),
                    outcome=str(row["outcome"] or ""),
                )
    return jsonify({"message": "Обновлено"})


@app.delete("/api/calls/<int:call_id>")
def api_calls_delete(call_id: int):
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    with _db() as conn:
        cur = conn.execute("DELETE FROM calls WHERE id = ? AND org_id = ?", (call_id, org_id))
        if cur.rowcount == 0:
            return jsonify({"message": "Не найдено"}), 404
    return jsonify({"message": "Удалено"})


# ── Лидоген по сайтам ────────────────────────────────────────────────────────


_WEB_LEAD_ALLOWED_STATUS = {"new", "queued", "ready", "empty", "error", "promoted", "ignored"}


@app.get("/api/web-leads/settings")
def api_web_leads_settings_get():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    return jsonify(
        {
            "org_id": org_id,
            "serpapi_key_configured": bool(_web_get_serpapi_key(org_id)),
            "worker_alive": bool(_WEB_JOBS_WORKER_STARTED),
        }
    )


@app.post("/api/web-leads/settings")
def api_web_leads_settings_set():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(silent=True) or {}
    key = str(body.get("serpapi_key") or "").strip()
    if key and (len(key) < 16 or len(key) > 200):
        return jsonify({"message": "Ключ SerpAPI имеет неожиданную длину"}), 400
    try:
        _web_set_serpapi_key(org_id, key)
    except OSError as exc:
        return jsonify({"message": f"Не удалось сохранить настройки: {exc}"}), 500
    return jsonify({"message": "Сохранено", "serpapi_key_configured": bool(key)})


@app.get("/api/web-leads")
def api_web_leads_list():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    status_filter = (request.args.get("status") or "").strip().lower()
    limit = max(1, min(500, int(request.args.get("limit") or 200)))
    sql = "SELECT * FROM web_leads WHERE org_id = ?"
    args: list[Any] = [org_id]
    if status_filter and status_filter in _WEB_LEAD_ALLOWED_STATUS:
        sql += " AND status = ?"
        args.append(status_filter)
    sql += " ORDER BY updated_at DESC, id DESC LIMIT ?"
    args.append(limit)
    with _db() as conn:
        rows = conn.execute(sql, args).fetchall()
        pending_jobs = conn.execute(
            "SELECT COUNT(1) AS n FROM web_lead_jobs WHERE org_id=? AND status IN ('pending','running')",
            (org_id,),
        ).fetchone()
    return jsonify(
        {
            "org_id": org_id,
            "items": [_web_lead_row_to_dict(r) for r in rows],
            "pending_jobs": int(pending_jobs["n"] if pending_jobs else 0),
            "serpapi_key_configured": bool(_web_get_serpapi_key(org_id)),
        }
    )


@app.post("/api/web-leads/search")
@limiter.limit("30/hour")
def api_web_leads_search():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(silent=True) or {}
    query = str(body.get("query") or "").strip()
    if len(query) < 2:
        return jsonify({"message": "Запрос слишком короткий (минимум 2 символа)"}), 400
    if len(query) > 300:
        return jsonify({"message": "Запрос слишком длинный (макс. 300 символов)"}), 400
    count = max(1, min(50, int(body.get("count") or 20)))
    gl = str(body.get("gl") or "ru")[:5]
    hl = str(body.get("hl") or "ru")[:5]
    if not _web_get_serpapi_key(org_id):
        return jsonify({"message": "Не настроен ключ SerpAPI (раздел «Настройки → Подключения»)"}), 400
    with _db() as conn:
        job_id = _web_jobs_enqueue(
            conn,
            org_id=org_id,
            kind="serp_search",
            payload={"query": query, "count": count, "gl": gl, "hl": hl},
        )
    _ensure_web_jobs_worker()
    return jsonify({"message": "Поиск поставлен в очередь", "job_id": job_id})


@app.post("/api/web-leads/import")
@limiter.limit("60/hour")
def api_web_leads_import():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    body = request.get_json(silent=True) or {}
    raw = body.get("domains")
    if isinstance(raw, str):
        candidates = re.split(r"[\s,;]+", raw)
    elif isinstance(raw, list):
        candidates = [str(x) for x in raw]
    else:
        return jsonify({"message": "Поле domains: строка или массив"}), 400
    domains: list[str] = []
    for s in candidates:
        d = web_lead_finder.normalize_domain(s)
        if d and d not in domains:
            domains.append(d)
    if not domains:
        return jsonify({"message": "Нет валидных доменов"}), 400
    if len(domains) > 1000:
        return jsonify({"message": "Слишком много доменов (макс. 1000 за раз)"}), 400
    enqueued = 0
    with _db() as conn:
        for d in domains:
            _web_lead_upsert(conn, org_id=org_id, domain=d, source="import", query="")
            _web_jobs_enqueue(
                conn,
                org_id=org_id,
                kind="parse_domain",
                payload={"domain": d, "source": "import", "query": ""},
            )
            enqueued += 1
    _ensure_web_jobs_worker()
    return jsonify({"message": "Импорт поставлен в очередь", "queued": enqueued})


@app.post("/api/web-leads/<int:lead_id>/refresh")
@limiter.limit("60/hour")
def api_web_leads_refresh(lead_id: int):
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    with _db() as conn:
        row = conn.execute(
            "SELECT id, domain FROM web_leads WHERE id = ? AND org_id = ?",
            (int(lead_id), org_id),
        ).fetchone()
        if not row:
            return jsonify({"message": "Не найдено"}), 404
        _web_jobs_enqueue(
            conn,
            org_id=org_id,
            kind="parse_domain",
            payload={"domain": str(row["domain"]), "source": "manual", "query": ""},
        )
    _ensure_web_jobs_worker()
    return jsonify({"message": "Обновление запущено"})


@app.post("/api/web-leads/<int:lead_id>/promote")
def api_web_leads_promote(lead_id: int):
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM web_leads WHERE id = ? AND org_id = ?",
            (int(lead_id), org_id),
        ).fetchone()
        if not row:
            return jsonify({"message": "Не найдено"}), 404
        wl = _web_lead_row_to_dict(row)
        if not wl["emails"] and not wl["phones"] and not wl["telegrams"] and not wl["whatsapps"]:
            return jsonify({"message": "Нет контактов: сначала «Обновить» и дождитесь парсинга"}), 400
        if wl["conversation_id"]:
            return jsonify({"message": "Уже добавлено в CRM", "conversation_id": wl["conversation_id"]})
        # Лучший представитель контакта для отображения как "username".
        primary = ""
        if wl["telegrams"]:
            primary = "@" + str(wl["telegrams"][0])
        elif wl["emails"]:
            primary = str(wl["emails"][0])
        elif wl["whatsapps"]:
            primary = "wa:" + str(wl["whatsapps"][0])
        elif wl["phones"]:
            primary = str(wl["phones"][0])
        elif wl["vks"]:
            primary = "vk:" + str(wl["vks"][0])
        history = [
            {
                "ts": _now_iso(),
                "type": "detection",
                "source": "website",
                "domain": wl["domain"],
                "query": wl["query"],
                "contacts": {
                    "emails": wl["emails"],
                    "phones": wl["phones"],
                    "telegrams": wl["telegrams"],
                    "whatsapps": wl["whatsapps"],
                    "vks": wl["vks"],
                },
            }
        ]
        now = _now_iso()
        cur = conn.execute(
            """
            INSERT INTO conversations(
                org_id, lead_user_id, lead_username, source_chat, status, history_json,
                current_stage, lead_snippet, trigger_match, created_at, updated_at, last_activity_at
            ) VALUES (?, ?, ?, ?, 'active', ?, 1, ?, ?, ?, ?, ?)
            """,
            (
                int(org_id),
                f"web:{wl['domain']}",
                primary or wl["domain"],
                f"website:{wl['domain']}",
                json.dumps(history, ensure_ascii=False),
                (wl["title"] or "")[:1000],
                "website",
                now,
                now,
                now,
            ),
        )
        conv_id = int(cur.lastrowid or 0)
        conn.execute(
            "UPDATE web_leads SET status='promoted', conversation_id=?, updated_at=? WHERE id=?",
            (conv_id, now, int(lead_id)),
        )
    return jsonify({"message": "Добавлено в CRM", "conversation_id": conv_id})


@app.delete("/api/web-leads/<int:lead_id>")
def api_web_leads_delete(lead_id: int):
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    with _db() as conn:
        cur = conn.execute(
            "DELETE FROM web_leads WHERE id = ? AND org_id = ?",
            (int(lead_id), org_id),
        )
        if cur.rowcount == 0:
            return jsonify({"message": "Не найдено"}), 404
    return jsonify({"message": "Удалено"})


@app.get("/api/billing/payments")
def api_billing_payments():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM payments WHERE org_id = ? ORDER BY id DESC LIMIT 100",
            (org_id,),
        ).fetchall()
    return jsonify({"org_id": org_id, "items": [dict(r) for r in rows]})


@app.post("/api/billing/checkout")
@limiter.limit("10 per minute", methods=["POST"])
def api_billing_checkout():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    user = _session_user()
    assert user is not None
    uid = int(user["id"])
    if _yookassa_credentials() is None:
        return jsonify(
            {
                "message": "ЮKassa не настроена на сервере (YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY).",
                "checkout_url": None,
                "payment_id": None,
            }
        ), 503
    body = request.get_json(force=True, silent=True) or {}
    plan_id = str(body.get("plan_id") or "").strip()
    if not plan_id:
        return jsonify({"message": "Укажите plan_id"}), 400
    with _db() as conn:
        plan = conn.execute("SELECT * FROM plans WHERE id = ?", (plan_id,)).fetchone()
    if not plan:
        return jsonify({"message": "Неизвестный тариф"}), 404
    price = int(plan["price_rub_month"] or 0)
    if price <= 0:
        return jsonify({"message": "Этот тариф не требует оплаты"}), 400
    base = _public_app_base_url()
    if not base:
        return jsonify({"message": "Задайте PUBLIC_APP_URL для return_url оплаты"}), 500
    return_url = f"{base}/account/billing?paid=1"
    ts = _now_iso()
    amount_str = f"{price}.00"
    idem = secrets.token_hex(24)
    with _db() as conn:
        cur = conn.execute(
            """
            INSERT INTO payments (
                org_id, user_id, plan_id, amount_rub_gross, fee_rub, amount_rub_net,
                currency, provider, provider_payment_id, status, created_at
            )
            VALUES (?, ?, ?, ?, 0, ?, 'RUB', 'yookassa', '', 'pending', ?)
            """,
            (org_id, uid, plan_id, price, price, ts),
        )
        internal_id = int(cur.lastrowid)
    try:
        y_resp = _yookassa_api_json(
            "POST",
            "/payments",
            {
                "amount": {"value": amount_str, "currency": "RUB"},
                "confirmation": {"type": "redirect", "return_url": return_url},
                "capture": True,
                "description": f"Leadgen: тариф {plan_id}, org {org_id}",
                "metadata": {
                    "leadgen_payment_id": str(internal_id),
                    "org_id": str(org_id),
                    "plan_id": plan_id,
                },
            },
            idempotence_key=idem,
        )
    except ValueError as exc:
        with _db() as conn:
            conn.execute(
                "UPDATE payments SET status = 'failed', provider = 'yookassa' WHERE id = ? AND org_id = ?",
                (internal_id, org_id),
            )
        return jsonify({"message": str(exc), "checkout_url": None, "payment_id": internal_id}), 502
    yid = str(y_resp.get("id") or "").strip()
    conf = y_resp.get("confirmation") or {}
    pay_url = str(conf.get("confirmation_url") or "").strip()
    with _db() as conn:
        conn.execute(
            "UPDATE payments SET provider_payment_id = ? WHERE id = ? AND org_id = ?",
            (yid, internal_id, org_id),
        )
    return jsonify(
        {
            "message": "Перейдите по ссылке для оплаты. Для клиента сумма без доп. комиссии (комиссия провайдера — с мерчанта).",
            "checkout_url": pay_url or None,
            "payment_id": internal_id,
            "yookassa_id": yid or None,
        }
    )


@app.post("/api/billing/webhook/yookassa")
@limiter.limit("60 per minute", methods=["POST"])
def api_billing_webhook_yookassa():
    """Уведомления YooKassa: без CSRF и без сессии. Платёж перепроверяется через API.

    Защита: rate-limit + ограничение размера тела + Content-Type guard +
    логирование событий (включая отбракованные)."""
    ct = (request.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
    if ct not in ("application/json", "text/json", ""):
        _audit_login(None, "billing_webhook_bad_content_type", email=ct or "")
        return jsonify({"message": "unsupported content-type"}), 415
    cl = request.headers.get("Content-Length")
    if cl and cl.isdigit() and int(cl) > 64 * 1024:
        _audit_login(None, "billing_webhook_body_too_large", email=cl)
        return jsonify({"message": "payload too large"}), 413
    raw_body = request.get_data(as_text=True, cache=False) or ""
    if len(raw_body) > 64 * 1024:
        _audit_login(None, "billing_webhook_body_too_large", email=str(len(raw_body)))
        return jsonify({"message": "payload too large"}), 413
    try:
        note = json.loads(raw_body)
    except json.JSONDecodeError:
        _audit_login(None, "billing_webhook_bad_json")
        return jsonify({"message": "invalid json"}), 400
    if not isinstance(note, dict):
        return jsonify({"message": "invalid payload"}), 400
    evt = str(note.get("event") or "")
    if evt != "payment.succeeded":
        _audit_login(None, "billing_webhook_event_skipped", email=evt[:64])
        return jsonify({"ok": True})
    obj = note.get("object") or {}
    yid = str(obj.get("id") or "").strip()
    if not yid:
        _audit_login(None, "billing_webhook_no_id")
        return jsonify({"message": "no payment id"}), 400
    try:
        pay = _yookassa_api_json("GET", f"/payments/{yid}")
    except ValueError as exc:
        _audit_login(None, "billing_webhook_api_error", email=str(exc)[:200])
        return jsonify({"message": str(exc)}), 502
    try:
        _apply_yookassa_payment_paid(yid, pay)
        _audit_login(None, "billing_webhook_applied", email=yid)
    except Exception as exc:
        _audit_login(None, "billing_webhook_apply_failed", email=f"{yid}:{type(exc).__name__}")
        return jsonify({"message": str(exc)}), 500
    return jsonify({"ok": True})


@app.get("/api/bot/progress")
def bot_progress():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, _role = ctx
    paths = _tenant_paths(org_id)
    info = _bot_run_status(int(org_id))
    proc = bot_processes.get(org_id)
    running = bool((proc and proc.poll() is None) or info.get("alive"))
    default: dict[str, Any] = {
        "pass_index": 0,
        "pass_total": 0,
        "current_chat": None,
        "updated_at": None,
        "phase": "idle",
        "idle_reason": None,
        "last_action": None,
        "running": running,
        "stale": bool(info.get("stale")),
        "last_heartbeat": info.get("last_heartbeat"),
    }
    if not paths["state"].exists():
        return jsonify(default)
    try:
        st = json.loads(paths["state"].read_text(encoding="utf-8"))
    except Exception:
        return jsonify(default)
    sp = st.get("scan_progress") if isinstance(st, dict) else None
    if not isinstance(sp, dict):
        return jsonify(dict(default))
    idx = int(sp.get("pass_index", 0) or 0)
    tot = int(sp.get("pass_total", 0) or 0)
    cur_chat = sp.get("current_chat")
    raw_phase = sp.get("phase")
    if isinstance(raw_phase, str) and raw_phase.strip():
        phase = raw_phase.strip()
    elif tot > 0 and (cur_chat or (0 < idx < tot)):
        phase = "scanning"
    elif tot > 0 and idx >= tot and not cur_chat:
        phase = "idle"
    else:
        phase = "idle"
    if phase == "scanning" and tot > 0 and idx >= tot and not cur_chat:
        phase = "idle"
    # Если бот ещё не дошёл до сканирования, фаза в state.json может быть 'idle',
    # хотя в bot_runs уже стоит 'connecting' / 'awaiting_code' / 'awaiting_password' / 'ready'. Берём свежий приоритет.
    db_phase = (info.get("phase") or "").strip()
    if db_phase in ("connecting", "awaiting_code", "awaiting_password") and phase in ("idle", "scanning"):
        phase = db_phase
    return jsonify(
        {
            "pass_index": idx,
            "pass_total": tot,
            "current_chat": cur_chat,
            "updated_at": sp.get("updated_at"),
            "phase": phase,
            "idle_reason": sp.get("idle_reason"),
            "last_action": sp.get("last_action"),
            "running": running,
            "stale": bool(info.get("stale")),
            "last_heartbeat": info.get("last_heartbeat"),
        }
    )


@app.get("/api/bot/info")
def bot_info():
    ok, err = require_auth()
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, org_role = ctx

    plan = _get_plan_for_org(org_id)
    plan_id = str(plan["id"]) if plan else "free"
    sub_status = str(plan["sub_status"]) if plan and "sub_status" in plan.keys() else "unknown"

    paths = _tenant_paths(org_id)
    cfg = {}
    if paths["config"].exists():
        try:
            cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
        except Exception:
            cfg = {}
    if isinstance(cfg, dict):
        _in_memory_telegram_accounts_migrate(cfg, org_id)
    tg_brief: list[dict[str, Any]] = []
    active_tg = str(cfg.get("active_telegram_account") or "") if isinstance(cfg, dict) else ""
    if isinstance(cfg, dict):
        for a in cfg.get("telegram_accounts") or []:
            if isinstance(a, dict) and str(a.get("id", "")).strip():
                tg_brief.append(
                    {
                        "id": str(a.get("id", "")).strip(),
                        "label": str(a.get("label", "") or "").strip() or str(a.get("id", "")).strip(),
                    }
                )

    info = _bot_run_status(int(org_id))
    proc = bot_processes.get(org_id)
    if proc and proc.poll() is None:
        pid = proc.pid
        running = True
    elif info.get("alive"):
        pid = int(info.get("pid") or 0) or None
        running = True
    else:
        pid = None
        running = False
    dry_run = bool(cfg.get("dry_run", False))
    limits = cfg.get("limits", {}) if isinstance(cfg.get("limits", {}), dict) else {}
    chats = cfg.get("target_chats", [])
    sched_raw = limits.get("schedule")
    sched = sched_raw if isinstance(sched_raw, dict) else {}
    sched_en = bool(sched.get("enabled"))
    sched_tz = str(sched.get("timezone") or "UTC")
    sched_hours = sched.get("active_hours")
    if isinstance(sched_hours, (list, tuple)) and len(sched_hours) >= 2:
        schedule_summary = (
            f"вкл, {sched_tz}, {sched_hours[0]}–{sched_hours[1]} ч локально"
            if sched_en
            else f"выкл (в конфиге задано {sched_tz} {sched_hours[0]}–{sched_hours[1]})"
        )
    else:
        schedule_summary = "вкл" if sched_en else "выкл"
    stage_followup_hours = limits.get("stage_followup_hours")
    if not isinstance(stage_followup_hours, dict):
        stage_followup_hours = None

    try:
        max_monitor_passes = max(0, int(limits.get("max_monitor_passes", 0)))
    except (TypeError, ValueError):
        max_monitor_passes = 0

    can_start = org_role in _ORG_FUNNEL_ROLES and sub_status in ("active", "trial")
    reasons: list[str] = []
    if org_role not in _ORG_FUNNEL_ROLES:
        reasons.append("Недостаточно прав (нужна роль admin, manager, client или tester)")
    if sub_status not in ("active", "trial"):
        reasons.append(f"Подписка не активна ({sub_status})")

    return jsonify(
        {
            "org_id": org_id,
            "org_role": org_role,
            "running": running,
            "pid": pid,
            "plan_id": plan_id,
            "subscription_status": sub_status,
            "dry_run": dry_run,
            "schedule_summary": schedule_summary,
            "stage_followup_hours": stage_followup_hours,
            "target_chats": len(chats) if isinstance(chats, list) else 0,
            "monitor_interval_sec": limits.get("monitor_interval_sec", None),
            "max_monitor_passes": max_monitor_passes,
            "max_dm_per_hour_per_chat": limits.get("max_dm_per_hour_per_chat", None),
            "daily_limit_range": limits.get("daily_limit_range", None),
            "max_dm_month": limits.get("max_dm_month", None),
            "can_start": can_start,
            "reasons": reasons,
            "telegram_accounts": tg_brief,
            "active_telegram_account": active_tg,
            "stale": bool(info.get("stale")),
            "last_heartbeat": info.get("last_heartbeat"),
        }
    )


@app.post("/api/bot/stop")
def stop_bot():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    ctx = _current_user_org_role()
    if ctx is None:
        return jsonify({"message": "Unauthorized"}), 401
    org_id, role = ctx
    proc = bot_processes.get(org_id)
    info = _bot_run_status(int(org_id))
    if (not proc or proc.poll() is not None) and not info.get("alive"):
        return jsonify({"message": "Бот уже остановлен"})

    # Локальный proc — terminate, ждём, потом kill (если не выходит).
    if proc and proc.poll() is None:
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            proc.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except Exception:
                pass

    # Кросс-воркер: PID живёт у другого worker'а — пробуем сигнал по PID.
    if info.get("alive"):
        pid = int(info.get("pid") or 0)
        if pid > 0 and _pid_alive(pid):
            try:
                if os.name == "nt":
                    import ctypes  # noqa: PLC0415

                    PROCESS_TERMINATE = 0x0001
                    handle = ctypes.windll.kernel32.OpenProcess(PROCESS_TERMINATE, False, pid)  # type: ignore[attr-defined]
                    if handle:
                        ctypes.windll.kernel32.TerminateProcess(handle, 0)  # type: ignore[attr-defined]
                        ctypes.windll.kernel32.CloseHandle(handle)  # type: ignore[attr-defined]
                else:
                    os.kill(pid, signal.SIGTERM)
            except Exception as exc:  # noqa: BLE001
                print(f"WARNING: cross-worker terminate failed pid={pid}: {exc}", file=sys.stderr)
    _bot_run_clear(int(org_id))
    return jsonify({"message": f"Отправлен сигнал остановки (org={org_id})"})


@app.get("/api/folders/list")
def list_folders():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    paths = _tenant_paths(org_id)
    if not paths["config"].exists() and CONFIG_PATH.exists():
        _bootstrap_tenant_config_from_root(paths["config"])
    try:
        cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        cfg = {}
    acc = str(request.args.get("telegram_account_id") or "").strip() or None
    session_path = _session_path_for_telegram_account(org_id, cfg, acc)
    code, output = run_bot_command(
        [
            sys.executable,
            str(BOT_SCRIPT),
            "--list-folders",
            *_bot_argv_paths_and_session(paths, session_path),
        ]
    )
    return jsonify({"code": code, "output": output})


@app.post("/api/folders/import")
def import_folder():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    payload = request.get_json(force=True, silent=True) or {}
    folder = str(payload.get("folder", "")).strip()
    if not folder:
        return jsonify({"message": "Укажите название или id папки"}), 400
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    paths = _tenant_paths(org_id)
    if not paths["config"].exists() and CONFIG_PATH.exists():
        _bootstrap_tenant_config_from_root(paths["config"])
    try:
        cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        cfg = {}
    acc = str(payload.get("telegram_account_id") or "").strip() or None
    session_path = _session_path_for_telegram_account(org_id, cfg, acc)
    code, output = run_bot_command(
        [
            sys.executable,
            str(BOT_SCRIPT),
            "--import-folder",
            folder,
            "--apply-import",
            "--force-unlock",
            *_bot_argv_paths_and_session(paths, session_path),
        ]
    )
    return jsonify({"code": code, "output": output})


def _mask_config_telegram_for_tester(cfg: dict[str, Any]) -> None:
    """Не показываем hash и полностью телефон тестеру (GET config)."""
    if not isinstance(cfg, dict):
        return
    cfg["api_hash"] = ""
    accounts = cfg.get("telegram_accounts")
    if isinstance(accounts, list):
        for a in accounts:
            if not isinstance(a, dict):
                continue
            a["api_hash"] = ""
            ph = str(a.get("phone") or "").strip()
            if len(ph) > 6:
                a["phone"] = ph[:3] + "…" + ph[-2:]
            elif ph:
                a["phone"] = "•••"


def _merge_telegram_credentials_from_disk(org_id: int, payload: dict[str, Any]) -> None:
    """Тестер не может менять подключение Telegram — восстанавливаем из сохранённого config."""
    paths = _tenant_paths(org_id)
    if not paths["config"].is_file():
        return
    try:
        old_cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(old_cfg, dict):
        return
    if isinstance(old_cfg.get("telegram_accounts"), list):
        payload["telegram_accounts"] = json.loads(json.dumps(old_cfg["telegram_accounts"]))
    for key in ("api_id", "api_hash", "phone", "session_name"):
        if key in old_cfg:
            payload[key] = old_cfg[key]


# ─── Пресеты настроек ──────────────────────────────────────────────────────
#
# Пресет — снимок «безопасной» части config.json: лимиты, расписание,
# шаблоны, ключевые слова, channel_search_quality, llm_prompts и LLM-настройки
# без API-ключа. Чувствительные/per-org поля исключаются: telegram_accounts,
# api_id/api_hash/phone, llm.api_key, target_chats, partner_name (это
# индивидуально для пользователя).
#
# Применение делает deep-merge: преобразует только поля, заданные в пресете;
# остальное в config.json остаётся как есть. Это позволяет «подружить» пресет
# и индивидуальные настройки (Telegram-аккаунты, ключ LLM, целевые чаты).

_PRESET_INCLUDE_TOP_KEYS = (
    "dry_run",
    "human_approval",
    "human_approval_stages",
    "templates",
    "keywords",
    "limits",
    "schedule",
    "channel_search_quality",
    "llm",  # без api_key, см. _strip_sensitive_for_preset
    "llm_prompts",
)


def _strip_sensitive_for_preset(data: dict[str, Any]) -> dict[str, Any]:
    """Готовит данные пресета: убираем чувствительные/индивидуальные поля."""
    if not isinstance(data, dict):
        return {}
    out: dict[str, Any] = {}
    for k in _PRESET_INCLUDE_TOP_KEYS:
        if k in data:
            out[k] = json.loads(json.dumps(data[k]))
    llm = out.get("llm")
    if isinstance(llm, dict):
        llm.pop("api_key", None)
        # base_url и model можно оставить — это не секрет.
    # Никогда не сохраняем в пресет:
    for forbidden in (
        "api_id",
        "api_hash",
        "phone",
        "telegram_accounts",
        "active_telegram_account_id",
        "target_chats",
        "partner_name",
    ):
        out.pop(forbidden, None)
    return out


def _extract_preset_from_config(cfg: dict[str, Any]) -> dict[str, Any]:
    return _strip_sensitive_for_preset(cfg)


def _deep_merge_preset(cfg: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    """Глубокий merge `patch` поверх `cfg`. Списки заменяются целиком (атомарно),
    словари сливаются по ключам. Возвращает новый dict (cfg не мутируется)."""
    if not isinstance(cfg, dict):
        return json.loads(json.dumps(patch)) if isinstance(patch, dict) else {}
    if not isinstance(patch, dict):
        return json.loads(json.dumps(cfg))
    out = json.loads(json.dumps(cfg))
    for k, v in patch.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge_preset(out[k], v)
        else:
            out[k] = json.loads(json.dumps(v))
    return out


def _diff_preset_vs_config(
    cfg: dict[str, Any],
    patch: dict[str, Any],
    *,
    path: str = "",
) -> list[dict[str, Any]]:
    """Возвращает плоский список изменений: [{path, before, after, kind}]."""
    diffs: list[dict[str, Any]] = []
    if isinstance(patch, dict) and isinstance(cfg, dict):
        for k, v in patch.items():
            sub_path = f"{path}.{k}" if path else k
            if k not in cfg:
                diffs.append({"path": sub_path, "before": None, "after": v, "kind": "add"})
            elif isinstance(v, dict) and isinstance(cfg[k], dict):
                diffs.extend(_diff_preset_vs_config(cfg[k], v, path=sub_path))
            elif cfg[k] != v:
                diffs.append({"path": sub_path, "before": cfg[k], "after": v, "kind": "update"})
        return diffs
    if cfg != patch:
        diffs.append({"path": path or "<root>", "before": cfg, "after": patch, "kind": "update"})
    return diffs


def _preset_row_to_dict(row: sqlite3.Row, *, include_data: bool = False) -> dict[str, Any]:
    out = {
        "id": int(row["id"]),
        "kind": str(row["kind"]),
        "org_id": int(row["org_id"]) if row["org_id"] is not None else None,
        "name": str(row["name"]),
        "description": str(row["description"] or ""),
        "created_by": int(row["created_by"]) if row["created_by"] is not None else None,
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }
    if include_data:
        try:
            out["data"] = json.loads(row["data_json"] or "{}")
        except json.JSONDecodeError:
            out["data"] = {}
    return out


def _user_can_edit_preset(row: sqlite3.Row, *, user_role_global: str, org_role: str, org_id: int) -> bool:
    """Платформенный admin (users.role='admin') редактирует любой пресет; org admin —
    только пресеты своей org с kind='org'."""
    if user_role_global == "admin":
        return True
    if str(row["kind"]) == "org" and org_role == "admin" and int(row["org_id"] or 0) == int(org_id):
        return True
    return False


@app.get("/api/presets")
def api_presets_list():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    with _db() as conn:
        rows = conn.execute(
            """
            SELECT id, kind, org_id, name, description, created_by, created_at, updated_at
            FROM config_presets
            WHERE kind = 'system' OR (kind = 'org' AND org_id = ?)
            ORDER BY kind = 'system' DESC, name COLLATE NOCASE
            """,
            (int(org_id),),
        ).fetchall()
    items = [_preset_row_to_dict(r) for r in rows]
    return jsonify({"items": items})


@app.get("/api/presets/<int:preset_id>")
def api_presets_get(preset_id: int):
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM config_presets WHERE id = ?",
            (int(preset_id),),
        ).fetchone()
    if not row:
        return jsonify({"message": "Пресет не найден"}), 404
    if row["kind"] == "org" and int(row["org_id"] or 0) != int(org_id):
        return jsonify({"message": "Нет доступа к чужому org-пресету"}), 403
    return jsonify(_preset_row_to_dict(row, include_data=True))


@app.post("/api/presets")
@limiter.limit("20 per minute", methods=["POST"])
def api_presets_create():
    """Создать пресет. Платформенный admin может создавать kind='system',
    org admin — kind='org' для своей org."""
    ok, err = require_org_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _org_role = _user_org_role(int(user["id"]))
    body = request.get_json(force=True, silent=True) or {}
    name = str(body.get("name", "")).strip()
    description = str(body.get("description", "")).strip()
    if not (2 <= len(name) <= 80):
        return jsonify({"message": "Название от 2 до 80 символов"}), 400
    kind = str(body.get("kind", "org")).strip().lower()
    if kind not in ("system", "org"):
        return jsonify({"message": "kind должен быть 'system' или 'org'"}), 400
    user_role_global = str(user["role"] or "")
    if kind == "system" and user_role_global != "admin":
        return jsonify({"message": "Только платформенный admin создаёт system-пресеты"}), 403
    # Источник данных: либо переданный JSON `data`, либо снимок текущего config.json org'a.
    raw_data = body.get("data")
    if isinstance(raw_data, dict):
        data = _strip_sensitive_for_preset(raw_data)
    else:
        paths = _tenant_paths(org_id)
        if not paths["config"].exists():
            return jsonify({"message": "Нет config.json для снимка"}), 400
        try:
            cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
        except Exception:
            return jsonify({"message": "config.json повреждён"}), 500
        data = _extract_preset_from_config(cfg)
    if not data:
        return jsonify({"message": "Пустые данные пресета"}), 400
    target_org_id = None if kind == "system" else int(org_id)
    now = _now_iso()
    try:
        with _db() as conn:
            cur = conn.execute(
                """
                INSERT INTO config_presets(kind, org_id, name, description, data_json, created_by, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    kind,
                    target_org_id,
                    name,
                    description,
                    json.dumps(data, ensure_ascii=False),
                    int(user["id"]),
                    now,
                    now,
                ),
            )
            new_id = int(cur.lastrowid or 0)
    except sqlite3.IntegrityError:
        return jsonify({"message": "Пресет с таким названием уже существует"}), 409
    return jsonify({"message": "Пресет создан", "id": new_id})


@app.put("/api/presets/<int:preset_id>")
@limiter.limit("60 per minute", methods=["PUT"])
def api_presets_update(preset_id: int):
    ok, err = require_org_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, org_role = _user_org_role(int(user["id"]))
    user_role_global = str(user["role"] or "")
    body = request.get_json(force=True, silent=True) or {}
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM config_presets WHERE id = ?",
            (int(preset_id),),
        ).fetchone()
        if not row:
            return jsonify({"message": "Пресет не найден"}), 404
        if not _user_can_edit_preset(row, user_role_global=user_role_global, org_role=org_role, org_id=org_id):
            return jsonify({"message": "Нет прав на редактирование этого пресета"}), 403
        fields: list[str] = []
        params: list[Any] = []
        if "name" in body:
            new_name = str(body.get("name", "")).strip()
            if not (2 <= len(new_name) <= 80):
                return jsonify({"message": "Название от 2 до 80 символов"}), 400
            fields.append("name = ?")
            params.append(new_name)
        if "description" in body:
            fields.append("description = ?")
            params.append(str(body.get("description", "")).strip())
        if "data" in body:
            raw_data = body.get("data")
            if not isinstance(raw_data, dict):
                return jsonify({"message": "data должен быть объектом"}), 400
            fields.append("data_json = ?")
            params.append(json.dumps(_strip_sensitive_for_preset(raw_data), ensure_ascii=False))
        if not fields:
            return jsonify({"message": "Нет полей для обновления"}), 400
        fields.append("updated_at = ?")
        params.append(_now_iso())
        params.append(int(preset_id))
        try:
            conn.execute(
                f"UPDATE config_presets SET {', '.join(fields)} WHERE id = ?",
                params,
            )
        except sqlite3.IntegrityError:
            return jsonify({"message": "Пресет с таким названием уже существует"}), 409
    return jsonify({"message": "Сохранено"})


@app.delete("/api/presets/<int:preset_id>")
@limiter.limit("30 per minute", methods=["DELETE"])
def api_presets_delete(preset_id: int):
    ok, err = require_org_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, org_role = _user_org_role(int(user["id"]))
    user_role_global = str(user["role"] or "")
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM config_presets WHERE id = ?",
            (int(preset_id),),
        ).fetchone()
        if not row:
            return jsonify({"message": "Пресет не найден"}), 404
        if not _user_can_edit_preset(row, user_role_global=user_role_global, org_role=org_role, org_id=org_id):
            return jsonify({"message": "Нет прав на удаление"}), 403
        conn.execute("DELETE FROM config_presets WHERE id = ?", (int(preset_id),))
    return jsonify({"message": "Пресет удалён"})


@app.get("/api/presets/<int:preset_id>/diff")
def api_presets_diff(preset_id: int):
    """Возвращает плоский список различий между пресетом и текущим config.json org'а.
    Удобно для превью «что изменится при применении»."""
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM config_presets WHERE id = ?",
            (int(preset_id),),
        ).fetchone()
    if not row:
        return jsonify({"message": "Пресет не найден"}), 404
    if row["kind"] == "org" and int(row["org_id"] or 0) != int(org_id):
        return jsonify({"message": "Нет доступа"}), 403
    paths = _tenant_paths(org_id)
    cfg: dict[str, Any] = {}
    if paths["config"].exists():
        try:
            cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
        except Exception:
            cfg = {}
    try:
        data = json.loads(row["data_json"] or "{}")
    except json.JSONDecodeError:
        data = {}
    diffs = _diff_preset_vs_config(cfg, data)
    return jsonify(
        {
            "preset": _preset_row_to_dict(row),
            "diffs": diffs,
            "diffs_count": len(diffs),
        }
    )


@app.post("/api/presets/<int:preset_id>/apply")
@limiter.limit("20 per minute", methods=["POST"])
def api_presets_apply(preset_id: int):
    """Применить пресет: deep-merge в config.json org'а. Требует подтверждения:
    body должен содержать `"confirm": true` (защита от случайного клика).
    Никогда не трогает Telegram-аккаунты, llm.api_key, target_chats."""
    ok, err = require_org_role({"admin"})
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _org_role = _user_org_role(int(user["id"]))
    body = request.get_json(force=True, silent=True) or {}
    if not body.get("confirm"):
        return jsonify({"message": "Нужно подтверждение: confirm=true"}), 400
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM config_presets WHERE id = ?",
            (int(preset_id),),
        ).fetchone()
    if not row:
        return jsonify({"message": "Пресет не найден"}), 404
    if row["kind"] == "org" and int(row["org_id"] or 0) != int(org_id):
        return jsonify({"message": "Нет доступа"}), 403
    try:
        patch = json.loads(row["data_json"] or "{}")
    except json.JSONDecodeError:
        patch = {}
    patch = _strip_sensitive_for_preset(patch)
    paths = _tenant_paths(org_id)
    if not paths["config"].exists():
        if CONFIG_PATH.exists():
            _bootstrap_tenant_config_from_root(paths["config"])
    cfg: dict[str, Any] = {}
    if paths["config"].exists():
        try:
            cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
        except Exception:
            cfg = {}
    merged = _deep_merge_preset(cfg, patch)
    plan = _get_plan_for_org(org_id)
    if plan:
        merged = _enforce_plan_on_config(merged, plan)
    paths["config"].write_text(
        json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    try:
        _audit_login(
            int(user["id"]),
            "preset_applied",
            email=f"preset#{int(preset_id)}",
        )
    except Exception:  # noqa: BLE001
        pass
    return jsonify(
        {
            "message": f"Пресет «{row['name']}» применён",
            "preset_id": int(preset_id),
        }
    )


def _redact_config_for_client(cfg: dict[str, Any], org_role: str) -> dict[str, Any]:
    """Не отдаём llm.api_key не-админам; добавляем llm_key_configured для UI."""
    out = json.loads(json.dumps(cfg))
    llm = out.get("llm")
    key_set = False
    if isinstance(llm, dict):
        key_set = bool(str(llm.get("api_key", "")).strip())
        if org_role != "admin":
            llm = dict(llm)
            llm["api_key"] = ""
            out["llm"] = llm
    out["llm_key_configured"] = key_set
    if org_role != "admin":
        out.pop("llm_prompts", None)
    if org_role not in ("admin", "tester"):
        out.pop("channel_search_quality", None)
    if org_role == "tester":
        _mask_config_telegram_for_tester(out)
    return out


def _merge_non_admin_llm_from_disk(org_id: int, payload: dict[str, Any], org_role: str) -> None:
    """Только админ может менять llm и llm_prompts; остальным подставляем сохранённое."""
    payload.pop("llm_key_configured", None)
    if org_role == "admin":
        return
    paths = _tenant_paths(org_id)
    old_llm: dict[str, Any] = {}
    old_prompts: dict[str, Any] = {}
    old_csq: dict[str, Any] = {}
    if paths["config"].exists():
        try:
            old_cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
            if isinstance(old_cfg.get("llm"), dict):
                old_llm = dict(old_cfg["llm"])
            lp = old_cfg.get("llm_prompts")
            if isinstance(lp, dict):
                old_prompts = dict(lp)
            csq = old_cfg.get("channel_search_quality")
            if isinstance(csq, dict):
                old_csq = dict(csq)
        except Exception:
            old_llm = {}
            old_prompts = {}
            old_csq = {}
    payload["llm"] = old_llm
    payload["llm_prompts"] = old_prompts
    payload["channel_search_quality"] = old_csq


def _normalize_target_chat_entry(raw: str) -> Any:
    s = raw.strip()
    if not s:
        return None
    if re.fullmatch(r"-?\d+", s):
        try:
            return int(s)
        except ValueError:
            return s
    return s


def _dialog_compare_key(ref: Any) -> str:
    norm = _normalize_target_chat_entry(str(ref or "").strip())
    return str(norm) if norm is not None else ""


def _dialog_merge_bucket_key(it: dict[str, Any]) -> str:
    """Ключ склейки одного чата между аккаунтами: merge_key из бота (-100…) или fallback по ref."""
    mk = it.get("merge_key")
    if isinstance(mk, str) and mk.strip():
        return mk.strip()
    ref_raw = it.get("ref")
    return _dialog_compare_key(ref_raw or "")


def _channel_search_exclude_key_variants(val: Any) -> set[str]:
    """Нормализация ref для пересечения с результатами поиска (@user / -100… / id)."""
    out: set[str] = set()
    if val is None:
        return out
    if isinstance(val, int):
        s = str(val)
    else:
        s = str(val).strip()
    if not s:
        return out
    out.add(s)
    low = s.lower()
    if s.startswith("@"):
        out.add(low)
        out.add(low[1:])
    elif re.fullmatch(r"-?\d+", s):
        out.add(s)
        if s.startswith("-100"):
            out.add(s[4:])
    else:
        out.add(low)
        out.add(f"@{low}")
    return out


def _channel_search_exclude_normalized_set(cfg: dict[str, Any]) -> set[str]:
    raw = cfg.get("channel_search_exclude")
    keys: set[str] = set()
    if not isinstance(raw, list):
        return keys
    for e in raw:
        keys |= _channel_search_exclude_key_variants(e)
    return keys


def _search_result_item_key_variants(it: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    pid = it.get("id")
    if pid is not None and str(pid).strip():
        out |= _channel_search_exclude_key_variants(str(pid).strip())
    u = (it.get("username") or "").strip()
    if u:
        out |= _channel_search_exclude_key_variants(u if u.startswith("@") else f"@{u}")
    return out


def _filter_channel_search_items_by_exclude(items: list[Any], ex_keys: set[str]) -> list[Any]:
    if not ex_keys:
        return items
    out: list[Any] = []
    for it in items:
        if not isinstance(it, dict):
            out.append(it)
            continue
        if _search_result_item_key_variants(it) & ex_keys:
            continue
        out.append(it)
    return out


def _list_chats_json_items_to_unique_norms(raw_items: Any) -> list[Any]:
    """Преобразует items из вывода --list-chats-json в нормализованные записи target_chats без дубликатов."""
    ordered_unique: list[Any] = []
    seen: set[str] = set()
    if not isinstance(raw_items, list):
        return ordered_unique
    for it in raw_items:
        ref_raw = (it or {}).get("ref") if isinstance(it, dict) else None
        if not ref_raw:
            continue
        norm = _normalize_target_chat_entry(str(ref_raw))
        if norm is None:
            continue
        k = str(norm)
        if k in seen:
            continue
        seen.add(k)
        ordered_unique.append(norm)
    return ordered_unique


def _parse_llm_keyword_lines(text: str, max_count: int) -> list[str]:
    """Разбор ответа модели в список фраз для поиска (строки + запятые/точки с запятой в одной строке)."""
    out: list[str] = []
    seen: set[str] = set()

    def add_candidate(frag: str) -> None:
        if len(out) >= max_count:
            return
        s = (frag or "").strip()
        if not s:
            return
        s = re.sub(r"^[\d\.\)\-]+\s*", "", s)
        s = s.strip("•-*–— \"'«»").strip()
        if len(s) < 2:
            return
        key = s.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(s)

    raw = (text or "").strip()
    if not raw:
        return out

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        line = re.sub(r"^[\d\.\)\-]+\s*", "", line)
        line = line.strip("•-*–— ").strip()
        if not line:
            continue
        if "," in line or ";" in line:
            parts = re.split(r"[,;]+", line)
            if len(parts) > 1:
                for p in parts:
                    add_candidate(p)
                    if len(out) >= max_count:
                        return out
                continue
        add_candidate(line)
        if len(out) >= max_count:
            return out

    if len(out) < 2:
        for part in re.split(r"[\n,;•|]+", raw):
            add_candidate(part)
            if len(out) >= max_count:
                break

    return out


@app.post("/api/search/stop")
@limiter.limit("60 per minute", methods=["POST"])
def api_search_stop():
    """Остановить активный пакетный поиск каналов для текущей организации.

    Прерывает все Telethon-подпроцессы, запущенные через
    `run_bot_json_stdout_cancellable`. Затем (на всякий случай) снимает
    висячие `*.session.lock`. Безопасно вызывать многократно.
    """
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    killed = _stop_search_subprocesses_for_org(int(org_id))
    locks_removed = 0
    locks: list[Path] = []
    try:
        locks.extend(SESSIONS_DIR.glob("*.session.lock"))
    except OSError:
        pass
    try:
        tenant_sess_dir = TENANTS_DIR / f"org_{int(org_id)}" / "sessions"
        if tenant_sess_dir.is_dir():
            locks.extend(tenant_sess_dir.glob("*.session.lock"))
    except OSError:
        pass
    seen: set[Path] = set()
    for lock_p in locks:
        if lock_p in seen:
            continue
        seen.add(lock_p)
        try:
            lock_p.unlink(missing_ok=True)
            locks_removed += 1
        except OSError:
            continue
    if killed or locks_removed:
        msg = f"Остановлено подпроцессов: {killed}; снято блокировок: {locks_removed}."
    else:
        msg = "Активных поисков не найдено."
    return jsonify({"message": msg, "killed": killed, "locks_removed": locks_removed})


@app.post("/api/search/clear-session-locks")
@limiter.limit("12 per minute", methods=["POST"])
def api_search_clear_session_locks():
    """Аварийная кнопка для UI: снимает «висячие» lock-файлы сессий Telethon
    у текущей организации.

    Безопасно для пользователя: если на самом деле идёт операция Telegram (бот
    работает, поиск выполняется), эти процессы создадут lock-файл заново сразу
    после короткой проверки. Поэтому действие не прерывает живые задачи —
    только убирает остатки аварийных рестартов.
    """
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    removed = 0
    seen: set[Path] = set()
    locks: list[Path] = []
    try:
        locks.extend(SESSIONS_DIR.glob("*.session.lock"))
    except OSError:
        pass
    try:
        tenant_sess_dir = TENANTS_DIR / f"org_{int(org_id)}" / "sessions"
        if tenant_sess_dir.is_dir():
            locks.extend(tenant_sess_dir.glob("*.session.lock"))
    except OSError:
        pass
    for lock_p in locks:
        if lock_p in seen:
            continue
        seen.add(lock_p)
        try:
            lock_p.unlink(missing_ok=True)
            removed += 1
        except OSError:
            continue
    msg = (
        f"Снято блокировок сессий: {removed}."
        if removed
        else "Висячих блокировок не найдено."
    )
    return jsonify({"message": msg, "removed": removed})


@app.post("/api/search/generate-keywords")
@limiter.limit("30 per minute", methods=["POST"])
def api_search_generate_keywords():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    body = request.get_json(force=True, silent=True) or {}
    niche = str(body.get("niche", "")).strip()
    audience = str(body.get("audience", "")).strip()
    stop_words_raw = str(body.get("stop_words", "")).strip()
    try:
        count = int(body.get("count", 15))
    except (TypeError, ValueError):
        count = 15
    count = max(3, min(50, count))
    if len(niche) < 3:
        return jsonify({"message": "Опишите нишу не короче 3 символов"}), 400
    paths = _tenant_paths(org_id)
    if not paths["config"].exists() and CONFIG_PATH.exists():
        _bootstrap_tenant_config_from_root(paths["config"])
    if not paths["config"].exists():
        return jsonify({"message": "Нет настроек организации"}), 400
    try:
        cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        return jsonify({"message": "Файл настроек повреждён"}), 400
    llm = cfg.get("llm", {})
    if not isinstance(llm, dict) or not llm.get("enabled"):
        return jsonify(
            {
                "message": "LLM выключен или не настроен. Это делает администратор: «Настройки и лимиты» → «Подключения».",
            }
        ), 400
    api_key = str(llm.get("api_key", "")).strip()
    if not api_key:
        return jsonify(
            {
                "message": "API key LLM не задан. Попросите администратора сохранить ключ в «Настройки и лимиты» → «Подключения».",
            }
        ), 400
    base_url = str(llm.get("base_url", "") or "https://api.openai.com/v1").strip()
    model = str(llm.get("model", "") or "gpt-4o-mini").strip()
    extra_aud = f"\nЦелевая аудитория / кто в чате: {audience}." if len(audience) >= 2 else ""
    extra_stop = (
        f"\nНе используй и не близко к: {stop_words_raw}."
        if len(stop_words_raw) >= 2
        else ""
    )
    pr = effective_llm_prompts(cfg)
    system = pr["search_keywords_system"]
    user_msg = format_llm_prompt(
        pr["search_keywords_user"],
        niche=niche,
        extra_aud=extra_aud,
        extra_stop=extra_stop,
        count=str(count),
    )
    try:
        raw = _openai_chat_completion(base_url, api_key, model, system, user_msg)
    except ValueError as exc:
        return jsonify({"message": str(exc)}), 502
    keywords = _parse_llm_keyword_lines(raw, count)
    if stop_words_raw and keywords:
        bad = {w.strip().lower() for w in re.split(r"[\n,;]+", stop_words_raw) if w.strip()}
        if bad:
            keywords = [k for k in keywords if not any(b in k.lower() for b in bad if len(b) >= 2)]
    if len(keywords) < 2:
        return (
            jsonify(
                {
                    "message": "Модель вернула слишком мало фраз. Попробуйте уточнить нишу или повторить запрос.",
                    "keywords": keywords,
                }
            ),
            422,
        )
    return jsonify({"keywords": keywords})


@app.post("/api/config/generate-keywords-group")
@limiter.limit("20 per minute", methods=["POST"])
def api_config_generate_keywords_group():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    body = request.get_json(force=True, silent=True) or {}
    niche = str(body.get("niche", "")).strip()
    audience = str(body.get("audience", "")).strip()
    kind = str(body.get("kind", "")).strip()
    try:
        count = int(body.get("count", 20))
    except (TypeError, ValueError):
        count = 20
    count = max(3, min(60, count))
    if kind not in CONFIG_KEYWORD_GROUP_INSTRUCTIONS:
        return jsonify({"message": f"Неизвестный kind: {kind}"}), 400
    if len(niche) < 3:
        return jsonify({"message": "Опишите нишу не короче 3 символов"}), 400
    paths = _tenant_paths(org_id)
    if not paths["config"].exists() and CONFIG_PATH.exists():
        _bootstrap_tenant_config_from_root(paths["config"])
    if not paths["config"].exists():
        return jsonify({"message": "Нет настроек организации"}), 400
    try:
        cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        return jsonify({"message": "Файл настроек повреждён"}), 400
    llm = cfg.get("llm", {})
    if not isinstance(llm, dict) or not llm.get("enabled"):
        return jsonify({"message": "LLM выключен или не настроен."}), 400
    api_key = str(llm.get("api_key", "")).strip()
    if not api_key:
        return jsonify({"message": "API key LLM не задан."}), 400
    base_url = str(llm.get("base_url", "") or "https://api.openai.com/v1").strip()
    model = str(llm.get("model", "") or "gpt-4o-mini").strip()
    hint = CONFIG_KEYWORD_GROUP_INSTRUCTIONS[kind]
    extra_aud = f"\nАудитория/контекст: {audience}." if len(audience) >= 2 else ""
    pr = effective_llm_prompts(cfg)
    system = pr["config_keywords_system"]
    user_msg = format_llm_prompt(
        pr["config_keywords_user"],
        niche=niche,
        extra_aud=extra_aud,
        count=str(count),
        kind=kind,
        hint=hint,
    )
    try:
        raw = _openai_chat_completion(base_url, api_key, model, system, user_msg)
    except ValueError as exc:
        return jsonify({"message": str(exc)}), 502
    keywords = _parse_llm_keyword_lines(raw, count)
    if len(keywords) < 2:
        return (
            jsonify(
                {
                    "message": "Модель вернула слишком мало фраз. Уточните нишу или повторите запрос.",
                    "keywords": keywords,
                }
            ),
            422,
        )
    return jsonify({"keywords": keywords, "kind": kind})


@app.post("/api/search/channels")
@limiter.limit("30 per minute", methods=["POST"])
def api_search_channels():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    proc = bot_processes.get(org_id)
    if proc and proc.poll() is None:
        return (
            jsonify(
                {
                    "message": "Остановите бота на странице «Управление»: поиск использует ту же сессию Telegram.",
                }
            ),
            409,
        )
    body = request.get_json(force=True, silent=True) or {}
    q = str(body.get("query", "")).strip()
    if len(q) < 2:
        return jsonify({"message": "Введите не меньше 2 символов для поиска"}), 400
    try:
        limit = int(body.get("limit", 20))
    except (TypeError, ValueError):
        limit = 20
    limit = max(5, min(50, limit))
    try:
        min_subscribers = int(body.get("min_subscribers", 0) or 0)
    except (TypeError, ValueError):
        min_subscribers = 0
    min_subscribers = max(0, min(10_000_000, min_subscribers))
    try:
        max_inactive_days = int(body.get("max_inactive_days", 0) or 0)
    except (TypeError, ValueError):
        max_inactive_days = 0
    max_inactive_days = max(0, min(365, max_inactive_days))
    include_stale = bool(body.get("include_stale", False))
    enrich = bool(body.get("enrich", False)) or max_inactive_days > 0
    search_options: dict[str, Any] = {
        "min_subscribers": min_subscribers,
        "max_inactive_days": max_inactive_days,
        "include_stale": include_stale,
        "enrich": enrich,
    }
    if bool(body.get("via_comments")):
        search_options["via_comments"] = True
        bio_kw = body.get("bio_keywords")
        if isinstance(bio_kw, str) and bio_kw.strip():
            search_options["bio_keywords"] = bio_kw.strip()
        elif isinstance(bio_kw, list):
            search_options["bio_keywords"] = bio_kw
        for opt_key in ("comments_messages_per_channel", "commenters_max_per_channel"):
            if opt_key not in body:
                continue
            try:
                search_options[opt_key] = int(body[opt_key])
            except (TypeError, ValueError):
                pass
    if bool(body.get("require_discussion")):
        search_options["require_discussion"] = True
    paths = _tenant_paths(org_id)
    if not paths["config"].exists() and CONFIG_PATH.exists():
        _bootstrap_tenant_config_from_root(paths["config"])
    if not paths["config"].exists():
        return jsonify({"message": "Нет config для организации"}), 400
    try:
        cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        return jsonify({"message": "config повреждён"}), 400
    _in_memory_telegram_accounts_migrate(cfg, org_id)
    qc = cfg.get("channel_search_quality")
    if isinstance(qc, dict) and bool(qc.get("enabled")):
        search_options["quality"] = qc
        if bool(qc.get("force_require_discussion")):
            search_options["require_discussion"] = True
    # Длительные режимы поиска (обход комментариев/о себе, анализ качества) могут занимать минуты.
    # Таймаут подпроцесса держим больше дефолтных 180с, иначе UI видит «прерван» на больших списках.
    timeout_sec: int | None = None
    try:
        if bool(search_options.get("via_comments")):
            timeout_sec = 900
        if isinstance(search_options.get("quality"), dict) and bool(search_options["quality"].get("enabled")):
            timeout_sec = max(timeout_sec or 0, 900)
    except Exception:
        timeout_sec = None
    login_code_file = paths["base"] / ".telegram_login_code"

    raw_aid = body.get("account_ids")
    account_ids: list[str] = []
    if isinstance(raw_aid, list):
        for x in raw_aid:
            s = str(x).strip()
            if s and s not in account_ids:
                account_ids.append(s)
    if len(account_ids) > 1:
        cap = _effective_max_telegram_accounts(org_id)
        use_ids = account_ids[:cap]
        merged: list[Any] = []
        seen_keys: set[str] = set()
        errors: list[str] = []

        def run_search_one(aid: str) -> tuple[str, dict[str, Any]]:
            sp = _session_path_for_telegram_account(org_id, cfg, aid)
            try:
                with _tg_session_lock(sp, action="search_channels"):
                    out = run_bot_json_stdout_cancellable(
                        [
                            sys.executable,
                            str(BOT_SCRIPT),
                            "--force-unlock",
                            *_bot_argv_paths_and_session(paths, sp),
                            "--login-code-file",
                            str(login_code_file),
                            "--search-channels",
                            q,
                            "--search-limit",
                            str(limit),
                            "--search-options-json",
                            json.dumps(search_options, ensure_ascii=False, separators=(",", ":")),
                        ],
                        org_id=int(org_id),
                        timeout_sec=timeout_sec,
                    )
            except TimeoutError as exc:
                out = {"ok": False, "error": _humanize_tg_session_lock_error(exc)}
            return aid, out

        for aid in use_ids:
            try:
                _rid, payload = run_search_one(aid)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{aid}: {exc}")
                continue
            if payload.get("cancelled"):
                errors.append(f"{aid}: остановлено пользователем")
                break
            if not payload.get("ok"):
                errors.append(f"{aid}: {payload.get('error', 'ошибка')}")
                continue
            raw_items = payload.get("items")
            if not isinstance(raw_items, list):
                continue
            for it in raw_items:
                if not isinstance(it, dict):
                    continue
                key = str(it.get("id") or it.get("username") or "") or str(it)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                it2 = dict(it)
                it2["found_by_account"] = aid
                merged.append(it2)
        if not merged and errors:
            return jsonify({"message": "; ".join(errors)[:800]}), 502
        ex_keys = _channel_search_exclude_normalized_set(cfg)
        merged = _filter_channel_search_items_by_exclude(merged, ex_keys)
        return jsonify({"items": merged, "errors": errors or None})

    acc_for_session: str | None
    if len(account_ids) == 1:
        acc_for_session = account_ids[0]
    else:
        single = str(body.get("telegram_account_id") or "").strip()
        acc_for_session = single or None
    session_path = _session_path_for_telegram_account(org_id, cfg, acc_for_session)
    try:
        with _tg_session_lock(session_path, action="search_channels"):
            payload = run_bot_json_stdout_cancellable(
                [
                    sys.executable,
                    str(BOT_SCRIPT),
                    "--force-unlock",
                    *_bot_argv_paths_and_session(paths, session_path),
                    "--login-code-file",
                    str(login_code_file),
                    "--search-channels",
                    q,
                    "--search-limit",
                    str(limit),
                    "--search-options-json",
                    json.dumps(search_options, ensure_ascii=False, separators=(",", ":")),
                ],
                org_id=int(org_id),
                timeout_sec=timeout_sec,
            )
    except TimeoutError as exc:
        return jsonify({"message": _humanize_tg_session_lock_error(exc)}), 409
    if payload.get("cancelled"):
        return jsonify({"message": "Поиск остановлен пользователем.", "cancelled": True}), 499
    if not payload.get("ok"):
        return jsonify({"message": str(payload.get("error", "Ошибка поиска"))[:800]}), 502
    items = payload.get("items")
    if not isinstance(items, list):
        items = []
    ex_keys = _channel_search_exclude_normalized_set(cfg)
    items = _filter_channel_search_items_by_exclude(items, ex_keys)
    return jsonify({"items": items})


@app.post("/api/search/add-to-monitoring")
@limiter.limit("30 per minute", methods=["POST"])
def api_search_add_to_monitoring():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    body = request.get_json(force=True, silent=True) or {}
    raw_items = body.get("chats")
    if not isinstance(raw_items, list) or not raw_items:
        return jsonify({"message": "Передайте chats: непустой массив ссылок (@channel или -100…)"}), 400
    auto_join = bool(body.get("auto_join", True))
    include_discussion = bool(body.get("include_discussion", True))
    about_links = str(body.get("about_links", "list") or "list").strip().lower()
    if about_links not in ("skip", "list", "join"):
        about_links = "list"
    need_enroll = auto_join or include_discussion or about_links != "skip"

    paths = _tenant_paths(org_id)
    if not paths["config"].exists():
        return jsonify({"message": "Нет config для организации"}), 400
    try:
        cfg = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        return jsonify({"message": "config повреждён"}), 400

    proc = bot_processes.get(org_id)
    if need_enroll and proc and proc.poll() is None:
        return (
            jsonify(
                {
                    "message": "Остановите бота на странице «Управление»: добавление с вступлением в Telegram использует ту же сессию.",
                }
            ),
            409,
        )

    enroll_extras: list[Any] = []
    enroll_details: list[Any] | None = None
    if need_enroll:
        raw_aid = body.get("account_ids")
        account_ids: list[str] = []
        if isinstance(raw_aid, list):
            for x in raw_aid:
                s = str(x).strip()
                if s and s not in account_ids:
                    account_ids.append(s)
        acc_for_session: str | None
        if len(account_ids) == 1:
            acc_for_session = account_ids[0]
        else:
            single = str(body.get("telegram_account_id") or "").strip()
            acc_for_session = single or None
        if len(account_ids) > 1:
            return (
                jsonify(
                    {
                        "message": "Добавление с вступлением в Telegram сейчас только с одного аккаунта: отметьте один чекбокс или не передавайте несколько account_ids.",
                    }
                ),
                400,
            )
        session_path = _session_path_for_telegram_account(org_id, cfg, acc_for_session)
        login_code_file = paths["base"] / ".telegram_login_code"
        enroll_opts: dict[str, Any] = {
            "auto_join": auto_join,
            "include_discussion": include_discussion,
            "about_links": about_links,
        }
        try:
            jg = float(body.get("join_gap_sec", 1.2) or 1.2)
        except (TypeError, ValueError):
            jg = 1.2
        enroll_opts["join_gap_sec"] = max(0.2, min(120.0, jg))
        refs_clean = [str(x).strip() for x in raw_items if str(x).strip()]
        try:
            with _tg_session_lock(session_path, action="enroll_monitoring"):
                payload = run_bot_json_stdout(
                    [
                        sys.executable,
                        str(BOT_SCRIPT),
                        "--force-unlock",
                        *_bot_argv_paths_and_session(paths, session_path),
                        "--login-code-file",
                        str(login_code_file),
                        "--enroll-monitoring-json",
                        json.dumps(refs_clean, ensure_ascii=False),
                        "--enroll-options-json",
                        json.dumps(enroll_opts, ensure_ascii=False, separators=(",", ":")),
                    ],
                    timeout_sec=900,
                )
        except TimeoutError:
            return jsonify({"message": "Сессия Telegram занята другим действием. Подождите 5–10 секунд и повторите."}), 409
        if not payload.get("ok"):
            return (
                jsonify(
                    {
                        "message": str(payload.get("error", "Ошибка вступления в Telegram"))[:900],
                    }
                ),
                502,
            )
        raw_ex = payload.get("extras")
        if isinstance(raw_ex, list):
            enroll_extras = [x for x in raw_ex]
        ed = payload.get("details")
        enroll_details = ed if isinstance(ed, list) else None

    existing = cfg.get("target_chats", [])
    if not isinstance(existing, list):
        existing = []
    seen: set[str] = {str(x) for x in existing}
    added = 0
    for item in raw_items:
        norm = _normalize_target_chat_entry(str(item))
        if norm is None:
            continue
        key = str(norm)
        if key in seen:
            continue
        existing.append(norm)
        seen.add(key)
        added += 1
    extras_added = 0
    for ex in enroll_extras:
        if isinstance(ex, int):
            norm = ex
        else:
            norm = _normalize_target_chat_entry(str(ex))
        if norm is None:
            continue
        key = str(norm)
        if key in seen:
            continue
        existing.append(norm)
        seen.add(key)
        extras_added += 1
    cfg["target_chats"] = existing
    before_trim = len(existing)
    plan = _get_plan_for_org(org_id)
    if plan:
        cfg = _enforce_plan_on_config(cfg, plan)
    final_list = cfg.get("target_chats", [])
    if not isinstance(final_list, list):
        final_list = []
    trimmed = before_trim - len(final_list)
    paths["config"].write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    msg = f"Добавлено новых чатов: {added}. Дополнительно из обсуждений/описания: {extras_added}. Всего в списке: {len(final_list)}."
    if trimmed > 0:
        msg += f" По лимиту тарифа убрано из списка: {trimmed}."
    out: dict[str, Any] = {
        "message": msg,
        "target_chats_count": len(final_list),
        "trimmed_due_to_plan": trimmed,
        "extras_from_enroll": extras_added,
    }
    if enroll_details is not None:
        out["enroll_details"] = enroll_details
    return jsonify(out)


@app.post("/api/search/create-folder")
@limiter.limit("20 per minute", methods=["POST"])
def api_search_create_folder():
    ok, err = require_org_role(_ORG_FUNNEL_ROLES)
    if not ok:
        payload, code = err  # type: ignore[misc]
        return jsonify(payload), code
    user = _session_user()
    assert user is not None
    org_id, _role = _user_org_role(int(user["id"]))
    proc = bot_processes.get(org_id)
    if proc and proc.poll() is None:
        return (
            jsonify(
                {
                    "message": "Остановите бота на странице «Управление»: создание папки использует ту же сессию Telegram.",
                }
            ),
            409,
        )
    body = request.get_json(force=True, silent=True) or {}
    folder_name = str(body.get("folder_name", "")).strip()
    chats = body.get("chats")
    if not folder_name:
        return jsonify({"message": "Укажите имя папки"}), 400
    if not isinstance(chats, list) or not chats:
        return jsonify({"message": "Передайте непустой массив chats"}), 400
    if len(chats) > 100:
        return (
            jsonify(
                {
                    "message": "В одной папке Telegram обычно не больше 100 чатов; уменьшите выбор.",
                }
            ),
            400,
        )
    paths = _tenant_paths(org_id)
    if not paths["config"].exists() and CONFIG_PATH.exists():
        _bootstrap_tenant_config_from_root(paths["config"])
    if not paths["config"].exists():
        return jsonify({"message": "Нет настроек организации"}), 400
    try:
        cfg_ff = json.loads(paths["config"].read_text(encoding="utf-8"))
    except Exception:
        return jsonify({"message": "config повреждён"}), 400
    acc_ff = str(body.get("telegram_account_id") or "").strip() or None
    session_path_ff = _session_path_for_telegram_account(org_id, cfg_ff, acc_ff)
    auto_join = bool(body.get("auto_join", True))
    folder_options_json = json.dumps(
        {"auto_join": auto_join}, ensure_ascii=False, separators=(",", ":")
    )
    login_code_file = paths["base"] / ".telegram_login_code"
    chats_json = json.dumps(chats, ensure_ascii=False)
    payload = run_bot_json_stdout(
        [
            sys.executable,
            str(BOT_SCRIPT),
            "--force-unlock",
            *_bot_argv_paths_and_session(paths, session_path_ff),
            "--login-code-file",
            str(login_code_file),
            "--create-folder",
            folder_name,
            "--folder-chats-json",
            chats_json,
            "--folder-options-json",
            folder_options_json,
        ]
    )
    if not payload.get("ok"):
        return jsonify({"message": str(payload.get("error", "Ошибка создания папки"))[:800]}), 502
    peers_added = int(payload.get("peers_added", 0) or 0)
    title = str(payload.get("title", folder_name))
    fid = payload.get("folder_id")
    manual = payload.get("manual_tme_links") or []
    if not isinstance(manual, list):
        manual = []
    only_manual = bool(payload.get("only_manual_links"))
    hint = str(payload.get("folder_hint_ru") or "").strip()
    auto_joined = int(payload.get("auto_joined_n", 0) or 0)
    used_auto = bool(payload.get("auto_join_used", auto_join))
    if only_manual and fid is None and not peers_added and manual:
        if used_auto:
            msg = (
                f"Папка не наполнена: к части чатов автовступ не прошёл (приват, лимит). "
                f"Ниже {len(manual)} ссылок — при необходимости откройте в Telegram, затем снова «Создать папку»."
            )
        else:
            msg = (
                f"Папка не наполнена (автовступ выключен; вы ещё не в этих чатах). "
                f"Откройте {len(manual)} ссылок или подпишитесь в клиенте, затем снова «Создать папку»."
            )
    elif only_manual and fid is not None and not peers_added and manual:
        msg = (
            f"Создана папка «{title}» (пока пустая). "
            f"По {len(manual)} ссылкам вступить не вышло (приват, лимит) — "
            f"вступайте вручную, затем снова «Создать папку» с тем же выбором."
        )
    elif hint:
        msg = f"Папка «{title}». {hint}" if "\n" not in hint else hint
    else:
        msg = (
            f"Папка «{title}» готова: {peers_added} чат(ов) в папке"
            f"{f', {auto_joined} с автовступом' if auto_joined and used_auto else ''}."
            f" Проверьте список папок в Telegram."
        )
    return jsonify(
        {
            "message": msg,
            "folder_id": fid,
            "peers_added": peers_added,
            "auto_joined_n": auto_joined,
            "no_auto_subscribe": bool(payload.get("no_auto_subscribe")),
            "only_manual_links": only_manual,
            "manual_tme_links": manual,
            "peer_errors": payload.get("peer_errors") or [],
            "folder_hint_ru": hint or None,
        }
    )


_migrate_sessions_from_root()
init_db()
_ensure_web_jobs_worker()


if __name__ == "__main__":
    app.run(host=HOST, port=PORT, debug=False)
