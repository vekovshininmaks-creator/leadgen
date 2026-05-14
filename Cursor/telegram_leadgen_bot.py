import asyncio
import argparse
import atexit
import csv
import json
import logging
import sqlite3
import os
import random
import re
import sys
import unicodedata
import time
import urllib.error
import urllib.request
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Set

from leadgen_prompts import effective_llm_prompts, format_llm_prompt

from telethon import TelegramClient, events, functions, utils
from telethon.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    UserAlreadyParticipantError,
    UserPrivacyRestrictedError,
)
from telethon.tl.types import Channel, Chat, DialogFilter, User

CONFIG_PATH = Path("config.json")
SESSIONS_DIR = Path("sessions")
CSV_PATH = Path("sent_leads.csv")
STATE_PATH = Path("state.json")
CSV_FIELDNAMES = [
    "timestamp",
    "username",
    "user_id",
    "source_chat",
    "message_id",
    "message",
    "stage",
    "status",
    "matched_keyword",
    "lead_tag",
    "deleted",
]
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "bot.log"
RUN_LOCK_PATH = Path(".bot_run.lock")

logger = logging.getLogger("leadgen_bot")


def _dt_to_utc_iso(dt: datetime | None) -> str:
    """ISO UTC для журнала: время исходного сообщения в чате или текущий момент (ЛС / служебные записи)."""
    if dt is None:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")
    if dt.tzinfo is None:
        d = dt.replace(tzinfo=timezone.utc)
    else:
        d = dt.astimezone(timezone.utc)
    return d.isoformat(timespec="seconds")


class BotAlreadyRunningError(Exception):
    pass


def setup_logging(log_file: Path = LOG_FILE) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=1_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


@dataclass
class PendingDM:
    user_id: int
    username: str
    stage: int
    source_chat: str
    due_at: datetime
    template_text: str
    task_hint: str = ""
    lead_snippet: str = ""
    trigger_match: str = ""
    force_template: bool = False
    outreach_db_id: int | None = None


def require_telegram_api_credentials(config: Dict[str, Any]) -> tuple[int, str]:
    """api_id / api_hash обязательны для Telethon; в JSON могут быть пустыми до заполнения в UI."""
    aid = config.get("api_id")
    ah = config.get("api_hash")
    if aid is None or str(aid).strip() == "":
        raise ValueError(
            "В конфиге не задан api_id. Получите пару api_id / api_hash на https://my.telegram.org "
            "и укажите в настройках вашей организации (не используйте чужие ключи)."
        )
    if ah is None or str(ah).strip() == "":
        raise ValueError("В конфиге не задан api_hash (см. https://my.telegram.org).")
    try:
        api_id = int(aid)
    except (TypeError, ValueError) as exc:
        raise ValueError("api_id должен быть числом.") from exc
    return api_id, str(ah).strip()


def _normalize_llm_model_id(base_url: str, model: str) -> str:
    """Синхронно с web_app: актуальные id у Groq / Cerebras (списки моделей меняются)."""
    b = (base_url or "").lower()
    m = (model or "").strip()
    if not m:
        return m
    if "groq.com" in b:
        groq = {
            "llama-3.3-70b": "llama-3.3-70b-versatile",
            "llama3.3-70b": "llama-3.3-70b-versatile",
        }
        return groq.get(m.lower(), m)
    if "cerebras.ai" in b:
        cerebras = {
            "llama-3.3-70b": "gpt-oss-120b",
            "llama-3.3-70b-versatile": "gpt-oss-120b",
        }
        return cerebras.get(m.lower(), m)
    return m


# Как в web_app: Cloudflare часто режет urllib без User-Agent.
_LLM_UA = os.getenv(
    "LLM_USER_AGENT",
    "Mozilla/5.0 (compatible; OpenAI-Compatible-Client/1.0; Leadgen-LLM) Python/urllib",
)


def _llm_http_error_hint_sync(status: int, detail: str) -> str:
    s = (detail or "").lower()
    if status == 403 and "1010" in detail:
        return (
            " [Подсказка] Код 1010 — часто блокировка Cloudflare. Задайте LLM_USER_AGENT или смените провайдера "
            "(например Groq: https://api.groq.com/openai/v1). Отключите VPN, если сеть «серая»."
        )
    if status == 403 and (
        "country" in s or "region" in s or "forbidden" in s or "not supported" in s or "permission" in s
    ):
        return (
            " [Подсказка] 403: проверьте ключ, регион провайдера и проект биллинга."
        )
    if status == 401:
        return " [Подсказка] Проверьте API key в настройках (без пробелов, ключ не отозван)."
    if status == 404 and "model" in s and ("not_found" in s or "does not exist" in s):
        return (
            " [Подсказка] Модель не найдена у провайдера. Для Cerebras: gpt-oss-120b; для Groq: llama-3.3-70b-versatile."
        )
    return ""


_TME_BIO_USER_RE = re.compile(
    r"(?:@|(?:https?://)?(?:t\.me|telegram\.me)/)([a-zA-Z][a-zA-Z0-9_]{3,31})\b",
    re.IGNORECASE,
)
_TME_JOIN_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/|\+)([a-zA-Z0-9_-]+)",
    re.IGNORECASE,
)


def _normalize_bio_keywords(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [p.strip().lower() for p in re.split(r"[\n,;]+", raw) if p.strip()]
    if isinstance(raw, list):
        out: list[str] = []
        for x in raw:
            s = str(x).strip().lower()
            if s:
                out.append(s)
        return out
    return []


def _parse_channel_refs_from_bio(about: str) -> list[str]:
    """Из поля «о себе» — публичные @username и ссылки t.me (в т.ч. +invite)."""
    if not (about or "").strip():
        return []
    refs: list[str] = []
    seen: set[str] = set()
    for m in _TME_BIO_USER_RE.finditer(about):
        u = m.group(1)
        key = u.lower()
        if key in seen:
            continue
        seen.add(key)
        refs.append(u)
    for m in _TME_JOIN_RE.finditer(about):
        slug = m.group(1)
        href = f"https://t.me/+{slug}"
        if href in seen:
            continue
        seen.add(href)
        refs.append(href)
    return refs


async def _expand_search_via_comment_bios(
    client: TelegramClient,
    pairs: list[tuple[Any, dict[str, Any]]],
    opts: dict[str, Any],
    existing_keys: set[str],
    *,
    min_subscribers: int,
) -> list[dict[str, Any]]:
    """Для каналов с обсуждением: читает комментарии, профили авторов, каналы из bio."""
    if not bool(opts.get("via_comments") or opts.get("via_comment_bios")):
        return []
    try:
        msgs_lim = int(opts.get("comments_messages_per_channel", 35) or 35)
    except (TypeError, ValueError):
        msgs_lim = 35
    msgs_lim = max(5, min(100, msgs_lim))
    try:
        users_lim = int(opts.get("commenters_max_per_channel", 12) or 12)
    except (TypeError, ValueError):
        users_lim = 12
    users_lim = max(3, min(40, users_lim))
    bio_kws = _normalize_bio_keywords(opts.get("bio_keywords"))

    out_extra: list[dict[str, Any]] = []
    seen_new: set[str] = set(existing_keys)

    for chat, row in pairs:
        if not isinstance(chat, Channel):
            continue
        if not bool(getattr(chat, "broadcast", False)) or bool(getattr(chat, "megagroup", False)):
            continue
        await asyncio.sleep(0.08)
        try:
            inp = await client.get_input_entity(chat)
            full_ch = await client(functions.channels.GetFullChannelRequest(channel=inp))
        except Exception as exc:  # noqa: BLE001
            logger.info("comment-expand: GetFullChannel %s: %s", row.get("title"), exc)
            continue
        linked = getattr(full_ch.full_chat, "linked_chat_id", None)
        if not linked:
            continue
        disc_id = int(f"-100{linked}")
        try:
            disc_ent = await client.get_entity(disc_id)
        except Exception as exc:  # noqa: BLE001
            logger.info("comment-expand: discussion entity %s: %s", disc_id, exc)
            continue

        user_ids: list[int] = []
        seen_uid: set[int] = set()
        try:
            async for msg in client.iter_messages(disc_ent, limit=msgs_lim):
                uid = getattr(msg, "sender_id", None)
                if uid is None or int(uid) <= 0:
                    continue
                ii = int(uid)
                if ii in seen_uid:
                    continue
                seen_uid.add(ii)
                user_ids.append(ii)
                if len(user_ids) >= users_lim * 4:
                    break
        except Exception as exc:  # noqa: BLE001
            logger.info("comment-expand: iter_messages %s: %s", disc_id, exc)
            continue

        processed = 0
        seed_title = str(row.get("title") or "")
        seed_un = str(row.get("username") or "").strip()
        seed_ref = f"@{seed_un}" if seed_un else str(row.get("id") or "")

        for uid in user_ids:
            if processed >= users_lim:
                break
            await asyncio.sleep(0.12)
            try:
                full_u = await client(functions.users.GetFullUserRequest(id=uid))
            except Exception:  # noqa: BLE001
                continue
            users_list = getattr(full_u, "users", None) or []
            user = users_list[0] if users_list else None
            if not isinstance(user, User) or user.bot:
                continue
            about = getattr(full_u.full_user, "about", "") or ""
            if bio_kws and not any(k in about.lower() for k in bio_kws):
                continue
            processed += 1
            cname = (user.username or "").strip()
            cmark = f"@{cname}" if cname else str(uid)
            for ref in _parse_channel_refs_from_bio(about)[:8]:
                await asyncio.sleep(0.1)
                try:
                    ent = await client.get_entity(ref)
                except Exception:  # noqa: BLE001
                    continue
                if not isinstance(ent, Channel):
                    continue
                username = (getattr(ent, "username", None) or "") or ""
                cid = getattr(ent, "id", None)
                if cid is None:
                    continue
                peer_id = f"-100{cid}" if not str(cid).startswith("-") else str(cid)
                if peer_id in seen_new:
                    continue
                if username:
                    lu = username.lower()
                    if lu in seen_new or f"@{lu}" in seen_new:
                        continue
                title = _search_result_title_for_channel(ent)
                participants = getattr(ent, "participants_count", None)
                if min_subscribers > 0 and participants is not None and int(participants) < min_subscribers:
                    continue
                seen_new.add(peer_id)
                if username:
                    seen_new.add(username.lower())
                    seen_new.add(f"@{username.lower()}")
                out_extra.append(
                    {
                        "title": title,
                        "username": username,
                        "id": peer_id,
                        "participants": participants,
                        "is_broadcast": bool(getattr(ent, "broadcast", False)),
                        "is_megagroup": bool(getattr(ent, "megagroup", False)),
                        "last_post_iso": None,
                        "inactive_days": None,
                        "activity": "unknown",
                        "found_via": "comment_bio",
                        "seed_channel": seed_ref,
                        "seed_title": seed_title,
                        "commenter": cmark,
                        "bio_snippet": (about[:200] + "…") if len(about) > 200 else about,
                    }
                )
    return out_extra


class LeadGenBot:
    def __init__(
        self,
        config: Dict[str, Any],
        *,
        login_code_file: Path | None = None,
        org_id: int | None = None,
        data_db_path: Path | None = None,
    ) -> None:
        self.config = config
        self.login_code_file = login_code_file
        self.org_id = org_id
        self.data_db_path = data_db_path
        self.human_approval_for_dm = bool(config.get("human_approval_for_dm", False))
        _st = config.get("human_approval_stages")
        self.human_approval_stages: dict[str, bool] = (
            {str(k): bool(v) for k, v in _st.items()} if isinstance(_st, dict) else {}
        )
        api_id, api_hash = require_telegram_api_credentials(config)
        self.client = TelegramClient(
            config["session_name"],
            api_id,
            api_hash,
        )

        self.hot_keywords = [kw.lower().strip() for kw in config["keywords"]["hot_lead"]]
        self.negative_keywords = [kw.lower().strip() for kw in config["keywords"]["negative"]]
        self.exclude_hot_keywords = [
            kw.lower().strip()
            for kw in config["keywords"].get(
                "exclude_hot_lead",
                [
                    "наши услуги",
                    "пишите в лс",
                    "пишите в личные сообщения",
                    "подключу",
                    "приведу клиентов",
                    "seo-компания",
                    "бесплатный разбор",
                    "скидывайте кейсы",
                    "я веб-дизайнер",
                    "мой канал",
                ],
            )
        ]
        self.required_intent_keywords = [
            kw.lower().strip()
            for kw in config["keywords"].get(
                "required_intent_hot_lead",
                [
                    "ищу",
                    "нужен",
                    "нужна",
                    "нужно",
                    "кто сделает",
                    "кто делает",
                    "сколько стоит",
                    "нужен сайт",
                    "нужна разработка",
                    "подскажите разработчика",
                ],
            )
        ]
        self.qualification_keywords = [
            kw.lower().strip() for kw in config["keywords"]["qualification"]
        ]
        self.interested_keywords = [kw.lower().strip() for kw in config["keywords"]["interested"]]
        self.bio_block_keywords = [kw.lower().strip() for kw in config["keywords"]["bio_block"]]

        self.monitor_interval = int(config["limits"]["monitor_interval_sec"])
        self.daily_limit_min = int(config["limits"]["daily_limit_range"][0])
        self.daily_limit_max = int(config["limits"]["daily_limit_range"][1])
        self.max_dm_month = int(config["limits"].get("max_dm_month", 10_000_000))
        self.max_dm_per_hour_per_chat = int(config["limits"]["max_dm_per_hour_per_chat"])
        self.typing_delay_min = int(config["limits"]["typing_delay_sec"][0])
        self.typing_delay_max = int(config["limits"]["typing_delay_sec"][1])
        self.per_chat_scan_delay_min = float(config["limits"].get("per_chat_scan_delay_sec", [0.35, 0.9])[0])
        self.per_chat_scan_delay_max = float(config["limits"].get("per_chat_scan_delay_sec", [0.35, 0.9])[1])
        self.fetch_limit_per_chat = int(config["limits"].get("fetch_limit_per_chat", 50))
        raw_max_pass = config.get("limits", {}).get("max_monitor_passes", 0)
        try:
            self.max_monitor_passes = max(0, int(raw_max_pass))
        except (TypeError, ValueError):
            self.max_monitor_passes = 0
        self.monitor_passes_done = 0

        self.templates = config["templates"]
        self.partner_name = config["partner_name"]
        self.dry_run = bool(config.get("dry_run", False))

        self.target_chats = self._normalize_target_chats(config["target_chats"])
        self.scan_chats = list(self.target_chats)
        self.pending_dms: list[PendingDM] = []
        self.contacted_users: Set[int] = set()
        self.blacklist_users: Set[int] = set()
        self.last_seen_msg_id: Dict[str, int] = {}
        self.private_stage: Dict[int, int] = {}
        self.private_task_hint: Dict[int, str] = {}
        self.last_private_reply_at: Dict[int, str] = {}
        self.last_stage_sent_at: Dict[int, Dict[str, str]] = {}
        self.daily_sent_count = 0
        self.current_day = date.today()
        self.daily_limit = random.randint(self.daily_limit_min, self.daily_limit_max)
        self.monthly_sent_count = 0
        self.current_month = f"{date.today().year:04d}-{date.today().month:02d}"
        self.hourly_chat_sends: Dict[str, deque[datetime]] = defaultdict(deque)
        self.invalid_chats: Set[str] = set()
        self.comment_source_map: Dict[str, str] = {}
        self.scan_progress: Dict[str, Any] = {
            "pass_index": 0,
            "pass_total": 0,
            "current_chat": None,
            "updated_at": None,
            "phase": "idle",
        }
        self.scan_audit_log: deque[dict[str, Any]] = deque(maxlen=200)
        self.lead_source_triggers: Dict[int, str] = {}
        self._csv_matched_keyword_ready = False

        self.schedule_enabled = False
        self.schedule_tz = "UTC"
        self.schedule_start_h = 0.0
        self.schedule_end_h = 24.0
        lim = config.get("limits", {})
        if isinstance(lim, dict):
            sched = lim.get("schedule")
            if isinstance(sched, dict) and sched.get("enabled"):
                self.schedule_enabled = True
                self.schedule_tz = str(sched.get("timezone") or "UTC").strip() or "UTC"
                ah = sched.get("active_hours")
                if isinstance(ah, (list, tuple)) and len(ah) >= 2:
                    try:
                        self.schedule_start_h = float(ah[0])
                        self.schedule_end_h = float(ah[1])
                    except (TypeError, ValueError):
                        self.schedule_start_h, self.schedule_end_h = 0.0, 24.0

    @staticmethod
    def _normalize_target_chats(chats: list[Any]) -> list[Any]:
        normalized: list[Any] = []
        for chat in chats:
            if isinstance(chat, str):
                chat_s = chat.strip()
                if re.fullmatch(r"-?\d+", chat_s):
                    normalized.append(int(chat_s))
                else:
                    normalized.append(chat_s)
            else:
                normalized.append(chat)
        return normalized

    @staticmethod
    def _now_utc() -> datetime:
        return datetime.now(timezone.utc)

    def _within_active_schedule(self) -> bool:
        if not self.schedule_enabled:
            return True
        try:
            tz = ZoneInfo(self.schedule_tz)
        except Exception:
            tz = timezone.utc
            logger.warning("Schedule: неверная зона '%s', используем UTC", self.schedule_tz)
        local = self._now_utc().astimezone(tz)
        t = local.hour + local.minute / 60.0 + local.second / 3600.0
        start, end = self.schedule_start_h, self.schedule_end_h
        if start == end or (start <= 0.0 and end >= 24.0):
            return True
        if start < end:
            return start <= t < end
        return t >= start or t < end

    def _stage_queue_delay(self, stage: int) -> timedelta:
        lim = self.config.get("limits", {})
        if isinstance(lim, dict):
            sfh = lim.get("stage_followup_hours")
            key = f"stage{stage}"
            if isinstance(sfh, dict):
                pair = sfh.get(key)
                if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                    try:
                        a = float(pair[0])
                        b = float(pair[1])
                        lo, hi = (a, b) if a <= b else (b, a)
                        if hi > 0:
                            hours = random.uniform(max(lo, 0.0), hi)
                            return timedelta(seconds=hours * 3600.0)
                    except (TypeError, ValueError):
                        pass
        if stage == 1:
            delay_min, delay_max = self.config["limits"]["stage_delays_sec"]["stage1"]
        elif stage == 2:
            delay_min, delay_max = self.config["limits"]["stage_delays_sec"]["stage2"]
        else:
            delay_min, delay_max = self.config["limits"]["stage_delays_sec"]["stage3"]
        return timedelta(seconds=random.randint(int(delay_min), int(delay_max)))

    @staticmethod
    def _normalize_text(text: str) -> str:
        return re.sub(r"\s+", " ", text.lower()).strip()

    def _contains_any(self, text: str, words: list[str]) -> bool:
        normalized = self._normalize_text(text)
        return any(word in normalized for word in words)

    def _hot_lead_match(self, text: str) -> tuple[bool, str]:
        if not (text or "").strip():
            return False, ""
        if self._contains_any(text, self.exclude_hot_keywords):
            return False, ""
        normalized = self._normalize_text(text)
        matched_trigger = ""
        for kw in self.hot_keywords:
            if kw and kw in normalized:
                matched_trigger = kw
                break
        if not matched_trigger:
            return False, ""
        if not self._contains_any(text, self.required_intent_keywords):
            return False, ""
        return True, matched_trigger

    def _set_scan_last_action(self, text: str) -> None:
        self.scan_progress["last_action"] = (text or "")[:480]
        self.scan_progress["updated_at"] = self._now_utc().isoformat()

    def _merge_scan_progress(self, **kwargs: Any) -> None:
        cur: Dict[str, Any] = dict(self.scan_progress)
        cur.update(kwargs)
        cur["updated_at"] = self._now_utc().isoformat()
        self.scan_progress = cur

    def _append_scan_activity(self, action: str, **extra: Any) -> None:
        """Короткая строка для UI (state.json): конкретное действие в реальном времени."""
        row: dict[str, Any] = {
            "kind": "activity",
            "action": str(action).strip()[:400],
            "ts": self._now_utc().isoformat(),
        }
        for k, v in extra.items():
            if v is None:
                continue
            if k in row:
                continue
            try:
                row[k] = v if isinstance(v, (str, int, float, bool)) else str(v)[:200]
            except Exception:  # noqa: BLE001
                row[k] = "?"
        self.scan_audit_log.append(row)
        self._set_scan_last_action(row["action"])
        self._persist_state()

    def _append_scan_audit(self, entry: dict[str, Any]) -> None:
        row = dict(entry)
        row["kind"] = str(row.get("kind") or "summary")
        row["ts"] = self._now_utc().isoformat()
        self.scan_audit_log.append(row)
        ch = row.get("chat")
        self._set_scan_last_action(
            f"Итог {ch}: сообщ. {row.get('messages_seen', '—')} · горячих {row.get('hot_leads', '—')} · в очередь {row.get('queued', '—')}"
        )
        self._persist_state()

    def _openai_chat_completion_sync(self, system: str, user: str) -> str:
        llm = self.config.get("llm")
        if not isinstance(llm, dict):
            raise ValueError("LLM не настроен")
        api_key = str(llm.get("api_key", "")).strip()
        if not api_key:
            raise ValueError("Пустой llm.api_key")
        base_url = str(llm.get("base_url", "") or "https://api.openai.com/v1").strip()
        model = _normalize_llm_model_id(
            base_url, str(llm.get("model", "") or "gpt-4o-mini").strip()
        )
        url = base_url.rstrip("/") + "/chat/completions"
        payload = json.dumps(
            {
                "model": model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.7,
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
            hint = _llm_http_error_hint_sync(int(exc.code), detail)
            raise ValueError(f"LLM HTTP {exc.code}: {detail[:800]}{hint}") from exc
        try:
            return str(raw["choices"][0]["message"]["content"]).strip()
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError(f"Неожиданный ответ LLM: {raw!r}") from exc

    def _llm_enabled(self) -> bool:
        llm = self.config.get("llm")
        if not isinstance(llm, dict):
            return False
        return bool(llm.get("enabled") and str(llm.get("api_key", "")).strip())

    async def _compose_stage23_message(self, stage: int, task_hint: str, lead_snippet: str) -> str:
        partner = self.partner_name or "партнёр"
        tpl = self.templates if isinstance(self.templates, dict) else {}
        cfg = self.config if isinstance(self.config, dict) else {}
        pr = effective_llm_prompts(cfg)
        system = pr["bot_stage23_system"]
        if stage == 2:
            user_msg = format_llm_prompt(
                pr["bot_stage2_user"],
                partner=partner,
                task_hint=task_hint or "не указана",
                lead_snippet=lead_snippet or "(нет текста)",
                stage2_tpl=str(tpl.get("stage2", "") or "")[:600],
            )
        elif stage == 3:
            user_msg = format_llm_prompt(
                pr["bot_stage3_user"],
                partner=partner,
                task_hint=task_hint or "—",
                lead_snippet=lead_snippet or "(нет)",
                stage3_tpl=str(tpl.get("stage3", "") or "")[:600],
            )
        else:
            return self._format_stage_message(stage, task_hint)
        try:
            return await asyncio.to_thread(self._openai_chat_completion_sync, system, user_msg)
        except Exception as exc:  # noqa: BLE001
            logger.warning("LLM stage%s compose failed: %s — шаблон", stage, exc)
            return self._format_stage_message(stage, task_hint)

    async def _final_dm_text(self, job: PendingDM) -> str:
        if job.force_template:
            return job.template_text
        if job.stage in (2, 3) and self._llm_enabled():
            return await self._compose_stage23_message(job.stage, job.task_hint, job.lead_snippet)
        return job.template_text

    def _outreach_db_ready(self) -> bool:
        return bool(self.org_id and self.data_db_path and self.data_db_path.is_file())

    # --------- CRM (conversations) helpers ---------
    # Бот должен сам поддерживать таблицу conversations: создавать строку при детекте лида
    # и дописывать историю на каждой итерации (отправка/входящий ответ/смена статуса).
    # Без этого «Воронка CRM» в UI остаётся пустой.
    def _conv_history_load(self, raw: str) -> list[dict[str, Any]]:
        try:
            h = json.loads(raw or "[]")
        except Exception:  # noqa: BLE001
            return []
        return h if isinstance(h, list) else []

    def _conv_history_save(self, hist: list[dict[str, Any]]) -> str:
        return json.dumps(hist, ensure_ascii=False)

    def _conv_get_or_create(
        self,
        *,
        user_id: int,
        username: str,
        source_chat: str,
        lead_snippet: str,
        trigger_match: str,
        status: str = "active",
        outreach_queue_id: int | None = None,
    ) -> int | None:
        """Idempotent. Возвращает id строки в conversations или None при ошибке/без БД.
        Идемпотентно по (org_id, lead_user_id): не создаёт дубль, но поднимает свежие
        lead_snippet/trigger_match/source_chat если они были пусты."""
        if not self._outreach_db_ready() or not self.org_id:
            return None
        ts = datetime.now(timezone.utc).isoformat()
        try:
            with sqlite3.connect(self.data_db_path) as conn:  # type: ignore[arg-type]
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT id, lead_snippet, trigger_match, source_chat, outreach_queue_id "
                    "FROM conversations WHERE org_id = ? AND lead_user_id = ? "
                    "ORDER BY id DESC LIMIT 1",
                    (int(self.org_id), str(user_id)),
                ).fetchone()
                if row:
                    cid = int(row["id"])
                    fields: list[str] = []
                    vals: list[Any] = []
                    if not (row["lead_snippet"] or "") and lead_snippet:
                        fields.append("lead_snippet = ?")
                        vals.append(lead_snippet)
                    if not (row["trigger_match"] or "") and trigger_match:
                        fields.append("trigger_match = ?")
                        vals.append(trigger_match)
                    if not (row["source_chat"] or "") and source_chat:
                        fields.append("source_chat = ?")
                        vals.append(source_chat)
                    if outreach_queue_id is not None and not row["outreach_queue_id"]:
                        fields.append("outreach_queue_id = ?")
                        vals.append(int(outreach_queue_id))
                    fields.append("last_activity_at = ?")
                    vals.append(ts)
                    fields.append("updated_at = ?")
                    vals.append(ts)
                    if username:
                        fields.append("lead_username = ?")
                        vals.append(username)
                    vals.append(cid)
                    conn.execute(
                        f"UPDATE conversations SET {', '.join(fields)} WHERE id = ?",
                        tuple(vals),
                    )
                    return cid
                cur = conn.execute(
                    """
                    INSERT INTO conversations (
                        org_id, lead_user_id, lead_username, source_chat, status,
                        history_json, current_stage, lead_snippet, trigger_match,
                        outreach_queue_id, created_at, updated_at, last_activity_at,
                        bot_user_id
                    ) VALUES (?, ?, ?, ?, ?, '[]', 1, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(self.org_id),
                        str(user_id),
                        username or "",
                        source_chat,
                        status,
                        lead_snippet,
                        trigger_match,
                        outreach_queue_id,
                        ts,
                        ts,
                        ts,
                        getattr(self, "_bot_self_username", "") or "",
                    ),
                )
                return int(cur.lastrowid)
        except Exception as exc:  # noqa: BLE001
            logger.warning("conversations get_or_create failed: %s", exc)
            return None

    def _conv_link_to_outreach(self, conv_id: int, outreach_queue_id: int) -> None:
        """Двусторонняя связь outreach_queue ↔ conversations.

        Нужна, чтобы при approve строки оффера UI мог открыть карточку CRM
        и наоборот — из CRM найти исходную запись на согласовании."""
        if not self._outreach_db_ready() or not conv_id or not outreach_queue_id:
            return
        try:
            with sqlite3.connect(self.data_db_path) as conn:  # type: ignore[arg-type]
                conn.execute(
                    "UPDATE outreach_queue SET conversation_id = ? WHERE id = ? AND org_id = ?",
                    (int(conv_id), int(outreach_queue_id), int(self.org_id)),
                )
                conn.execute(
                    "UPDATE conversations SET outreach_queue_id = ?, updated_at = ?, last_activity_at = ? "
                    "WHERE id = ? AND org_id = ?",
                    (
                        int(outreach_queue_id),
                        datetime.now(timezone.utc).isoformat(),
                        datetime.now(timezone.utc).isoformat(),
                        int(conv_id),
                        int(self.org_id),
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("conversations link to outreach failed: %s", exc)

    def _conv_append_event(
        self,
        conv_id: int,
        *,
        role: str,
        text: str,
        source: str = "",
        stage: int | None = None,
        status: str | None = None,
        new_status: str | None = None,
        new_stage: int | None = None,
    ) -> None:
        """Добавляет запись в history_json и (опционально) обновляет status / current_stage.

        role: 'assistant' | 'user' | 'note' | 'system'.
        source: 'bot_dm' | 'private_reply' | 'detection' | 'reject' | 'system'."""
        if not self._outreach_db_ready() or not conv_id:
            return
        ts = datetime.now(timezone.utc).isoformat()
        entry: dict[str, Any] = {
            "role": role,
            "text": (text or "")[:2000],
            "at": ts,
        }
        if source:
            entry["source"] = source
        if stage is not None:
            entry["stage"] = int(stage)
        if status:
            entry["status"] = status
        try:
            with sqlite3.connect(self.data_db_path) as conn:  # type: ignore[arg-type]
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT history_json FROM conversations WHERE id = ? AND org_id = ?",
                    (int(conv_id), int(self.org_id)),
                ).fetchone()
                if not row:
                    return
                hist = self._conv_history_load(str(row["history_json"] or "[]"))
                hist.append(entry)
                # Ограничиваем размер истории, чтобы строка не разрослась.
                if len(hist) > 500:
                    hist = hist[-500:]
                fields = ["history_json = ?", "updated_at = ?", "last_activity_at = ?"]
                vals: list[Any] = [self._conv_history_save(hist), ts, ts]
                if new_status:
                    fields.append("status = ?")
                    vals.append(new_status)
                if new_stage is not None:
                    fields.append("current_stage = ?")
                    vals.append(int(new_stage))
                vals.extend([int(conv_id), int(self.org_id)])
                conn.execute(
                    f"UPDATE conversations SET {', '.join(fields)} WHERE id = ? AND org_id = ?",
                    tuple(vals),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("conversations append event failed: %s", exc)

    def _conv_lookup_id(self, user_id: int) -> int | None:
        """Возвращает id conversation по user_id (последняя активная)."""
        if not self._outreach_db_ready():
            return None
        try:
            with sqlite3.connect(self.data_db_path) as conn:  # type: ignore[arg-type]
                row = conn.execute(
                    "SELECT id FROM conversations WHERE org_id = ? AND lead_user_id = ? "
                    "ORDER BY id DESC LIMIT 1",
                    (int(self.org_id), str(user_id)),
                ).fetchone()
                return int(row[0]) if row else None
        except Exception as exc:  # noqa: BLE001
            logger.warning("conversations lookup failed: %s", exc)
            return None

    def _heartbeat_db(self, phase: str | None = None) -> None:
        """Бот периодически обновляет bot_runs.last_heartbeat — Flask использует это,
        чтобы UI видел «не отвечает» если процесс молчит больше 5 мин."""
        if not self._outreach_db_ready() or not self.org_id:
            return
        ts = datetime.now(timezone.utc).isoformat()
        try:
            with sqlite3.connect(self.data_db_path) as conn:  # type: ignore[arg-type]
                if phase:
                    conn.execute(
                        "UPDATE bot_runs SET last_heartbeat = ?, phase = ? WHERE org_id = ?",
                        (ts, phase, int(self.org_id)),
                    )
                else:
                    conn.execute(
                        "UPDATE bot_runs SET last_heartbeat = ? WHERE org_id = ?",
                        (ts, int(self.org_id)),
                    )
        except Exception as exc:  # noqa: BLE001
            logger.warning("bot_runs heartbeat failed: %s", exc)

    def _stage_needs_human_approval(self, stage: int) -> bool:
        """Мастер-флаг human_approval_for_dm + опционально выключение по этапам (stage1/2/3)."""
        if not self.human_approval_for_dm:
            return False
        key = f"stage{int(stage)}"
        if key in self.human_approval_stages:
            return bool(self.human_approval_stages[key])
        return True

    def _outreach_insert_pending(
        self,
        *,
        user_id: int,
        username: str,
        stage: int,
        source_chat: str,
        draft_text: str,
        task_hint: str = "",
        lead_snippet: str = "",
        trigger_match: str = "",
        conversation_id: int | None = None,
    ) -> int | None:
        """Возвращает id вставленной строки или None — нужен для связки с conversations."""
        if not self._stage_needs_human_approval(stage) or not self._outreach_db_ready():
            return None
        ts = datetime.now(timezone.utc).isoformat()
        try:
            with sqlite3.connect(self.data_db_path) as conn:  # type: ignore[arg-type]
                cur = conn.execute(
                    """
                    INSERT INTO outreach_queue (
                        org_id, user_id, username, source_chat, stage, draft_text,
                        task_hint, lead_snippet, trigger_match, status, created_at,
                        conversation_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                    """,
                    (
                        int(self.org_id),
                        user_id,
                        username or "",
                        source_chat,
                        stage,
                        draft_text,
                        task_hint or "",
                        lead_snippet or "",
                        trigger_match or "",
                        ts,
                        int(conversation_id) if conversation_id else None,
                    ),
                )
                return int(cur.lastrowid)
        except Exception as exc:  # noqa: BLE001
            logger.warning("outreach INSERT failed: %s", exc)
            return None

    async def _outreach_pull_approved_into_queue(self) -> None:
        if not self.human_approval_for_dm or not self._outreach_db_ready():
            return
        try:
            with sqlite3.connect(self.data_db_path) as conn:  # type: ignore[arg-type]
                conn.row_factory = sqlite3.Row
                rows = list(
                    conn.execute(
                        "SELECT id, user_id, username, source_chat, stage, draft_text, task_hint, lead_snippet, trigger_match, conversation_id "
                        "FROM outreach_queue WHERE org_id = ? AND status = 'approved' ORDER BY id ASC LIMIT 15",
                        (int(self.org_id),),
                    )
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("outreach SELECT approved failed: %s", exc)
            return
        if not rows:
            return
        now = self._now_utc()
        for r in rows:
            try:
                uid = int(r["user_id"])
            except (TypeError, ValueError):
                continue
            oid = int(r["id"])
            try:
                with sqlite3.connect(self.data_db_path) as conn2:  # type: ignore[arg-type]
                    conn2.execute(
                        "UPDATE outreach_queue SET status = 'queued', updated_at = ? WHERE id = ? AND org_id = ?",
                        (now.isoformat(), oid, int(self.org_id)),
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning("outreach mark queued failed id=%s: %s", oid, exc)
                continue
            due_at = now + self._stage_queue_delay(int(r["stage"]))
            self.pending_dms.append(
                PendingDM(
                    user_id=uid,
                    username=str(r["username"] or ""),
                    stage=int(r["stage"]),
                    source_chat=str(r["source_chat"]),
                    due_at=due_at,
                    template_text=str(r["draft_text"] or ""),
                    task_hint=str(r["task_hint"] or ""),
                    lead_snippet=str(r["lead_snippet"] or ""),
                    trigger_match=str(r["trigger_match"] or ""),
                    force_template=True,
                    outreach_db_id=oid,
                )
            )
            # CRM: помечаем conversation как «approved → готовится к отправке».
            cid = r["conversation_id"]
            if cid:
                self._conv_append_event(
                    int(cid),
                    role="system",
                    source="approved",
                    stage=int(r["stage"]),
                    text=f"Оператор одобрил черновик stage{int(r['stage'])}. Сообщение поставлено в очередь отправки.",
                    new_status="active",
                )
            logger.info("Outreach id=%s added to send queue (user=%s stage=%s)", oid, uid, r["stage"])

    def _outreach_mark_sent(self, row_id: int) -> None:
        if not self._outreach_db_ready() or not self.org_id:
            return
        try:
            with sqlite3.connect(self.data_db_path) as conn:  # type: ignore[arg-type]
                conn.execute(
                    "UPDATE outreach_queue SET status = 'sent', updated_at = ? WHERE id = ? AND org_id = ?",
                    (datetime.now(timezone.utc).isoformat(), row_id, int(self.org_id)),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("outreach mark sent failed: %s", exc)

    def _is_negative(self, text: str) -> bool:
        return self._contains_any(text, self.negative_keywords)

    def _is_qualified(self, text: str) -> bool:
        return self._contains_any(text, self.qualification_keywords)

    def _is_interested(self, text: str) -> bool:
        return self._contains_any(text, self.interested_keywords)

    def _hour_limit_ok(self, chat_key: str) -> bool:
        now = self._now_utc()
        q = self.hourly_chat_sends[chat_key]
        one_hour_ago = now - timedelta(hours=1)
        while q and q[0] < one_hour_ago:
            q.popleft()
        return len(q) < self.max_dm_per_hour_per_chat

    def _register_chat_send(self, chat_key: str) -> None:
        self.hourly_chat_sends[chat_key].append(self._now_utc())

    def _roll_daily_limit_if_needed(self) -> None:
        today = date.today()
        if today != self.current_day:
            self.current_day = today
            self.daily_sent_count = 0
            self.daily_limit = random.randint(self.daily_limit_min, self.daily_limit_max)
            logger.info("New day started. Daily DM limit: %s", self.daily_limit)

    def _roll_monthly_limit_if_needed(self) -> None:
        now = date.today()
        month = f"{now.year:04d}-{now.month:02d}"
        if month != self.current_month:
            self.current_month = month
            self.monthly_sent_count = 0
            logger.info("New month started. Monthly DM count reset.")

    async def _safe_send_message(self, user_id: int, text: str) -> tuple[bool, str]:
        await asyncio.sleep(random.uniform(self.typing_delay_min, self.typing_delay_max))
        try:
            await self.client.send_message(user_id, text)
            return True, "sent"
        except FloodWaitError as exc:
            wait_for = int(exc.seconds) + random.randint(1, 3)
            logger.warning("FloodWait detected. Sleep %ss", wait_for)
            await asyncio.sleep(wait_for)
            return False, f"floodwait_{wait_for}s"
        except UserPrivacyRestrictedError:
            return False, "privacy_restricted"
        except Exception as exc:  # noqa: BLE001
            return False, f"error_{type(exc).__name__}"

    async def _has_user_reply_after(self, user_id: int, after: datetime | None) -> bool:
        try:
            async for msg in self.client.iter_messages(user_id, limit=20):
                if msg.out:
                    continue
                msg_dt = msg.date
                if msg_dt is None:
                    continue
                if msg_dt.tzinfo is None:
                    msg_dt = msg_dt.replace(tzinfo=timezone.utc)
                if after is None or msg_dt > after:
                    return True
            return False
        except Exception as exc:  # noqa: BLE001
            logger.warning("Reply-check failed for user=%s: %s", user_id, exc)
            return False

    def _migrate_csv_add_matched_keyword(self) -> None:
        if not CSV_PATH.exists():
            return
        with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
            first = f.readline()
        if not first or "matched_keyword" in first:
            return
        with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        tmp = CSV_PATH.with_suffix(".csv.__tmp__")
        try:
            with tmp.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
                writer.writeheader()
                for r in rows:
                    out = {fn: str(r.get(fn, "") or "").replace("\n", " ").strip() for fn in CSV_FIELDNAMES}
                    writer.writerow(out)
            tmp.replace(CSV_PATH)
            logger.info("CSV migrated: added column matched_keyword")
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            raise

    def _migrate_csv_add_deleted(self) -> None:
        if not CSV_PATH.exists():
            return
        with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
            first = f.readline()
        if not first or "deleted" in first:
            return
        with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        tmp = CSV_PATH.with_suffix(".csv.__tmp_del__")
        try:
            with tmp.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
                writer.writeheader()
                for r in rows:
                    out = {fn: str(r.get(fn, "") or "").replace("\n", " ").strip() for fn in CSV_FIELDNAMES}
                    writer.writerow(out)
            tmp.replace(CSV_PATH)
            logger.info("CSV migrated: added column deleted")
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            raise

    def _migrate_csv_add_message_id(self) -> None:
        if not CSV_PATH.exists():
            return
        with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
            first = f.readline()
        if not first or "message_id" in first:
            return
        with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        tmp = CSV_PATH.with_suffix(".csv.__tmp_msgid__")
        try:
            with tmp.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
                writer.writeheader()
                for r in rows:
                    out = {fn: str(r.get(fn, "") or "").replace("\n", " ").strip() for fn in CSV_FIELDNAMES}
                    writer.writerow(out)
            tmp.replace(CSV_PATH)
            logger.info("CSV migrated: added column message_id")
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            raise

    def _migrate_csv_add_lead_tag(self) -> None:
        if not CSV_PATH.exists():
            return
        with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
            first = f.readline()
        if not first or "lead_tag" in first:
            return
        with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        tmp = CSV_PATH.with_suffix(".csv.__tmp_tag__")
        try:
            with tmp.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
                writer.writeheader()
                for r in rows:
                    out = {fn: str(r.get(fn, "") or "").replace("\n", " ").strip() for fn in CSV_FIELDNAMES}
                    writer.writerow(out)
            tmp.replace(CSV_PATH)
            logger.info("CSV migrated: added column lead_tag")
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            raise

    def _log_csv(
        self,
        username: str,
        user_id: int,
        source_chat: str,
        message: str,
        stage: str,
        status: str,
        matched_keyword: str = "",
        *,
        message_id: int | str = "",
        source_message_time: datetime | None = None,
        lead_tag: str = "",
    ) -> None:
        if not self._csv_matched_keyword_ready:
            self._migrate_csv_add_matched_keyword()
            self._csv_matched_keyword_ready = True
        self._migrate_csv_add_deleted()
        self._migrate_csv_add_message_id()
        self._migrate_csv_add_lead_tag()
        mid_s = ""
        if message_id != "" and message_id is not None:
            try:
                mid_s = str(int(message_id))
            except (TypeError, ValueError):
                mid_s = str(message_id).strip()
        row = {
            "timestamp": _dt_to_utc_iso(source_message_time),
            "username": username or "",
            "user_id": user_id,
            "source_chat": source_chat,
            "message_id": mid_s,
            "message": message.replace("\n", " ").strip(),
            "stage": stage,
            "status": status,
            "matched_keyword": (matched_keyword or "").replace("\n", " ").strip(),
            "lead_tag": (lead_tag or "").strip(),
            "deleted": "",
        }
        file_exists = CSV_PATH.exists()
        with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

    def _persist_state(self) -> None:
        state = {
            "contacted_users": sorted(self.contacted_users),
            "blacklist_users": sorted(self.blacklist_users),
            "last_seen_msg_id": self.last_seen_msg_id,
            "private_stage": self.private_stage,
            "private_task_hint": self.private_task_hint,
            "last_private_reply_at": self.last_private_reply_at,
            "last_stage_sent_at": self.last_stage_sent_at,
            "daily_sent_count": self.daily_sent_count,
            "current_day": self.current_day.isoformat(),
            "daily_limit": self.daily_limit,
            "monthly_sent_count": self.monthly_sent_count,
            "current_month": self.current_month,
            "hourly_chat_sends": {
                key: [ts.isoformat() for ts in queue] for key, queue in self.hourly_chat_sends.items()
            },
            "scan_progress": self.scan_progress,
            "scan_audit_log": list(self.scan_audit_log),
            "lead_source_triggers": {str(k): v for k, v in self.lead_source_triggers.items()},
        }
        STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_state(self) -> None:
        if STATE_PATH.exists():
            raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            self.contacted_users = {int(x) for x in raw.get("contacted_users", [])}
            self.blacklist_users = {int(x) for x in raw.get("blacklist_users", [])}
            self.last_seen_msg_id = {
                str(k): int(v) for k, v in raw.get("last_seen_msg_id", {}).items()
            }
            self.private_stage = {int(k): int(v) for k, v in raw.get("private_stage", {}).items()}
            self.private_task_hint = {
                int(k): str(v) for k, v in raw.get("private_task_hint", {}).items()
            }
            self.last_private_reply_at = {
                int(k): str(v) for k, v in raw.get("last_private_reply_at", {}).items()
            }
            self.last_stage_sent_at = {
                int(k): {str(sk): str(sv) for sk, sv in stage_map.items()}
                for k, stage_map in raw.get("last_stage_sent_at", {}).items()
                if isinstance(stage_map, dict)
            }
            self.daily_sent_count = int(raw.get("daily_sent_count", 0))
            self.daily_limit = int(raw.get("daily_limit", self.daily_limit))
            self.monthly_sent_count = int(raw.get("monthly_sent_count", 0))
            self.current_month = str(raw.get("current_month", self.current_month))
            saved_day = raw.get("current_day")
            if saved_day:
                self.current_day = date.fromisoformat(saved_day)

            queues = raw.get("hourly_chat_sends", {})
            for key, timestamps in queues.items():
                q = deque()
                for ts in timestamps:
                    try:
                        q.append(datetime.fromisoformat(ts))
                    except ValueError:
                        continue
                self.hourly_chat_sends[key] = q

            sp = raw.get("scan_progress")
            if isinstance(sp, dict):
                try:
                    self.scan_progress = {
                        "pass_index": int(sp.get("pass_index", 0)),
                        "pass_total": int(sp.get("pass_total", 0)),
                        "current_chat": sp.get("current_chat"),
                        "updated_at": sp.get("updated_at"),
                        "phase": str(sp.get("phase") or "idle"),
                        "idle_reason": sp.get("idle_reason"),
                        "last_action": sp.get("last_action"),
                    }
                except (TypeError, ValueError):
                    pass

            sal = raw.get("scan_audit_log")
            if isinstance(sal, list):
                self.scan_audit_log.clear()
                for item in sal[-200:]:
                    if isinstance(item, dict):
                        self.scan_audit_log.append(dict(item))

            lst = raw.get("lead_source_triggers")
            if isinstance(lst, dict):
                self.lead_source_triggers = {}
                for k, v in lst.items():
                    try:
                        self.lead_source_triggers[int(k)] = str(v)
                    except (TypeError, ValueError):
                        continue

        if CSV_PATH.exists():
            with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    user_id_raw = row.get("user_id")
                    if user_id_raw and user_id_raw.isdigit():
                        self.contacted_users.add(int(user_id_raw))

    async def _can_contact_user(self, user: User) -> tuple[bool, str]:
        if user.bot:
            return False, "user_is_bot"
        if user.id in self.blacklist_users:
            return False, "blacklisted_user"
        if user.id in self.contacted_users:
            return False, "already_contacted"

        try:
            full = await self.client(functions.users.GetFullUserRequest(id=user.id))
            about = getattr(full.full_user, "about", "") or ""
        except Exception:  # noqa: BLE001
            about = ""

        if about and self._contains_any(about, self.bio_block_keywords):
            return False, "bio_forbidden"
        return True, "ok"

    def _format_stage_message(self, stage: int, task_hint: str = "") -> str:
        if stage == 1:
            return self.templates["stage1"]
        if stage == 2:
            safe_hint = task_hint if task_hint else "ваш проект"
            return self.templates["stage2"].replace("[их задача]", safe_hint)
        if stage == 3:
            return self.templates["stage3"].replace("[Имя партнёра]", self.partner_name)
        raise ValueError(f"Unsupported stage: {stage}")

    def _queue_dm(
        self,
        user_id: int,
        username: str,
        stage: int,
        source_chat: str,
        task_hint: str = "",
        *,
        lead_snippet: str = "",
        trigger_match: str = "",
    ) -> None:
        due_at = self._now_utc() + self._stage_queue_delay(stage)
        text = self._format_stage_message(stage, task_hint)
        self.pending_dms.append(
            PendingDM(
                user_id=user_id,
                username=username or "",
                stage=stage,
                source_chat=source_chat,
                due_at=due_at,
                template_text=text,
                task_hint=task_hint,
                lead_snippet=lead_snippet,
                trigger_match=trigger_match,
            )
        )

    async def _monitor_groups_loop(self) -> None:
        logger.info("Monitoring chats every %ss: %s", self.monitor_interval, self.scan_chats)

        while True:
            self._roll_daily_limit_if_needed()
            if not self._within_active_schedule():
                logger.info(
                    "Расписание: вне окна (%s, часы %.1f–%.1f локально) — пауза 60 с",
                    self.schedule_tz,
                    self.schedule_start_h,
                    self.schedule_end_h,
                )
                self._merge_scan_progress(
                    pass_index=0,
                    pass_total=int(self.scan_progress.get("pass_total", 0) or 0),
                    current_chat=None,
                    phase="schedule_paused",
                    idle_reason="outside_active_hours",
                )
                self._append_scan_activity(
                    "Пауза: вне окна расписания "
                    f"({self.schedule_tz}, {self.schedule_start_h:.1f}–{self.schedule_end_h:.1f} ч локально), жду 60 с",
                )
                await asyncio.sleep(60)
                continue

            eligible: list[Any] = []
            for chat in list(self.scan_chats):
                chat_key = str(chat)
                if chat_key in self.invalid_chats:
                    continue
                if not self._hour_limit_ok(chat_key):
                    continue
                eligible.append(chat)

            total = len(eligible)
            self._merge_scan_progress(
                pass_index=0,
                pass_total=total,
                current_chat=None,
                phase="scanning",
                idle_reason=None,
            )
            if total == 0:
                self._append_scan_activity(
                    "Проход пропущен: нет чатов (все в invalid, или лимит ЛС/час по каждому чату)",
                )
            else:
                self._append_scan_activity(f"Начало прохода: {total} чат(ов) в очереди")

            for idx, chat in enumerate(eligible, start=1):
                chat_key = str(chat)
                self._merge_scan_progress(
                    pass_index=idx,
                    pass_total=total,
                    current_chat=chat_key,
                    phase="scanning",
                )
                source_chat_label = (
                    f"{chat_key}|comments_for={self.comment_source_map[chat_key]}"
                    if chat_key in self.comment_source_map
                    else chat_key
                )
                seen_users_in_cycle: Set[int] = set()

                last_id = self.last_seen_msg_id.get(chat_key, 0)
                self._append_scan_activity(
                    f"Чат {idx}/{total}: читаю сообщения после id={last_id} · {source_chat_label}",
                    chat=chat_key,
                )
                logger.info(
                    "CHAT_WINDOW chat=%s | reading messages with msg_id > %s",
                    chat_key,
                    last_id,
                )
                new_messages = []
                try:
                    async for msg in self.client.iter_messages(
                        chat, min_id=last_id, limit=self.fetch_limit_per_chat
                    ):
                        new_messages.append(msg)
                except ValueError as exc:
                    self.invalid_chats.add(chat_key)
                    logger.error(
                        "Chat '%s' is invalid/unavailable and will be skipped. Error: %s",
                        chat_key,
                        exc,
                    )
                    self._append_scan_activity(f"Чат недоступен, исключаю из обхода: {chat_key} ({exc})", chat=chat_key)
                    continue
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "Failed to read messages from chat '%s'. Will retry next cycle. Error: %s",
                        chat_key,
                        exc,
                    )
                    self._append_scan_activity(f"Ошибка чтения чата {chat_key}, повтор в следующем цикле: {exc}", chat=chat_key)
                    continue
                new_messages.reverse()
                if new_messages:
                    logger.info(
                        "CHAT_WINDOW chat=%s | processed_range=%s..%s | fetched=%s",
                        chat_key,
                        last_id + 1,
                        new_messages[-1].id,
                        len(new_messages),
                    )
                    self._append_scan_activity(
                        f"Загружено {len(new_messages)} сообщ. (id {last_id + 1}…{new_messages[-1].id}), разбор триггеров…",
                        chat=chat_key,
                    )
                else:
                    logger.info("CHAT_WINDOW chat=%s | no new messages after msg_id=%s", chat_key, last_id)
                    self._append_scan_activity(f"Нет новых сообщений (после id={last_id})", chat=chat_key)

                msgs_seen = len(new_messages)
                hot_n = 0
                dry_n = 0
                queued_n = 0
                for msg in new_messages:
                    self.last_seen_msg_id[chat_key] = max(self.last_seen_msg_id.get(chat_key, 0), msg.id)
                    text = msg.raw_text or ""
                    is_hot, trigger_kw = self._hot_lead_match(text)
                    if not is_hot:
                        continue
                    hot_n += 1
                    sender = await msg.get_sender()
                    if not isinstance(sender, User):
                        continue
                    if sender.id in seen_users_in_cycle:
                        continue
                    can_contact, reason = await self._can_contact_user(sender)
                    if not can_contact:
                        self._log_csv(
                            sender.username or "",
                            sender.id,
                            source_chat_label,
                            text,
                            "stage1",
                            f"skip_{reason}",
                            matched_keyword=trigger_kw,
                            message_id=msg.id,
                            source_message_time=msg.date,
                        )
                        seen_users_in_cycle.add(sender.id)
                        continue
                    if self.daily_sent_count >= self.daily_limit:
                        self._log_csv(
                            sender.username or "",
                            sender.id,
                            source_chat_label,
                            text,
                            "stage1",
                            "skip_daily_limit_reached",
                            matched_keyword=trigger_kw,
                            message_id=msg.id,
                            source_message_time=msg.date,
                        )
                        seen_users_in_cycle.add(sender.id)
                        continue

                    snippet = text.strip()[:2000]
                    if self.dry_run:
                        dry_n += 1
                        self.contacted_users.add(sender.id)
                        self._log_csv(
                            sender.username or "",
                            sender.id,
                            source_chat_label,
                            text,
                            "stage1",
                            "dry_run_detected_no_dm",
                            matched_keyword=trigger_kw,
                            message_id=msg.id,
                            source_message_time=msg.date,
                        )
                        # CRM: фиксируем детект даже в dry-run, чтобы воронка имела запись.
                        conv_id = self._conv_get_or_create(
                            user_id=sender.id,
                            username=sender.username or "",
                            source_chat=chat_key,
                            lead_snippet=snippet,
                            trigger_match=trigger_kw,
                            status="active",
                        )
                        if conv_id:
                            self._conv_append_event(
                                conv_id,
                                role="system",
                                source="detection",
                                stage=1,
                                text=f"Детект лида в чате {chat_key} (dry-run, без отправки): «{snippet[:200]}»",
                            )
                        logger.info("DRY-RUN lead detected user=%s chat=%s", sender.id, chat_key)
                        seen_users_in_cycle.add(sender.id)
                        continue

                    self.lead_source_triggers[sender.id] = trigger_kw
                    draft1 = self._format_stage_message(1, "")
                    if self._stage_needs_human_approval(1):
                        if self._outreach_db_ready():
                            # CRM: создаём запись со статусом waiting_approval и связкой.
                            conv_id = self._conv_get_or_create(
                                user_id=sender.id,
                                username=sender.username or "",
                                source_chat=chat_key,
                                lead_snippet=snippet,
                                trigger_match=trigger_kw,
                                status="waiting_approval",
                            )
                            oq_id = self._outreach_insert_pending(
                                user_id=sender.id,
                                username=sender.username or "",
                                stage=1,
                                source_chat=chat_key,
                                draft_text=draft1,
                                lead_snippet=snippet,
                                trigger_match=trigger_kw,
                                conversation_id=conv_id,
                            )
                            if conv_id and oq_id:
                                self._conv_link_to_outreach(conv_id, oq_id)
                                self._conv_append_event(
                                    conv_id,
                                    role="system",
                                    source="detection",
                                    stage=1,
                                    text=f"Детект лида в чате {chat_key}: «{snippet[:200]}». Черновик stage1 ждёт согласования.",
                                    new_status="waiting_approval",
                                )
                            self._log_csv(
                                sender.username or "",
                                sender.id,
                                source_chat_label,
                                text,
                                "stage1",
                                "pending_approval",
                                matched_keyword=trigger_kw,
                                message_id=msg.id,
                                source_message_time=msg.date,
                            )
                            logger.info("Stage1 pending approval user=%s chat=%s", sender.id, chat_key)
                        else:
                            logger.warning(
                                "Согласование stage1 включено, но нет --org-id/--data-db — уходит в автоочередь"
                            )
                            self._queue_dm(
                                user_id=sender.id,
                                username=sender.username or "",
                                stage=1,
                                source_chat=chat_key,
                                lead_snippet=snippet,
                                trigger_match=trigger_kw,
                            )
                            self._log_csv(
                                sender.username or "",
                                sender.id,
                                source_chat_label,
                                text,
                                "stage1",
                                "queued",
                                matched_keyword=trigger_kw,
                                message_id=msg.id,
                                source_message_time=msg.date,
                            )
                            queued_n += 1
                    else:
                        # CRM: создаём active conversation сразу.
                        conv_id = self._conv_get_or_create(
                            user_id=sender.id,
                            username=sender.username or "",
                            source_chat=chat_key,
                            lead_snippet=snippet,
                            trigger_match=trigger_kw,
                            status="active",
                        )
                        if conv_id:
                            self._conv_append_event(
                                conv_id,
                                role="system",
                                source="detection",
                                stage=1,
                                text=f"Детект лида в чате {chat_key}: «{snippet[:200]}». stage1 поставлен в очередь автоотправки.",
                                new_status="active",
                            )
                        self._queue_dm(
                            user_id=sender.id,
                            username=sender.username or "",
                            stage=1,
                            source_chat=chat_key,
                            lead_snippet=snippet,
                            trigger_match=trigger_kw,
                        )
                        self._log_csv(
                            sender.username or "",
                            sender.id,
                            source_chat_label,
                            text,
                            "stage1",
                            "queued",
                            matched_keyword=trigger_kw,
                            message_id=msg.id,
                            source_message_time=msg.date,
                        )
                        queued_n += 1
                        logger.info("Queued stage1 DM to user=%s from chat=%s", sender.id, chat_key)
                    self.contacted_users.add(sender.id)
                    seen_users_in_cycle.add(sender.id)
                self._append_scan_audit(
                    {
                        "chat": chat_key,
                        "messages_seen": msgs_seen,
                        "hot_leads": hot_n,
                        "dry_run": dry_n,
                        "queued": queued_n,
                    }
                )
                await asyncio.sleep(random.uniform(self.per_chat_scan_delay_min, self.per_chat_scan_delay_max))
            self._merge_scan_progress(
                pass_index=0,
                pass_total=total,
                current_chat=None,
                phase="idle",
                idle_reason="between_passes",
            )
            self._append_scan_activity(
                f"Цикл обхода завершён. Следующий старт через ~{self.monitor_interval} с",
            )
            self.monitor_passes_done += 1
            if self.max_monitor_passes > 0 and self.monitor_passes_done >= self.max_monitor_passes:
                self._merge_scan_progress(
                    pass_index=0,
                    pass_total=total,
                    current_chat=None,
                    phase="idle",
                    idle_reason="max_monitor_passes",
                )
                self._append_scan_activity(
                    f"Мониторинг чатов остановлен: выполнено {self.max_monitor_passes} полн. проход(ов). "
                    "Очередь ЛС и приём ответов в ЛС продолжают работать. Перезапустите бота для нового отсчёта."
                )
                logger.info(
                    "Monitor chat loop stopped after %s passes (configured max_monitor_passes=%s)",
                    self.monitor_passes_done,
                    self.max_monitor_passes,
                )
                return
            await asyncio.sleep(self.monitor_interval)

    async def _dm_sender_loop(self) -> None:
        while True:
            self._roll_daily_limit_if_needed()
            self._roll_monthly_limit_if_needed()
            await self._outreach_pull_approved_into_queue()
            now = self._now_utc()
            ready = [job for job in self.pending_dms if job.due_at <= now]
            self.pending_dms = [job for job in self.pending_dms if job.due_at > now]

            if ready and not self._within_active_schedule():
                for job in ready:
                    job.due_at = self._now_utc() + timedelta(seconds=random.randint(120, 400))
                    self.pending_dms.append(job)
                await asyncio.sleep(5)
                continue

            for job in ready:
                if self.monthly_sent_count >= self.max_dm_month:
                    self._log_csv(
                        job.username,
                        job.user_id,
                        job.source_chat,
                        job.template_text,
                        f"stage{job.stage}",
                        "skip_monthly_limit_reached",
                        matched_keyword=job.trigger_match,
                    )
                    continue
                if self.daily_sent_count >= self.daily_limit:
                    self._log_csv(
                        job.username,
                        job.user_id,
                        job.source_chat,
                        job.template_text,
                        f"stage{job.stage}",
                        "skip_daily_limit_reached",
                        matched_keyword=job.trigger_match,
                    )
                    continue
                if not self._hour_limit_ok(job.source_chat):
                    job.due_at = self._now_utc() + timedelta(seconds=random.randint(60, 180))
                    self.pending_dms.append(job)
                    continue

                reply_gate_ok = True
                reply_gate_status = ""
                if job.stage == 1:
                    # If user already replied in private before first outreach, do not auto-send cold DM.
                    has_any_reply = await self._has_user_reply_after(job.user_id, None)
                    if has_any_reply:
                        reply_gate_ok = False
                        reply_gate_status = "skip_stage1_user_already_replied"
                elif job.stage in (2, 3):
                    prev_stage = f"stage{job.stage - 1}"
                    stage_ts_raw = self.last_stage_sent_at.get(job.user_id, {}).get(prev_stage)
                    if not stage_ts_raw:
                        reply_gate_ok = False
                        reply_gate_status = f"skip_stage{job.stage}_missing_prev_stage_timestamp"
                    else:
                        stage_ts = datetime.fromisoformat(stage_ts_raw)
                        if stage_ts.tzinfo is None:
                            stage_ts = stage_ts.replace(tzinfo=timezone.utc)
                        has_reply_after_prev = await self._has_user_reply_after(job.user_id, stage_ts)
                        if not has_reply_after_prev:
                            reply_gate_ok = False
                            reply_gate_status = f"skip_stage{job.stage}_no_reply_after_{prev_stage}"

                if not reply_gate_ok:
                    self._log_csv(
                        job.username,
                        job.user_id,
                        job.source_chat,
                        job.template_text,
                        f"stage{job.stage}",
                        reply_gate_status,
                        matched_keyword=job.trigger_match,
                    )
                    logger.info("Reply-gate blocked user=%s stage=%s status=%s", job.user_id, job.stage, reply_gate_status)
                    self._append_scan_activity(
                        f"ЛС пропущено: user={job.user_id} stage{job.stage} — {reply_gate_status}",
                        user_id=job.user_id,
                    )
                    self._persist_state()
                    continue

                text_to_send = await self._final_dm_text(job)
                ok, status = await self._safe_send_message(job.user_id, text_to_send)
                self._log_csv(
                    job.username,
                    job.user_id,
                    job.source_chat,
                    text_to_send,
                    f"stage{job.stage}",
                    status,
                    matched_keyword=job.trigger_match,
                )
                if ok:
                    self._register_chat_send(job.source_chat)
                    self.daily_sent_count += 1
                    self.monthly_sent_count += 1
                    self.private_stage[job.user_id] = job.stage
                    user_stage_map = self.last_stage_sent_at.setdefault(job.user_id, {})
                    user_stage_map[f"stage{job.stage}"] = self._now_utc().isoformat()
                    if job.task_hint:
                        self.private_task_hint[job.user_id] = job.task_hint
                    if job.outreach_db_id:
                        self._outreach_mark_sent(job.outreach_db_id)
                    # CRM: фиксируем отправку stage в истории conversation.
                    conv_id = self._conv_lookup_id(job.user_id)
                    if not conv_id:
                        conv_id = self._conv_get_or_create(
                            user_id=job.user_id,
                            username=job.username,
                            source_chat=job.source_chat,
                            lead_snippet=job.lead_snippet,
                            trigger_match=job.trigger_match,
                            status="active",
                        )
                    if conv_id:
                        self._conv_append_event(
                            conv_id,
                            role="assistant",
                            source="bot_dm",
                            stage=job.stage,
                            text=text_to_send,
                            new_status="active",
                            new_stage=int(job.stage),
                        )
                    logger.info("Stage%s DM sent to user=%s", job.stage, job.user_id)
                    un = f"@{job.username}" if (job.username or "").strip() else "—"
                    self._append_scan_activity(
                        f"Отправлено ЛС stage{job.stage} → {un} (id {job.user_id})",
                        user_id=job.user_id,
                    )
                else:
                    if status.startswith("privacy_restricted"):
                        self.blacklist_users.add(job.user_id)
                        logger.warning("Privacy restricted for user=%s", job.user_id)
                    self._append_scan_activity(
                        f"ЛС не доставлено stage{job.stage} user={job.user_id}: {status}",
                        user_id=job.user_id,
                    )

                self._persist_state()
                await asyncio.sleep(random.uniform(1, 3))
            await asyncio.sleep(2)

    async def _process_private_reply(
        self, user: User, text: str, *, source_message_time: datetime | None = None
    ) -> None:
        user_id = user.id
        username = user.username or ""
        normalized = self._normalize_text(text)
        self.last_private_reply_at[user_id] = self._now_utc().isoformat()

        # CRM: любой входящий ответ — событие в истории conversation.
        conv_id = self._conv_lookup_id(user_id)
        if conv_id:
            self._conv_append_event(
                conv_id,
                role="user",
                source="private_reply",
                stage=self.private_stage.get(user_id, 0) or None,
                text=text,
            )

        if self.dry_run:
            self._log_csv(
                username, user_id, "private", text, "dry_run", "dry_run_ignore_private", "", source_message_time=source_message_time
            )
            return

        if self._is_negative(normalized):
            self.blacklist_users.add(user_id)
            self.lead_source_triggers.pop(user_id, None)
            self._log_csv(username, user_id, "private", text, "blacklist", "user_rejected", "", source_message_time=source_message_time)
            if conv_id:
                self._conv_append_event(
                    conv_id,
                    role="system",
                    source="reject",
                    text="Ответ распознан как отказ — собеседник добавлен в чёрный список.",
                    new_status="dead",
                )
            self._persist_state()
            return

        current_stage = self.private_stage.get(user_id, 0)
        if current_stage == 1:
            if not self._is_qualified(normalized):
                self._log_csv(
                    username,
                    user_id,
                    "private",
                    text,
                    "stage1",
                    "reply_not_qualified",
                    "",
                    source_message_time=source_message_time,
                )
                return
            task_hint = "лендинг/сайт"
            for hint in ("лендинг", "магазин", "интернет-магазин", "портал", "личный кабинет"):
                if hint in normalized:
                    task_hint = hint
                    break
            trig = self.lead_source_triggers.get(user_id, "")
            draft2 = self._format_stage_message(2, task_hint)
            if self._stage_needs_human_approval(2) and self._outreach_db_ready():
                oq_id = self._outreach_insert_pending(
                    user_id=user_id,
                    username=username,
                    stage=2,
                    source_chat="private",
                    draft_text=draft2,
                    task_hint=task_hint,
                    lead_snippet=text.strip()[:2000],
                    trigger_match=trig,
                    conversation_id=conv_id,
                )
                if conv_id and oq_id:
                    self._conv_link_to_outreach(conv_id, oq_id)
                    self._conv_append_event(
                        conv_id,
                        role="system",
                        source="qualified",
                        stage=2,
                        text="Ответ квалифицирован: stage2-черновик ждёт согласования.",
                        new_status="waiting_approval",
                    )
                self._log_csv(username, user_id, "private", text, "stage2", "pending_approval", "", source_message_time=source_message_time)
            else:
                if conv_id:
                    self._conv_append_event(
                        conv_id,
                        role="system",
                        source="qualified",
                        stage=2,
                        text="Ответ квалифицирован: stage2 поставлен в очередь автоотправки.",
                        new_status="active",
                    )
                self._queue_dm(
                    user_id=user_id,
                    username=username,
                    stage=2,
                    source_chat="private",
                    task_hint=task_hint,
                    lead_snippet=text.strip()[:2000],
                    trigger_match=trig,
                )
                self._log_csv(username, user_id, "private", text, "stage2", "queued", "", source_message_time=source_message_time)
        elif current_stage == 2:
            if self._is_interested(normalized):
                th = self.private_task_hint.get(user_id, "")
                tr = self.lead_source_triggers.get(user_id, "")
                draft3 = self._format_stage_message(3, th)
                if self._stage_needs_human_approval(3) and self._outreach_db_ready():
                    oq_id = self._outreach_insert_pending(
                        user_id=user_id,
                        username=username,
                        stage=3,
                        source_chat="private",
                        draft_text=draft3,
                        task_hint=th,
                        lead_snippet=text.strip()[:2000],
                        trigger_match=tr,
                        conversation_id=conv_id,
                    )
                    if conv_id and oq_id:
                        self._conv_link_to_outreach(conv_id, oq_id)
                        self._conv_append_event(
                            conv_id,
                            role="system",
                            source="interested",
                            stage=3,
                            text="Ответ распознан как «интересно»: stage3 (приглашение на созвон) ждёт согласования.",
                            new_status="waiting_approval",
                        )
                    self._log_csv(username, user_id, "private", text, "stage3", "pending_approval", "", source_message_time=source_message_time)
                else:
                    if conv_id:
                        self._conv_append_event(
                            conv_id,
                            role="system",
                            source="interested",
                            stage=3,
                            text="Ответ распознан как «интересно»: stage3 поставлен в очередь автоотправки.",
                            new_status="active",
                        )
                    self._queue_dm(
                        user_id=user_id,
                        username=username,
                        stage=3,
                        source_chat="private",
                        task_hint=th,
                        lead_snippet=text.strip()[:2000],
                        trigger_match=tr,
                    )
                    self._log_csv(username, user_id, "private", text, "stage3", "queued", "", source_message_time=source_message_time)
            else:
                self._log_csv(username, user_id, "private", text, "stage2", "reply_not_interested", "", source_message_time=source_message_time)
        else:
            self._log_csv(username, user_id, "private", text, "unknown", "no_active_stage", "", source_message_time=source_message_time)
        self._persist_state()

    async def _private_listener_loop(self) -> None:
        @self.client.on(events.NewMessage(incoming=True))
        async def _handler(event):  # type: ignore[no-untyped-def]
            if not event.is_private:
                return
            sender = await event.get_sender()
            if not isinstance(sender, User):
                return
            if sender.id in self.blacklist_users:
                return
            text = event.raw_text or ""
            if not text:
                return
            await self._process_private_reply(sender, text, source_message_time=getattr(event.message, "date", None))

        logger.info("Private replies listener started")
        while True:
            await asyncio.sleep(3600)

    def _wait_telegram_code_from_file(self) -> str:
        if self.login_code_file is None:
            return ""
        return read_telegram_login_code_from_file(self.login_code_file)

    async def run(self) -> None:
        phone = str(self.config.get("phone") or "").strip()
        if not phone:
            raise ValueError("В конфиге не задан phone (номер Telegram в международном формате).")
        if not self.target_chats:
            raise ValueError(
                "В config.json пустой target_chats. Добавьте чаты (@username или id) в настройках или импортируйте из папки Telegram."
            )
        self._load_state()
        self.monitor_passes_done = 0
        self._heartbeat_db(phase="connecting")
        await start_telegram_client(
            self.client,
            phone,
            self.login_code_file,
            on_phase=lambda ph: self._heartbeat_db(phase=ph),
        )
        me = await self.client.get_me()
        self._bot_self_username = (me.username or "").strip() if hasattr(me, "username") else ""
        logger.info("Logged in as @%s", me.username or me.id)
        self._heartbeat_db(phase="ready")
        if self.dry_run:
            logger.info("DRY-RUN mode is ON: no DMs will be sent.")
        if self.schedule_enabled:
            logger.info(
                "Расписание вкл: %s, локальные часы активности %.1f–%.1f",
                self.schedule_tz,
                self.schedule_start_h,
                self.schedule_end_h,
            )

        # Pre-check chat entities once so invalid chats are skipped early.
        for chat in self.target_chats:
            chat_key = str(chat)
            try:
                input_entity = await self.client.get_input_entity(chat)
                entity = await self.client.get_entity(chat)
                if isinstance(entity, Channel):
                    try:
                        full = await self.client(functions.channels.GetFullChannelRequest(channel=input_entity))
                        linked_chat_id = getattr(full.full_chat, "linked_chat_id", None)
                        if linked_chat_id:
                            discussion_chat = int(f"-100{linked_chat_id}")
                            if discussion_chat not in self.scan_chats:
                                self.scan_chats.append(discussion_chat)
                                self.comment_source_map[str(discussion_chat)] = chat_key
                                logger.info(
                                    "Comments monitoring enabled: channel=%s -> discussion_chat=%s",
                                    chat_key,
                                    discussion_chat,
                                )
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Cannot resolve linked discussion chat for channel %s: %s", chat_key, exc)
            except Exception as exc:  # noqa: BLE001
                self.invalid_chats.add(chat_key)
                logger.error("Chat '%s' cannot be resolved and will be skipped: %s", chat_key, exc)

        if self.comment_source_map:
            logger.info("Comment sources map: %s", self.comment_source_map)

        who = f"@{me.username}" if getattr(me, "username", None) else str(me.id)
        self._merge_scan_progress(
            pass_index=0,
            pass_total=0,
            current_chat=None,
            phase="idle",
            idle_reason="ready",
        )
        start_msg = (
            f"Старт: {who} · в target_chats {len(self.target_chats)} · к обходу {len(self.scan_chats)} peer(s) · "
            f"интервал {self.monitor_interval} с"
        )
        if self.dry_run:
            start_msg += " · тестовый режим (ЛС не уходят)"
        if self.schedule_enabled:
            start_msg += f" · расписание {self.schedule_tz} {self.schedule_start_h:.0f}–{self.schedule_end_h:.0f} ч локально"
        if self.max_monitor_passes > 0:
            start_msg += f" · лимит полных проходов мониторинга: {self.max_monitor_passes}"
        self._append_scan_activity(start_msg)

        await asyncio.gather(
            self._monitor_groups_loop(),
            self._dm_sender_loop(),
            self._private_listener_loop(),
            self._heartbeat_loop(),
        )

    async def _heartbeat_loop(self) -> None:
        """Раз в 30 секунд — обновление last_heartbeat в bot_runs."""
        while True:
            try:
                self._heartbeat_db()
            except Exception as exc:  # noqa: BLE001
                logger.debug("heartbeat tick failed: %s", exc)
            await asyncio.sleep(30)


def read_telegram_login_code_from_file(path: Path, *, on_phase: Any = None) -> str:
    """Читает код из файла, отправленного web-UI. Если передан on_phase(phase:str),
    UI получит фазу 'awaiting_code' пока ждём ввод и сможет показать таймер."""
    path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(
        "Ожидание кода Telegram: в веб-интерфейсе откройте «Управление», "
        "вставьте код из приложения и нажмите «Отправить код» (до 15 мин)."
    )
    if callable(on_phase):
        try:
            on_phase("awaiting_code")
        except Exception:  # noqa: BLE001
            pass
    deadline = time.monotonic() + 900.0
    while time.monotonic() < deadline:
        try:
            if path.exists():
                raw = path.read_text(encoding="utf-8").strip()
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
                if raw:
                    return raw
        except OSError:
            pass
        time.sleep(0.35)
    raise TimeoutError("Код Telegram не получен за 15 минут.")


def read_telegram_cloud_password_from_file(path: Path, *, on_phase: Any = None) -> str:
    """Облачный пароль двухэтапной проверки (2FA) — не то же самое, что код из SMS/Telegram.
    Читается из того же файла `.telegram_login_code`, вторым запросом после успешного кода."""
    path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(
        "Ожидание пароля 2FA Telegram (облачный пароль аккаунта): это не код из SMS. "
        "В веб-интерфейсе в том же поле «Код из Telegram» введите пароль двухэтапной проверки "
        "и нажмите «Отправить» (до 15 мин)."
    )
    if callable(on_phase):
        try:
            on_phase("awaiting_password")
        except Exception:  # noqa: BLE001
            pass
    deadline = time.monotonic() + 900.0
    while time.monotonic() < deadline:
        try:
            if path.exists():
                raw = path.read_text(encoding="utf-8").strip()
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
                if raw:
                    return raw
        except OSError:
            pass
        time.sleep(0.35)
    raise TimeoutError("Пароль Telegram 2FA не получен за 15 минут.")


async def start_telegram_client(
    client: TelegramClient,
    phone: str,
    login_code_file: Path | None,
    *,
    on_phase: Any = None,
) -> None:
    for attempt in range(1, 7):
        try:
            if login_code_file is not None:
                await client.start(
                    phone=phone,
                    code_callback=lambda: read_telegram_login_code_from_file(login_code_file, on_phase=on_phase),
                    password=lambda: read_telegram_cloud_password_from_file(login_code_file, on_phase=on_phase),
                    max_attempts=10,
                )
            else:
                await client.start(phone=phone)
            return
        except sqlite3.OperationalError as exc:
            # Telethon SQLite session can be locked if two subprocesses hit the same session file.
            msg = str(exc).lower()
            if "locked" not in msg:
                raise
            wait_s = min(2 * attempt, 12)
            logger.warning("Telegram session DB is locked (attempt %s/6). Sleep %ss", attempt, wait_s)
            if callable(on_phase):
                try:
                    on_phase("connecting")
                except Exception:  # noqa: BLE001
                    pass
            await asyncio.sleep(wait_s)
            continue
        except PasswordHashInvalidError as exc:
            logger.error("Telegram 2FA password invalid: %s", exc)
            if callable(on_phase):
                try:
                    on_phase("awaiting_password")
                except Exception:  # noqa: BLE001
                    pass
            raise ValueError(
                "Неверный пароль 2FA Telegram (облачный пароль). Введите правильный пароль в поле «Код из Telegram» и отправьте ещё раз."
            ) from exc
    raise ValueError(
        "Сессия Telegram занята (sqlite database is locked). Остановите бота и дождитесь завершения фоновых операций, затем повторите."
    )


def _norm_display_title(s: str) -> str:
    t = (s or "").replace("\u200f", "").replace("\u200e", "")
    t = unicodedata.normalize("NFC", t).strip()
    return t


def _search_result_title_for_channel(chat: Any) -> str:
    title = _norm_display_title(
        (getattr(chat, "title", None) or utils.get_display_name(chat) or "").strip() or ""
    )
    if not title and getattr(chat, "usernames", None):
        uu = chat.usernames
        if uu and len(uu) > 0 and getattr(uu[0], "username", None):
            return f"@{uu[0].username}"
    un = getattr(chat, "username", None) or ""
    if not title and un:
        return f"@{un}"
    if not title:
        cid = getattr(chat, "id", None)
        return f"чат (id: {cid})" if cid is not None else "—"
    return title


def _tme_url_for_entity(ent: Any) -> str:
    un = getattr(ent, "username", None)
    if un:
        return f"https://t.me/{un}"
    if isinstance(ent, (Channel, Chat)):
        return f"https://t.me/c/{int(ent.id)}/1"
    return ""


async def _iter_dialog_participant_ids(client: TelegramClient) -> set[int]:
    """id тех чатов/каналов, где аккаунт уже состоит (в диалогах)."""
    seen: set[int] = set()
    async for d in client.iter_dialogs():
        e = d.entity
        if isinstance(e, Channel):
            seen.add(int(e.id))
        elif isinstance(e, Chat):
            seen.add(int(e.id))
    return seen


async def _enrich_channel_activity(
    client: TelegramClient, chat: Channel
) -> dict[str, Any]:
    """Последний пост публичного канала/супергруппы; при ошибке — activity=unknown."""
    extra: dict[str, Any] = {
        "last_post_iso": None,
        "inactive_days": None,
        "activity": "unknown",
    }
    try:
        msgs = await client.get_messages(chat, limit=1)
        if not msgs:
            extra["activity"] = "stale"
            extra["inactive_days"] = 9999
            return extra
        m = msgs[0]
        d = m.date
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        extra["last_post_iso"] = d.isoformat()
        days = max(0, (now - d).days)
        extra["inactive_days"] = days
        extra["activity"] = "ok"
    except Exception as exc:  # noqa: BLE001
        logger.info("enrich search row: %s", exc)
    return extra


async def _filter_rows_require_discussion(
    client: TelegramClient,
    rows: list[dict[str, Any]],
    opts: dict[str, Any],
) -> list[dict[str, Any]]:
    """Оставить мегагруппы и не-broadcast; для каналов — только с привязанным чатом обсуждения."""
    if not bool(opts.get("require_discussion")):
        return rows
    out: list[dict[str, Any]] = []
    cache_ok: dict[int, bool] = {}

    async def broadcast_has_discussion(ent: Channel) -> bool:
        icid = int(getattr(ent, "id", 0) or 0)
        if icid in cache_ok:
            return cache_ok[icid]
        await asyncio.sleep(0.12)
        try:
            inp = await client.get_input_entity(ent)
            full_ch = await client(functions.channels.GetFullChannelRequest(channel=inp))
            linked = getattr(full_ch.full_chat, "linked_chat_id", None)
            cache_ok[icid] = bool(linked)
        except Exception as exc:  # noqa: BLE001
            logger.info("require_discussion: GetFullChannel пропуск: %s", exc)
            cache_ok[icid] = False
        return cache_ok[icid]

    for row in rows:
        ref = row.get("username") or row.get("id")
        if not ref:
            out.append(row)
            continue
        try:
            ent = await client.get_entity(ref)
        except Exception as exc:  # noqa: BLE001
            logger.info("require_discussion: get_entity %s: %s — строка оставлена", ref, exc)
            out.append(row)
            continue
        if not isinstance(ent, Channel):
            out.append(row)
            continue
        if bool(getattr(ent, "megagroup", False)):
            out.append(row)
            continue
        if not bool(getattr(ent, "broadcast", False)):
            out.append(row)
            continue
        if await broadcast_has_discussion(ent):
            out.append(row)
    return out


_CONTACT_HINT_RE = re.compile(
    r"(https?://|t\.me/|telegram\.me/|tg://|@[\w\d_]{3,}|[\w.+-]+@[\w.-]+\.\w{2,})",
    re.IGNORECASE,
)
_PROMO_HINT_RE = re.compile(
    r"(https?://|\bt\.me/|@\w{3,}|скидк|акци|промокод|купить|заказать|подпиш|реклам)",
    re.IGNORECASE,
)


def _normalized_channel_search_quality(raw: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    q = raw if isinstance(raw, dict) else {}
    try:
        window_sec = int(q.get("window_sec", 86400) or 86400)
    except (TypeError, ValueError):
        window_sec = 86400
    window_sec = max(300, min(604800, window_sec))
    try:
        sample_max = int(q.get("sample_messages_max", 200) or 200)
    except (TypeError, ValueError):
        sample_max = 200
    sample_max = max(20, min(500, sample_max))
    try:
        delay = float(q.get("per_row_delay_sec", 0.18) or 0.18)
    except (TypeError, ValueError):
        delay = 0.18
    delay = max(0.05, min(2.0, delay))

    def _pct(key: str, default: float, lo: float = 0.0, hi: float = 100.0) -> float:
        try:
            v = float(q.get(key, default))
        except (TypeError, ValueError):
            v = default
        return max(lo, min(hi, v))

    def _int(key: str, default: int, lo: int, hi: int) -> int:
        try:
            v = int(q.get(key, default))
        except (TypeError, ValueError):
            v = default
        return max(lo, min(hi, v))

    sources = q.get("niche_keyword_sources")
    if not isinstance(sources, list):
        sources = ["hot_lead", "required_intent_hot_lead"]

    return {
        "enabled": bool(q.get("enabled")),
        "debug_metrics": bool(q.get("debug_metrics")),
        "window_sec": window_sec,
        "sample_messages_max": sample_max,
        "per_row_delay_sec": delay,
        "min_about_len": _int("min_about_len", 12, 0, 500),
        "discussion_min_unique_authors": _int("discussion_min_unique_authors", 0, 0, 200),
        "discussion_max_top_author_pct": _pct("discussion_max_top_author_pct", 100.0, 0.0, 100.0),
        "discussion_min_messages": _int("discussion_min_messages", 0, 0, 5000),
        "exclude_single_author_discussion": bool(q.get("exclude_single_author_discussion")),
        "min_mean_message_len": _int("min_mean_message_len", 0, 0, 500),
        "max_duplicate_text_pct": _pct("max_duplicate_text_pct", 100.0, 0.0, 100.0),
        "max_promo_ratio_pct": _pct("max_promo_ratio_pct", 100.0, 0.0, 100.0),
        "max_forward_ratio_pct": _pct("max_forward_ratio_pct", 100.0, 0.0, 100.0),
        "min_question_ratio_pct": _pct("min_question_ratio_pct", 0.0, 0.0, 100.0),
        "min_reply_ratio_pct": _pct("min_reply_ratio_pct", 0.0, 0.0, 100.0),
        "min_cyrillic_ratio_pct": _pct("min_cyrillic_ratio_pct", 0.0, 0.0, 100.0),
        "min_quality_score": _int("min_quality_score", 0, 0, 100),
        "min_keyword_hits": _int("min_keyword_hits", 1, 0, 500),
        "exclude_empty_about": bool(q.get("exclude_empty_about")),
        "exclude_no_contact_in_about": bool(q.get("exclude_no_contact_in_about")),
        "exclude_forward_heavy": bool(q.get("exclude_forward_heavy")),
        "niche_keywords_only": bool(q.get("niche_keywords_only")),
        "russian_only": bool(q.get("russian_only")),
        "niche_keyword_sources": [str(x) for x in sources if str(x).strip()],
        "extra_keywords_lines": str(q.get("extra_keywords", "") or ""),
    }


def _quality_keyword_tokens(config: Dict[str, Any], qnorm: Dict[str, Any]) -> list[str]:
    kw_root = config.get("keywords") if isinstance(config.get("keywords"), dict) else {}
    toks: list[str] = []
    seen: set[str] = set()
    for src in qnorm.get("niche_keyword_sources") or []:
        chunk = kw_root.get(src)
        if not isinstance(chunk, list):
            continue
        for line in chunk:
            s = str(line).strip().lower()
            if len(s) < 2:
                continue
            if s not in seen:
                seen.add(s)
                toks.append(s)
    for line in str(qnorm.get("extra_keywords_lines") or "").splitlines():
        s = line.strip().lower()
        if len(s) < 2:
            continue
        if s not in seen:
            seen.add(s)
            toks.append(s)
    return toks


def _quality_needs_message_sample(qn: Dict[str, Any]) -> bool:
    if qn["discussion_min_unique_authors"] > 0:
        return True
    if qn["discussion_min_messages"] > 0:
        return True
    if qn["exclude_single_author_discussion"]:
        return True
    if qn["discussion_max_top_author_pct"] < 99.5:
        return True
    if qn["min_mean_message_len"] > 0:
        return True
    if qn["max_duplicate_text_pct"] < 99.5:
        return True
    if qn["max_promo_ratio_pct"] < 99.5:
        return True
    if qn["max_forward_ratio_pct"] < 99.5:
        return True
    if qn["min_question_ratio_pct"] > 0.05:
        return True
    if qn["min_reply_ratio_pct"] > 0.05:
        return True
    if qn["min_cyrillic_ratio_pct"] > 0.05 and qn["russian_only"]:
        return True
    if qn["niche_keywords_only"]:
        return True
    if qn["exclude_forward_heavy"]:
        return True
    return False


def _normalize_msg_text_for_dup(text: str) -> str:
    s = re.sub(r"\s+", " ", (text or "").strip().lower())
    return s[:240] if s else ""


async def _gather_discussion_message_stats(
    client: TelegramClient,
    msg_entity: Any,
    *,
    window_sec: int,
    sample_max: int,
    keyword_tokens: list[str],
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=window_sec)
    messages_count = 0
    with_text = 0
    forwards = 0
    replies = 0
    questions = 0
    promo_hits = 0
    author_counts: dict[int, int] = {}
    norm_texts: list[str] = []
    cyr_letters = 0
    latin_letters = 0
    keyword_hits = 0

    async for msg in client.iter_messages(msg_entity, limit=sample_max):
        md = getattr(msg, "date", None)
        if md is None:
            continue
        if md.tzinfo is None:
            md = md.replace(tzinfo=timezone.utc)
        if md < cutoff:
            break
        messages_count += 1
        body = getattr(msg, "message", None) or ""
        st = str(body).strip()
        if st:
            with_text += 1
            if "?" in st:
                questions += 1
            if _PROMO_HINT_RE.search(st):
                promo_hits += 1
            low = st.lower()
            for tok in keyword_tokens:
                if tok and tok in low:
                    keyword_hits += 1
                    break
            for ch in st:
                if "\u0400" <= ch <= "\u04ff":
                    cyr_letters += 1
                elif "a" <= ch.lower() <= "z":
                    latin_letters += 1
            nt = _normalize_msg_text_for_dup(st)
            if nt:
                norm_texts.append(nt)
        if getattr(msg, "fwd_from", None):
            forwards += 1
        if getattr(msg, "reply_to", None) or getattr(msg, "reply_to_msg_id", None):
            replies += 1

        sid = getattr(msg, "sender_id", None)
        if isinstance(sid, int) and sid != 0:
            author_counts[sid] = author_counts.get(sid, 0) + 1

    unique_authors = len(author_counts)
    top_author_share = 0.0
    if author_counts and messages_count > 0:
        top_author_share = max(author_counts.values()) / float(messages_count)

    dup_ratio = 0.0
    if norm_texts:
        uniq_norm = len(set(norm_texts))
        dup_ratio = 1.0 - (uniq_norm / float(len(norm_texts)))

    mean_len = 0.0
    if norm_texts:
        mean_len = sum(len(x) for x in norm_texts) / float(len(norm_texts))

    question_ratio = questions / float(with_text) if with_text else 0.0
    reply_ratio = replies / float(messages_count) if messages_count else 0.0
    forward_ratio = forwards / float(messages_count) if messages_count else 0.0
    promo_ratio = promo_hits / float(with_text) if with_text else 0.0

    letters = cyr_letters + latin_letters
    cyrillic_ratio = (cyr_letters / float(letters)) if letters else 0.0

    return {
        "messages_count": messages_count,
        "messages_with_text": with_text,
        "unique_authors": unique_authors,
        "top_author_share": top_author_share,
        "question_ratio": question_ratio,
        "reply_ratio": reply_ratio,
        "forward_ratio": forward_ratio,
        "duplicate_text_ratio": dup_ratio,
        "promo_ratio": promo_ratio,
        "mean_message_len": mean_len,
        "cyrillic_ratio": cyrillic_ratio,
        "keyword_hits": keyword_hits,
    }


def _compute_quality_score(m: dict[str, Any], qn: Dict[str, Any]) -> int:
    ua = int(m.get("unique_authors") or 0)
    score = min(30, ua * 5)
    ts = float(m.get("top_author_share") or 0.0)
    score += int(25 * max(0.0, 1.0 - min(1.0, ts)))
    mc = int(m.get("messages_count") or 0)
    score += min(20, mc // 2)
    score += int(10 * min(1.0, float(m.get("question_ratio") or 0.0)))
    score += int(10 * min(1.0, float(m.get("reply_ratio") or 0.0)))
    kh = int(m.get("keyword_hits") or 0)
    need_kw = int(qn.get("min_keyword_hits") or 0)
    if (need_kw <= 0 and kh > 0) or (need_kw > 0 and kh >= need_kw):
        score += 10
    fr = float(m.get("forward_ratio") or 0.0)
    if fr > 0.35:
        score -= 15
    pr = float(m.get("promo_ratio") or 0.0)
    if pr > 0.3:
        score -= 15
    dr = float(m.get("duplicate_text_ratio") or 0.0)
    if dr > 0.4:
        score -= 10
    return max(0, min(100, score))


def _about_contact_ok(about: str) -> bool:
    s = (about or "").strip()
    if not s:
        return False
    return bool(_CONTACT_HINT_RE.search(s))


async def _filter_rows_channel_quality(
    client: TelegramClient,
    config: Dict[str, Any],
    rows: list[dict[str, Any]],
    opts: Dict[str, Any],
) -> list[dict[str, Any]]:
    raw_q = opts.get("quality")
    if not isinstance(raw_q, dict) or not bool(raw_q.get("enabled")):
        return rows
    qn = _normalized_channel_search_quality(raw_q, config)
    keyword_tokens = _quality_keyword_tokens(config, qn)
    need_msgs = _quality_needs_message_sample(qn)

    out: list[dict[str, Any]] = []
    for row in rows:
        await asyncio.sleep(qn["per_row_delay_sec"])
        ref = row.get("username") or row.get("id")
        if not ref:
            continue
        about = ""
        linked: int | None = None
        analysis_entity: Any | None = None
        try:
            ent = await client.get_entity(ref)
        except Exception as exc:  # noqa: BLE001
            logger.info("channel quality: get_entity %s: %s — пропуск строки", ref, exc)
            continue
        if isinstance(ent, Channel):
            try:
                inp = await client.get_input_entity(ent)
                full = await client(functions.channels.GetFullChannelRequest(channel=inp))
                about = str(getattr(full.full_chat, "about", "") or "")
                raw_link = getattr(full.full_chat, "linked_chat_id", None)
                linked = int(raw_link) if raw_link else None
            except Exception as exc:  # noqa: BLE001
                logger.info("channel quality: GetFullChannel %s: %s", ref, exc)
            if bool(getattr(ent, "megagroup", False)):
                analysis_entity = ent
            elif bool(getattr(ent, "broadcast", False)) and linked:
                try:
                    analysis_entity = await client.get_entity(int(f"-100{linked}"))
                except Exception as exc:  # noqa: BLE001
                    logger.info("channel quality: discussion entity %s: %s", linked, exc)
                    analysis_entity = None

        title_blob = f"{row.get('title') or ''} {about}".strip()
        title_low = title_blob.lower()

        if qn["exclude_empty_about"] and len(about.strip()) < qn["min_about_len"]:
            continue
        if qn["exclude_no_contact_in_about"] and not _about_contact_ok(about):
            continue

        if need_msgs:
            if analysis_entity is None:
                continue
            try:
                stats = await _gather_discussion_message_stats(
                    client,
                    analysis_entity,
                    window_sec=qn["window_sec"],
                    sample_max=qn["sample_messages_max"],
                    keyword_tokens=keyword_tokens,
                )
            except Exception as exc:  # noqa: BLE001
                logger.info("channel quality: iter_messages %s: %s", ref, exc)
                continue

            if qn["discussion_min_messages"] > 0 and stats["messages_count"] < qn["discussion_min_messages"]:
                continue
            if qn["discussion_min_unique_authors"] > 0 and stats["unique_authors"] < qn["discussion_min_unique_authors"]:
                continue
            if qn["exclude_single_author_discussion"] and stats["unique_authors"] <= 1:
                continue
            max_share = qn["discussion_max_top_author_pct"] / 100.0
            if max_share < 0.999 and stats["top_author_share"] > max_share:
                continue
            if qn["min_mean_message_len"] > 0 and stats["mean_message_len"] < qn["min_mean_message_len"]:
                continue
            max_dup = qn["max_duplicate_text_pct"] / 100.0
            if max_dup < 0.999 and stats["duplicate_text_ratio"] > max_dup:
                continue
            max_pr = qn["max_promo_ratio_pct"] / 100.0
            if max_pr < 0.999 and stats["promo_ratio"] > max_pr:
                continue
            max_fr = qn["max_forward_ratio_pct"] / 100.0
            if (qn["exclude_forward_heavy"] or qn["max_forward_ratio_pct"] < 99.5) and stats["forward_ratio"] > max_fr:
                continue
            min_q = qn["min_question_ratio_pct"] / 100.0
            if min_q > 0 and stats["question_ratio"] < min_q:
                continue
            min_rp = qn["min_reply_ratio_pct"] / 100.0
            if min_rp > 0 and stats["reply_ratio"] < min_rp:
                continue
            if qn["niche_keywords_only"] and stats["keyword_hits"] < max(1, qn["min_keyword_hits"]):
                continue

            if qn["russian_only"]:
                cyr_min = qn["min_cyrillic_ratio_pct"] / 100.0
                if stats["messages_with_text"] == 0:
                    if not re.search(r"[\u0400-\u04ff]", title_low):
                        continue
                elif cyr_min <= 0.01:
                    if not re.search(r"[\u0400-\u04ff]", title_low):
                        continue
                elif float(stats["cyrillic_ratio"]) < cyr_min:
                    continue

            score = _compute_quality_score(stats, qn)
            if qn["min_quality_score"] > 0 and score < qn["min_quality_score"]:
                continue
            row2 = dict(row)
            row2["quality_score"] = score
            if qn["debug_metrics"]:
                row2["quality_metrics"] = stats
            out.append(row2)
        else:
            score = 50
            if qn["russian_only"] and not re.search(r"[\u0400-\u04ff]", title_low):
                continue
            if qn["niche_keywords_only"]:
                hit = any(t in title_low for t in keyword_tokens)
                if not hit:
                    continue
            if qn["min_quality_score"] > 0 and score < qn["min_quality_score"]:
                continue
            row2 = dict(row)
            row2["quality_score"] = score
            if qn["debug_metrics"]:
                row2["quality_metrics"] = {"note": "без выборки сообщений (только карточка)"}
            out.append(row2)

    return out


async def search_public_channels_json(
    config: Dict[str, Any],
    query: str,
    limit: int,
    login_code_file: Path | None,
    search_options: Dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    opts = search_options or {}
    try:
        min_sub = max(0, int(opts.get("min_subscribers", 0) or 0))
    except (TypeError, ValueError):
        min_sub = 0
    try:
        max_inact = max(0, int(opts.get("max_inactive_days", 0) or 0))
    except (TypeError, ValueError):
        max_inact = 0
    try:
        include_stale = bool(int(opts.get("include_stale", 0) or 0))
    except (TypeError, ValueError):
        include_stale = False
    enrich = bool(opts.get("enrich", False)) or max_inact > 0

    api_id, api_hash = require_telegram_api_credentials(config)
    client = TelegramClient(config["session_name"], api_id, api_hash)
    phone = str(config.get("phone") or "").strip()
    if not phone:
        raise ValueError("В конфиге не задан phone.")
    lim = max(1, min(50, int(limit)))
    await start_telegram_client(client, phone, login_code_file)
    try:
        found = await client(functions.contacts.SearchRequest(q=query, limit=lim))
        chats = getattr(found, "chats", None) or []
        pairs: list[tuple[Channel, dict[str, Any]]] = []
        for chat in chats:
            if not isinstance(chat, Channel):
                continue
            username = (getattr(chat, "username", None) or "") or ""
            title = _search_result_title_for_channel(chat)
            cid = getattr(chat, "id", None)
            if cid is None:
                continue
            peer_id = f"-100{cid}" if not str(cid).startswith("-") else str(cid)
            participants = getattr(chat, "participants_count", None)
            if min_sub > 0 and participants is not None and int(participants) < min_sub:
                continue
            row: dict[str, Any] = {
                "title": title,
                "username": username,
                "id": peer_id,
                "participants": participants,
                "is_broadcast": bool(getattr(chat, "broadcast", False)),
                "is_megagroup": bool(getattr(chat, "megagroup", False)),
                "last_post_iso": None,
                "inactive_days": None,
                "activity": "unknown",
            }
            pairs.append((chat, row))
        if enrich and pairs:
            for chat, row in pairs:
                await asyncio.sleep(0.12)
                extra = await _enrich_channel_activity(client, chat)
                row.update(extra)
        direct_rows = [r for _c, r in pairs]
        existing_keys: set[str] = set()
        for r in direct_rows:
            pid = str(r.get("id") or "")
            if pid:
                existing_keys.add(pid)
            u = (r.get("username") or "").strip().lower()
            if u:
                existing_keys.add(u)
                existing_keys.add(f"@{u}")

        extra_rows = await _expand_search_via_comment_bios(
            client,
            pairs,
            opts,
            existing_keys,
            min_subscribers=min_sub,
        )
        if enrich and extra_rows:
            for er in extra_rows:
                await asyncio.sleep(0.12)
                try:
                    ref = er.get("username") or er.get("id")
                    if not ref:
                        continue
                    ent = await client.get_entity(ref)
                    if isinstance(ent, Channel):
                        ex = await _enrich_channel_activity(client, ent)
                        er.update(ex)
                except Exception as exc:  # noqa: BLE001
                    logger.info("enrich comment-bio row: %s", exc)

        out = direct_rows + extra_rows
        out = await _filter_rows_require_discussion(client, out, opts)
        out = await _filter_rows_channel_quality(client, config, out, opts)
        if max_inact > 0 and out:
            nxt: list[dict[str, Any]] = []
            for row in out:
                if row.get("activity") == "unknown":
                    nxt.append(row)
                    continue
                inv = row.get("inactive_days")
                too_old = inv is not None and int(inv) > max_inact
                if too_old:
                    row = {**row, "activity": "stale"}
                if too_old and not include_stale:
                    continue
                nxt.append(row)
            out = nxt
        return out
    finally:
        await client.disconnect()


async def _try_join_channel_for_folder(client: TelegramClient, ent: Channel) -> tuple[bool, str | None]:
    """
    Публичное вступление в канал / супергруппу (Join). Уже участник — успех.
    """
    try:
        inp = await client.get_input_entity(ent)
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
    for _attempt in range(6):
        try:
            await client(functions.channels.JoinChannelRequest(channel=inp))
            return True, None
        except UserAlreadyParticipantError:
            return True, None
        except FloodWaitError as e:
            w = min(int(getattr(e, "seconds", 0) or 0) + 2, 320)
            logger.info("JoinChannel FloodWait, sleep %ss", w)
            await asyncio.sleep(w)
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
    return False, "JoinChannel: не удалось после FloodWait"


_INVITE_HASH_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/|\+)([a-zA-Z0-9_-]+)",
    re.IGNORECASE,
)


async def _try_import_invite_link(client: TelegramClient, piece: str) -> tuple[Any | None, str | None]:
    """Вступление по invite; возвращает peer id (-100…) или None."""
    s = (piece or "").strip()
    m = _INVITE_HASH_RE.search(s)
    if not m:
        return None, "not_invite_link"
    slug = m.group(1)
    try:
        upd = await client(functions.messages.ImportChatInviteRequest(hash=slug))
        chats = getattr(upd, "chats", None) or []
        for ch in chats:
            if isinstance(ch, Channel):
                cid = getattr(ch, "id", None)
                if cid is None:
                    continue
                peer = f"-100{cid}" if not str(cid).startswith("-") else str(cid)
                try:
                    return int(peer), None
                except ValueError:
                    return peer, None
        return None, "no_channel_in_updates"
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def _enroll_add_extra_unique(extras: list[Any], seen: set[str], ref: Any) -> None:
    if ref is None:
        return
    if isinstance(ref, int):
        k = str(ref)
    else:
        k = str(ref).strip()
    if not k or k in seen:
        return
    seen.add(k)
    extras.append(ref if isinstance(ref, int) else str(ref).strip())


async def enroll_chats_for_monitoring_json(
    config: Dict[str, Any],
    refs: list[str],
    login_code_file: Path | None,
    enroll_options: Dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Вступление в каналы, при необходимости — в чат обсуждения; разбор ссылок из описания канала.
    Возвращает extras — записи для добавления в target_chats (обсуждения и успешные вступления из about).
    """
    eopt = enroll_options or {}
    auto_join = bool(eopt.get("auto_join", True))
    include_discussion = bool(eopt.get("include_discussion", True))
    about_mode = str(eopt.get("about_links", "list") or "list").strip().lower()
    if about_mode not in ("skip", "list", "join"):
        about_mode = "list"

    try:
        join_gap = float(eopt.get("join_gap_sec", 1.2) or 1.2)
    except (TypeError, ValueError):
        join_gap = 1.2
    join_gap = max(0.2, min(120.0, join_gap))
    inner_pause = max(0.12, min(join_gap, 0.45))

    api_id, api_hash = require_telegram_api_credentials(config)
    client = TelegramClient(config["session_name"], api_id, api_hash)
    phone = str(config.get("phone") or "").strip()
    if not phone:
        raise ValueError("В конфиге не задан phone.")
    await start_telegram_client(client, phone, login_code_file)

    extras: list[Any] = []
    extras_seen: set[str] = set()
    details: list[dict[str, Any]] = []

    try:
        for idx, ref_s in enumerate(refs):
            ref_s = str(ref_s).strip()
            if not ref_s:
                continue
            if idx > 0:
                await asyncio.sleep(join_gap)
            d: dict[str, Any] = {
                "ref": ref_s,
                "joined_main": None,
                "discussion": None,
                "about_links": None,
            }
            try:
                ent = await client.get_entity(ref_s)
            except Exception as exc:  # noqa: BLE001
                d["error"] = str(exc)
                details.append(d)
                continue

            if not isinstance(ent, Channel):
                d["discussion"] = None
                d["about_links"] = None
                details.append(d)
                continue

            joined_main: bool | None = None
            if auto_join:
                ok_m, _ = await _try_join_channel_for_folder(client, ent)
                joined_main = ok_m
                await asyncio.sleep(inner_pause)
            d["joined_main"] = joined_main

            about = ""
            linked: int | None = None
            try:
                inp = await client.get_input_entity(ent)
                full = await client(functions.channels.GetFullChannelRequest(channel=inp))
                about = getattr(full.full_chat, "about", "") or ""
                raw_link = getattr(full.full_chat, "linked_chat_id", None)
                linked = int(raw_link) if raw_link else None
            except Exception as exc:  # noqa: BLE001
                d["full_channel_error"] = str(exc)

            if include_discussion and linked:
                disc_block: dict[str, Any] = {"ref": None, "joined": False, "error": None}
                try:
                    disc_id = int(f"-100{linked}")
                    disc_ent = await client.get_entity(disc_id)
                    disc_block["ref"] = disc_id
                    _enroll_add_extra_unique(extras, extras_seen, disc_id)
                    if auto_join and isinstance(disc_ent, Channel):
                        ok_d, err_d = await _try_join_channel_for_folder(client, disc_ent)
                        disc_block["joined"] = ok_d
                        if not ok_d:
                            disc_block["error"] = err_d
                    elif auto_join:
                        disc_block["error"] = "discussion_not_channel"
                    await asyncio.sleep(inner_pause)
                except Exception as exc:  # noqa: BLE001
                    disc_block["error"] = str(exc)
                d["discussion"] = disc_block
            elif include_discussion:
                d["discussion"] = {"ref": None, "joined": False, "error": None}
            else:
                d["discussion"] = None

            al: dict[str, Any] = {"listed": [], "joined": [], "failed": []}
            if about_mode != "skip" and about:
                parsed = _parse_channel_refs_from_bio(about)
                if about_mode == "list":
                    al["listed"] = list(parsed)
                elif about_mode == "join" and auto_join:
                    for piece in parsed:
                        await asyncio.sleep(inner_pause)
                        if _INVITE_HASH_RE.search(piece):
                            peer_i, ierr = await _try_import_invite_link(client, piece)
                            if peer_i is not None:
                                _enroll_add_extra_unique(extras, extras_seen, peer_i)
                                al["joined"].append(piece)
                            else:
                                al["failed"].append({"ref": piece, "error": ierr or "?"})
                            continue
                        try:
                            sub = await client.get_entity(piece)
                        except Exception as exc:  # noqa: BLE001
                            al["failed"].append({"ref": piece, "error": str(exc)})
                            continue
                        if isinstance(sub, Channel):
                            ok_s, err_s = await _try_join_channel_for_folder(client, sub)
                            if ok_s:
                                un = (getattr(sub, "username", None) or "") or ""
                                cid = getattr(sub, "id", None)
                                if un:
                                    _enroll_add_extra_unique(extras, extras_seen, f"@{un}")
                                elif cid is not None:
                                    pid = (
                                        f"-100{cid}"
                                        if not str(cid).startswith("-")
                                        else str(cid)
                                    )
                                    try:
                                        _enroll_add_extra_unique(extras, extras_seen, int(pid))
                                    except ValueError:
                                        _enroll_add_extra_unique(extras, extras_seen, pid)
                                al["joined"].append(piece)
                            else:
                                al["failed"].append({"ref": piece, "error": err_s or "join"})
                        else:
                            al["failed"].append({"ref": piece, "error": "not_a_channel"})
                elif about_mode == "join" and not auto_join:
                    al["listed"] = list(parsed)
            d["about_links"] = al if about_mode != "skip" else None
            details.append(d)

        return {"extras": extras, "details": details}
    finally:
        await client.disconnect()


async def create_telegram_folder_json(
    config: Dict[str, Any],
    folder_title: str,
    chat_refs: list[str],
    login_code_file: Path | None,
    max_peers: int = 100,
    folder_options: Dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Создаёт пользовательскую папку (dialog filter).
    По умолчанию — автовступ (Join) в публичные каналы/супергруппы с @, затем добавление в папку;
    что не вступается (приват, лимит) — t.me в ответе. Отключение: folder_options['auto_join']=False.
    """
    fopts = folder_options or {}
    auto_join = bool(fopts.get("auto_join", True))

    title = folder_title.strip()
    if not title:
        raise ValueError("Пустое имя папки.")
    if len(title) > 64:
        title = title[:64]
    api_id, api_hash = require_telegram_api_credentials(config)
    client = TelegramClient(config["session_name"], api_id, api_hash)
    phone = str(config.get("phone") or "").strip()
    if not phone:
        raise ValueError("В конфиге не задан phone.")
    await start_telegram_client(client, phone, login_code_file)
    try:
        member_of = await _iter_dialog_participant_ids(client)
        res = await client(functions.messages.GetDialogFiltersRequest())
        filters = getattr(res, "filters", res) or []
        max_id = 0
        for filt in filters:
            fid = getattr(filt, "id", None)
            if fid is not None:
                max_id = max(max_id, int(fid))
        new_id = max_id + 1

        include_peers: list[Any] = []
        errors: list[str] = []
        manual_tme_links: list[str] = []
        auto_joined_n = 0
        for ref in chat_refs[:max_peers]:
            ref_s = str(ref).strip()
            if not ref_s:
                continue
            try:
                ent = await client.get_entity(ref_s)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{ref_s}: {exc}")
                continue
            tme = _tme_url_for_entity(ent)
            ent_id: int | None = None
            if isinstance(ent, (Channel, Chat)):
                ent_id = int(ent.id)
            if ent_id is not None and ent_id in member_of:
                try:
                    include_peers.append(await client.get_input_entity(ent))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{ref_s}: {exc}")
                continue

            if auto_join and isinstance(ent, Channel) and ent_id is not None:
                ok, jerr = await _try_join_channel_for_folder(client, ent)
                if ok:
                    member_of.add(ent_id)
                    auto_joined_n += 1
                    try:
                        include_peers.append(await client.get_input_entity(ent))
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"{ref_s} (после вступления): {exc}")
                    await asyncio.sleep(0.4)
                else:
                    err_line = f"{ref_s}: не вступить автоматически: {jerr or '—'}"
                    errors.append(err_line)
                    if tme:
                        manual_tme_links.append(tme)
                continue

            if tme:
                manual_tme_links.append(tme)
            else:
                if not auto_join:
                    errors.append(
                        f"{ref_s}: нет в диалогах; включите автовступ или откройте в Telegram (автовступ выключен)"
                    )
                else:
                    errors.append(
                        f"{ref_s}: не супергруппа/канал или нет @ — откройте в Telegram, затем снова «Создать папку»"
                    )

        if not include_peers and not manual_tme_links and not errors:
            raise ValueError("Не выбрано ни одного чата.")
        if not include_peers and not manual_tme_links and errors:
            raise ValueError(
                "Не удалось обработать выбор: " + ("; ".join(errors[:5]) if errors else "ошибка")
            )

        ulinks = list(dict.fromkeys(manual_tme_links))[:200]

        def _df(with_peers: list[Any]) -> DialogFilter:
            return DialogFilter(
                id=new_id,
                title=title,
                pinned_peers=[],
                include_peers=with_peers,
                exclude_peers=[],
                contacts=False,
                non_contacts=False,
                groups=True,
                broadcasts=True,
                bots=False,
            )

        if include_peers:
            await client(
                functions.messages.UpdateDialogFilterRequest(
                    id=new_id, filter=_df(include_peers)
                )
            )
            out: dict[str, Any] = {
                "folder_id": new_id,
                "title": title,
                "peers_added": len(include_peers),
                "auto_join_used": auto_join,
                "auto_joined_n": auto_joined_n,
                "no_auto_subscribe": not auto_join,
                "manual_tme_links": ulinks,
                "peer_errors": errors[:20],
            }
            if ulinks and auto_join:
                out["folder_hint_ru"] = (
                    f"В папку добавлено {len(include_peers)} чат(ов) "
                    f"({auto_joined_n} с автовступом). "
                    f"По {len(ulinks)} ссылкам вступить не вышло (приват, лимит) — "
                    f"при необходимости вручную, затем «{title}» — «Добавить чаты»."
                )
            elif ulinks and not auto_join:
                out["folder_hint_ru"] = (
                    f"Автовступ отключён: в папку попали {len(include_peers)} чат(ов) из уже открытых. "
                    f"Остальные {len(ulinks)} — по ссылкам, затем снова «Создать папку» или папка вручную в клиенте."
                )
            return out

        # Не удалось взять ни одного пира: только ссылки и/или пустая папка.
        only_links: dict[str, Any] = {
            "folder_id": None,
            "title": title,
            "peers_added": 0,
            "auto_join_used": auto_join,
            "auto_joined_n": 0,
            "no_auto_subscribe": not auto_join,
            "only_manual_links": True,
            "manual_tme_links": ulinks,
            "peer_errors": errors[:20],
            "folder_hint_ru": (
                f"Автовступ: {'включён' if auto_join else 'выкл.'}. "
                f"Ни в один чат не вступить ({len(ulinks)} ссыл. ниже) — "
                f"приват, инвайт или слишком плотный лимит. Откройте в Telegram, затем снова «Создать папку»."
                if ulinks
                else f"Автовступ: {'включён' if auto_join else 'выкл.'}. "
                f"Проверьте выбор: не удалось разрешить чаты."
            ),
        }
        try:
            await client(
                functions.messages.UpdateDialogFilterRequest(id=new_id, filter=_df([]))
            )
            only_links["folder_id"] = new_id
            if ulinks:
                only_links["folder_hint_ru"] = (
                    f"Создана папка «{title}» (id {new_id}). В чаты не вступилось: ссылки ниже, "
                    f"затем снова «Создать папку» с тем же выбором."
                )
        except Exception:  # noqa: BLE001
            only_links["empty_folder_tried"] = False
        else:
            only_links["empty_folder_tried"] = True
        return only_links
    finally:
        await client.disconnect()


def validate_config(config: Dict[str, Any]) -> None:
    required_top = [
        "api_id",
        "api_hash",
        "phone",
        "session_name",
        "target_chats",
        "keywords",
        "templates",
        "limits",
        "partner_name",
    ]
    missing = [k for k in required_top if k not in config]
    if missing:
        raise ValueError(f"Missing config keys: {missing}")
    # api_id / api_hash могут быть пустыми в черновике конфига; перед запуском бота проверяет require_telegram_api_credentials
    if not isinstance(config["target_chats"], list):
        raise ValueError("target_chats must be a list (may be empty until you add chats)")
    for chat in config["target_chats"]:
        if isinstance(chat, str) and "public_chat_username" in chat:
            logger.warning(
                "Placeholder chat '%s' found in config. Replace with real @username or numeric ID.",
                chat,
            )


def _utility_mode_skips_run_lock(args: Any) -> bool:
    """Команды «выполнил и вышел» не держат .bot_run.lock — их можно параллелить по разным session_name (напр. sync-dialogs)."""
    if getattr(args, "list_chats_json", False):
        return True
    if getattr(args, "list_chats", False):
        return True
    if getattr(args, "list_folders", False):
        return True
    if str(getattr(args, "import_folder", "") or "").strip():
        return True
    if str(getattr(args, "leave_chats_json", "") or "").strip():
        return True
    if str(getattr(args, "create_folder", "") or "").strip():
        return True
    if str(getattr(args, "enroll_monitoring_json", "") or "").strip():
        return True
    if str(getattr(args, "search_channels", "") or "").strip():
        return True
    return False


def acquire_run_lock() -> None:
    if RUN_LOCK_PATH.exists():
        existing = RUN_LOCK_PATH.read_text(encoding="utf-8").strip()
        try:
            existing_pid = int(existing)
        except ValueError:
            existing_pid = -1

        is_running = False
        if existing_pid > 0:
            try:
                os.kill(existing_pid, 0)
                is_running = True
            except OSError:
                is_running = False

        if is_running:
            raise BotAlreadyRunningError(
                "Bot is already running. "
                f"Active PID from lock: {existing_pid}. Stop it before starting another instance."
            )

        logger.warning("Stale lock detected at %s (pid=%s). Removing.", RUN_LOCK_PATH, existing)
        try:
            RUN_LOCK_PATH.unlink()
        except OSError as exc:
            raise RuntimeError(f"Cannot remove stale lock file {RUN_LOCK_PATH}: {exc}") from exc

    RUN_LOCK_PATH.write_text(str(os.getpid()), encoding="utf-8")

    def _cleanup_lock() -> None:
        try:
            if RUN_LOCK_PATH.exists():
                RUN_LOCK_PATH.unlink()
        except OSError:
            pass

    atexit.register(_cleanup_lock)


def apply_telegram_active_account_to_config(cfg: Dict[str, Any]) -> None:
    """Несколько аккаунта в `telegram_accounts`: подставляем active в api_id/phone/session_name для Telethon."""
    accs = cfg.get("telegram_accounts")
    if not isinstance(accs, list) or not accs:
        return
    active = str(cfg.get("active_telegram_account") or "").strip() or "default"
    chosen: Dict[str, Any] | None = None
    for a in accs:
        if isinstance(a, dict) and str(a.get("id", "")).strip() == active:
            chosen = a
            break
    if chosen is None:
        for a in accs:
            if isinstance(a, dict):
                chosen = a
                break
    if not isinstance(chosen, dict):
        return
    for k in ("api_id", "api_hash", "phone"):
        if k in chosen and chosen.get(k) is not None:
            cfg[k] = chosen[k]
    stem = str(chosen.get("session_stem") or "").strip()
    if stem:
        p = Path(stem)
        cfg["session_name"] = str(p) if p.is_absolute() else str(SESSIONS_DIR / stem)


def apply_telegram_account_for_session_path(cfg: Dict[str, Any], session_path_arg: str) -> None:
    """Подставить api_id/api_hash/phone из `telegram_accounts` под переданный --session-name (не активный)."""
    raw = (session_path_arg or "").strip()
    if not raw:
        return
    accs = cfg.get("telegram_accounts")
    if not isinstance(accs, list) or not accs:
        return
    try:
        target = Path(raw).expanduser().resolve()
    except OSError:
        target = Path(raw)
    tgt_name = target.name
    chosen: Dict[str, Any] | None = None
    for a in accs:
        if not isinstance(a, dict):
            continue
        stem = str(a.get("session_stem") or "").strip()
        if not stem:
            continue
        cand = Path(stem) if Path(stem).is_absolute() else (SESSIONS_DIR / stem)
        try:
            cand_res = cand.expanduser().resolve()
        except OSError:
            cand_res = cand
        if cand_res == target or cand_res.name == tgt_name:
            chosen = a
            break
    if chosen is None:
        return
    for k in ("api_id", "api_hash", "phone"):
        if k in chosen and chosen.get(k) is not None:
            cfg[k] = chosen[k]


def load_config(config_path: Path = CONFIG_PATH) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(
            "config.json not found. Create it from template in README before running."
        )
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    apply_telegram_active_account_to_config(cfg)
    validate_config(cfg)
    return cfg


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as f:
        return max(sum(1 for _ in f) - 1, 0)


def _print_console_ui(config: Dict[str, Any]) -> None:
    print("=" * 58)
    print(" Telegram Leadgen Bot - Console UI ")
    print("=" * 58)
    print(f"Mode: {'DRY-RUN (safe)' if config.get('dry_run', False) else 'LIVE (sends DM)'}")
    print(f"Session: {config['session_name']}")
    print(f"Phone: {config['phone']}")
    print(f"Chats: {', '.join(str(x) for x in config['target_chats'])}")
    print(
        "Limits: "
        f"{config['limits']['max_dm_per_hour_per_chat']} DM/hour/chat, "
        f"daily {config['limits']['daily_limit_range'][0]}-{config['limits']['daily_limit_range'][1]}"
    )
    print(f"Logged leads in CSV: {_count_csv_rows(CSV_PATH)}")
    print(f"Log file: {LOG_FILE}")
    print("-" * 58)
    print("1) Start bot")
    print("2) Toggle dry-run")
    print("3) Exit")
    print("4) Import chats from Telegram folder")
    print("-" * 58)


def _interactive_menu(config: Dict[str, Any]) -> Dict[str, Any] | None:
    while True:
        _print_console_ui(config)
        choice = input("Select action [1-4]: ").strip()
        if choice == "1":
            return config
        if choice == "2":
            config["dry_run"] = not bool(config.get("dry_run", False))
            print(
                f"[INFO] dry_run changed to {config['dry_run']} "
                "(only for this run; config.json unchanged)"
            )
            logger.info("dry_run toggled in menu: %s", config["dry_run"])
            continue
        if choice == "3":
            print("[INFO] Exit")
            return None
        if choice == "4":
            print("\nFolder title (exact or partial):")
            folder_query = input("> ").strip()
            if not folder_query:
                print("[WARN] Folder title is empty.\n")
                continue
            return {"__folder_import__": folder_query}
        print("[WARN] Invalid option. Please choose 1, 2, 3 or 4.\n")


async def main() -> None:
    global CONFIG_PATH, STATE_PATH, CSV_PATH, LOG_DIR, LOG_FILE, RUN_LOCK_PATH
    parser = argparse.ArgumentParser(description="Telegram Leadgen Bot (Telethon)")
    parser.add_argument("--config-path", type=str, default=str(CONFIG_PATH))
    parser.add_argument("--state-path", type=str, default=str(STATE_PATH))
    parser.add_argument("--csv-path", type=str, default=str(CSV_PATH))
    parser.add_argument("--log-path", type=str, default=str(LOG_FILE))
    parser.add_argument("--session-name", type=str, default="")
    parser.add_argument(
        "--list-chats",
        action="store_true",
        help="List available dialogs/chats with IDs and usernames, then exit.",
    )
    parser.add_argument(
        "--list-chats-json",
        action="store_true",
        help="List channels/groups in account as one JSON line on stdout, then exit.",
    )
    parser.add_argument(
        "--list-chats-limit",
        type=int,
        default=500,
        help="With --list-chats-json: max dialogs to scan (10–2000).",
    )
    parser.add_argument(
        "--leave-chats-json",
        type=str,
        default="",
        help="JSON-массив ссылок @name / -100… — выйти из чата/канала (delete_dialog), результат в stdout JSON.",
    )
    parser.add_argument(
        "--list-folders",
        action="store_true",
        help="List Telegram chat folders and exit.",
    )
    parser.add_argument(
        "--import-folder",
        type=str,
        default="",
        help="Import chats from Telegram folder by title or folder id.",
    )
    parser.add_argument(
        "--apply-import",
        action="store_true",
        help="With --import-folder, write found usernames/ids to config.json target_chats.",
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Start bot immediately with config.json, without interactive menu.",
    )
    parser.add_argument(
        "--force-unlock",
        action="store_true",
        help="Remove .bot_run.lock before start (only if you are sure no bot instance is running).",
    )
    parser.add_argument(
        "--login-code-file",
        type=str,
        default="",
        help="Путь к файлу: веб-консоль записывает OTP, бот читает при авторизации Telethon.",
    )
    parser.add_argument(
        "--org-id",
        type=int,
        default=0,
        help="ID организации (совпадает с tenants/org_N) для очереди согласований в data.db.",
    )
    parser.add_argument(
        "--data-db",
        type=str,
        default="",
        help="Путь к SQLite data.db веб-приложения (очередь outreach_queue).",
    )
    parser.add_argument(
        "--search-channels",
        type=str,
        default="",
        help="Поиск публичных каналов/чатов по строке; результат — одна строка JSON в stdout.",
    )
    parser.add_argument(
        "--search-limit",
        type=int,
        default=25,
        help="Максимум результатов для --search-channels (1–50).",
    )
    parser.add_argument(
        "--search-options-json",
        type=str,
        default="",
        help='JSON-опции поиска: min_subscribers, max_inactive_days, enrich, include_stale, '
        "via_comments, comments_messages_per_channel, commenters_max_per_channel, bio_keywords.",
    )
    parser.add_argument(
        "--create-folder",
        type=str,
        default="",
        help="Создать папку чатов Telegram; список пиров в --folder-chats-json; результат — JSON в stdout.",
    )
    parser.add_argument(
        "--folder-chats-json",
        type=str,
        default="",
        help='JSON-массив ссылок (@name или -100…) для папки (вместе с --create-folder).',
    )
    parser.add_argument(
        "--folder-options-json",
        type=str,
        default="",
        help='JSON: { "auto_join": true } — вступать в публичные каналы перед добавлением в папку.',
    )
    parser.add_argument(
        "--enroll-monitoring-json",
        type=str,
        default="",
        help="JSON-массив ссылок: вступление, обсуждение канала, ссылки из описания; ответ — одна строка JSON.",
    )
    parser.add_argument(
        "--enroll-options-json",
        type=str,
        default="",
        help='JSON: {"auto_join":true,"include_discussion":true,"about_links":"skip|list|join"}.',
    )
    args = parser.parse_args()
    CONFIG_PATH = Path(args.config_path)
    STATE_PATH = Path(args.state_path)
    CSV_PATH = Path(args.csv_path)
    LOG_FILE = Path(args.log_path)
    LOG_DIR = LOG_FILE.parent
    RUN_LOCK_PATH = LOG_FILE.parent / ".bot_run.lock"

    setup_logging(log_file=LOG_FILE)
    logger.info("Bot startup")

    skip_lock = _utility_mode_skips_run_lock(args)
    if not skip_lock:
        if args.force_unlock and RUN_LOCK_PATH.exists():
            RUN_LOCK_PATH.unlink(missing_ok=True)
            logger.warning("Force-unlock requested. Removed %s", RUN_LOCK_PATH)

        try:
            acquire_run_lock()
        except BotAlreadyRunningError as exc:
            logger.warning("%s", exc)
            print(f"[WARN] {exc}")
            return

    config = load_config(CONFIG_PATH)
    if args.session_name:
        config["session_name"] = args.session_name
        apply_telegram_account_for_session_path(config, args.session_name)
    lc_common = str(getattr(args, "login_code_file", "") or "").strip()
    lc_path_common = Path(lc_common) if lc_common else None
    if args.list_chats_json:
        await list_chats_for_monitoring_json(
            config,
            dialog_limit=int(getattr(args, "list_chats_limit", 500) or 500),
            login_code_file=lc_path_common,
        )
        return
    leave_raw = str(getattr(args, "leave_chats_json", "") or "").strip()
    if leave_raw:
        lc_raw = str(getattr(args, "login_code_file", "") or "").strip()
        lc_path = Path(lc_raw) if lc_raw else None
        try:
            parsed = json.loads(leave_raw)
            if not isinstance(parsed, list):
                raise ValueError("leave-chats-json: ожидается JSON-массив строк.")
            leave_refs = [str(x).strip() for x in parsed if str(x).strip()]
            if not leave_refs:
                raise ValueError("Массив ссылок пуст.")
        except Exception as exc:  # noqa: BLE001
            sys.stdout.write(
                json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False) + "\n"
            )
            sys.stdout.flush()
            return
        try:
            out = await leave_chats_from_dialogs_json(config, leave_refs, lc_path)
            sys.stdout.write(json.dumps({"ok": True, **out}, ensure_ascii=False) + "\n")
            sys.stdout.flush()
        except Exception as exc:  # noqa: BLE001
            sys.stdout.write(
                json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False) + "\n"
            )
            sys.stdout.flush()
        return
    if args.list_chats:
        await list_available_chats(config, login_code_file=lc_path_common)
        return
    if args.list_folders:
        await list_folders(config, login_code_file=lc_path_common)
        return
    if args.import_folder:
        await import_from_folder(config, args.import_folder, args.apply_import, CONFIG_PATH, login_code_file=lc_path_common)
        return
    folder_title = str(getattr(args, "create_folder", "") or "").strip()
    folder_chats_raw = str(getattr(args, "folder_chats_json", "") or "").strip()
    if folder_title:
        lc_raw = str(getattr(args, "login_code_file", "") or "").strip()
        lc_path = Path(lc_raw) if lc_raw else None
        try:
            if not folder_chats_raw:
                raise ValueError("Укажите --folder-chats-json с массивом чатов.")
            parsed = json.loads(folder_chats_raw)
            if not isinstance(parsed, list):
                raise ValueError("folder-chats-json должен быть JSON-массивом.")
            refs = [str(x).strip() for x in parsed if str(x).strip()]
            if not refs:
                raise ValueError("Массив чатов пуст.")
            fopts: dict[str, Any] = {}
            raw_f = str(getattr(args, "folder_options_json", "") or "").strip()
            if raw_f:
                try:
                    pfo = json.loads(raw_f)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"folder-options-json: {exc}") from exc
                if not isinstance(pfo, dict):
                    raise ValueError("folder-options-json: ожидается JSON-объект.")
                fopts = pfo
            result = await create_telegram_folder_json(
                config, folder_title, refs, lc_path, folder_options=fopts
            )
            sys.stdout.write(json.dumps({"ok": True, **result}, ensure_ascii=False) + "\n")
            sys.stdout.flush()
        except Exception as exc:  # noqa: BLE001
            sys.stdout.write(
                json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False) + "\n"
            )
            sys.stdout.flush()
        return
    enroll_raw = str(getattr(args, "enroll_monitoring_json", "") or "").strip()
    if enroll_raw:
        lc_raw = str(getattr(args, "login_code_file", "") or "").strip()
        lc_path = Path(lc_raw) if lc_raw else None
        eopts: dict[str, Any] = {}
        raw_e = str(getattr(args, "enroll_options_json", "") or "").strip()
        if raw_e:
            try:
                pe = json.loads(raw_e)
            except json.JSONDecodeError as exc:
                sys.stdout.write(
                    json.dumps({"ok": False, "error": f"enroll-options-json: {exc}"}, ensure_ascii=False) + "\n"
                )
                sys.stdout.flush()
                return
            if not isinstance(pe, dict):
                sys.stdout.write(
                    json.dumps(
                        {"ok": False, "error": "enroll-options-json: ожидается JSON-объект."},
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                sys.stdout.flush()
                return
            eopts = pe
        try:
            parsed = json.loads(enroll_raw)
            if not isinstance(parsed, list):
                raise ValueError("enroll-monitoring-json: ожидается JSON-массив строк.")
            eref = [str(x).strip() for x in parsed if str(x).strip()]
            if not eref:
                raise ValueError("Массив ссылок пуст.")
            out = await enroll_chats_for_monitoring_json(config, eref, lc_path, eopts)
            sys.stdout.write(json.dumps({"ok": True, **out}, ensure_ascii=False) + "\n")
            sys.stdout.flush()
        except Exception as exc:  # noqa: BLE001
            sys.stdout.write(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False) + "\n")
            sys.stdout.flush()
        return
    q_search = str(getattr(args, "search_channels", "") or "").strip()
    if q_search:
        lc_raw = str(getattr(args, "login_code_file", "") or "").strip()
        lc_path = Path(lc_raw) if lc_raw else None
        sopts: dict[str, Any] = {}
        raw_sopt = str(getattr(args, "search_options_json", "") or "").strip()
        if raw_sopt:
            try:
                parsed = json.loads(raw_sopt)
            except json.JSONDecodeError as exc:
                sys.stdout.write(
                    json.dumps({"ok": False, "error": f"search-options-json: {exc}"}, ensure_ascii=False) + "\n"
                )
                sys.stdout.flush()
                return
            if not isinstance(parsed, dict):
                sys.stdout.write(
                    json.dumps(
                        {"ok": False, "error": "search-options-json: ожидается JSON-объект."},
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                sys.stdout.flush()
                return
            sopts = parsed
        try:
            rows = await search_public_channels_json(
                config,
                q_search,
                int(getattr(args, "search_limit", 25) or 25),
                lc_path,
                search_options=sopts,
            )
            sys.stdout.write(json.dumps({"ok": True, "items": rows}, ensure_ascii=False) + "\n")
            sys.stdout.flush()
        except Exception as exc:  # noqa: BLE001
            sys.stdout.write(
                json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False) + "\n"
            )
            sys.stdout.flush()
        return
    if args.run_now:
        lc_raw = str(getattr(args, "login_code_file", "") or "").strip()
        lc_path = Path(lc_raw) if lc_raw else None
        oid = int(getattr(args, "org_id", 0) or 0)
        org_id_parsed = oid if oid > 0 else None
        ddb = str(getattr(args, "data_db", "") or "").strip()
        data_db_path = Path(ddb) if ddb else None
        bot = LeadGenBot(
            config,
            login_code_file=lc_path,
            org_id=org_id_parsed,
            data_db_path=data_db_path,
        )
        await bot.run()
        return

    selected_config = _interactive_menu(config)
    if selected_config is None:
        logger.info("Exited from menu by user")
        return
    if "__folder_import__" in selected_config:
        await import_from_folder(
            config, str(selected_config["__folder_import__"]), apply=True, config_path=CONFIG_PATH
        )
        return
    bot = LeadGenBot(selected_config)
    await bot.run()


async def list_available_chats(config: Dict[str, Any], *, login_code_file: Path | None = None) -> None:
    api_id, api_hash = require_telegram_api_credentials(config)
    client = TelegramClient(
        config["session_name"],
        api_id,
        api_hash,
    )
    phone = str(config.get("phone") or "").strip()
    if not phone:
        raise ValueError("В конфиге не задан phone.")
    await start_telegram_client(client, phone, login_code_file)
    me = await client.get_me()
    logger.info("Listing chats as @%s", me.username or me.id)

    print("\n=== Available chats/dialogs ===")
    print("Use `id` (recommended) or `@username` in config.json -> target_chats.\n")
    async for dialog in client.iter_dialogs(limit=200):
        ent = dialog.entity
        title = dialog.name or ""
        username = getattr(ent, "username", None)
        chat_id = getattr(ent, "id", None)
        if chat_id is None:
            continue

        # Telegram peer IDs for groups/channels are usually stored with -100 prefix in config.
        if dialog.is_group or dialog.is_channel:
            resolved_id = f"-100{chat_id}" if not str(chat_id).startswith("-100") else str(chat_id)
        else:
            resolved_id = str(chat_id)

        username_part = f"@{username}" if username else "-"
        kind = "channel/group" if (dialog.is_group or dialog.is_channel) else "user"
        print(f"[{kind}] id={resolved_id} | username={username_part} | title={title}")

    print("\nCopy needed values into config.json -> target_chats")
    await client.disconnect()


async def list_chats_for_monitoring_json(
    config: Dict[str, Any],
    *,
    dialog_limit: int = 500,
    login_code_file: Path | None = None,
) -> None:
    """Каналы и группы из диалогов аккаунта — одна строка JSON в stdout (для веб-консоли)."""
    api_id, api_hash = require_telegram_api_credentials(config)
    client = TelegramClient(
        config["session_name"],
        api_id,
        api_hash,
    )
    phone = str(config.get("phone") or "").strip()
    if not phone:
        raise ValueError("В конфиге не задан phone.")
    await start_telegram_client(client, phone, login_code_file)
    items: list[dict[str, Any]] = []
    lim = max(10, min(2000, int(dialog_limit)))
    async for dialog in client.iter_dialogs(limit=lim):
        if not (dialog.is_group or dialog.is_channel):
            continue
        ent = dialog.entity
        title = dialog.name or ""
        username = getattr(ent, "username", None)
        chat_id = getattr(ent, "id", None)
        if chat_id is None:
            continue
        cid_str = str(chat_id).strip()
        if cid_str.startswith("-100"):
            merge_key = cid_str
        elif cid_str.lstrip("-").isdigit():
            merge_key = f"-100{cid_str}" if not cid_str.startswith("-") else cid_str
        else:
            merge_key = cid_str
        if username:
            ref = f"@{username}"
        else:
            resolved_id = f"-100{chat_id}" if not str(chat_id).startswith("-100") else str(chat_id)
            ref = resolved_id
        is_broadcast = bool(getattr(ent, "broadcast", False))
        is_megagroup = bool(getattr(ent, "megagroup", False))
        items.append(
            {
                "ref": ref,
                "merge_key": merge_key,
                "title": title,
                "is_broadcast": is_broadcast,
                "is_megagroup": is_megagroup,
            }
        )
    await client.disconnect()
    print(
        json.dumps(
            {"ok": True, "items": items, "count": len(items)},
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )


async def leave_chats_from_dialogs_json(
    config: Dict[str, Any], refs: list[str], login_code_file: Path | None
) -> dict[str, Any]:
    """Выйти из групп/каналов по ref (не трогает личные диалоги с пользователями)."""
    api_id, api_hash = require_telegram_api_credentials(config)
    client = TelegramClient(
        config["session_name"],
        api_id,
        api_hash,
    )
    phone = str(config.get("phone") or "").strip()
    if not phone:
        raise ValueError("В конфиге не задан phone.")
    await start_telegram_client(client, phone, login_code_file)
    results: list[dict[str, Any]] = []
    try:
        for r in refs:
            s = str(r).strip()
            if not s:
                continue
            try:
                ent = await client.get_entity(s)
                if isinstance(ent, User):
                    results.append(
                        {
                            "ref": s,
                            "ok": False,
                            "error": "Пропуск: пользователь, не группа/канал.",
                        }
                    )
                    continue
                await client.delete_dialog(ent, revoke=False)
                results.append({"ref": s, "ok": True})
            except Exception as exc:  # noqa: BLE001
                results.append({"ref": s, "ok": False, "error": str(exc)})
        return {
            "results": results,
            "ok_count": sum(1 for x in results if x.get("ok")),
        }
    finally:
        await client.disconnect()


async def list_folders(config: Dict[str, Any], *, login_code_file: Path | None = None) -> None:
    api_id, api_hash = require_telegram_api_credentials(config)
    client = TelegramClient(config["session_name"], api_id, api_hash)
    phone = str(config.get("phone") or "").strip()
    if not phone:
        raise ValueError("В конфиге не задан phone.")
    await start_telegram_client(client, phone, login_code_file)
    filters_result = await client(functions.messages.GetDialogFiltersRequest())
    filters = getattr(filters_result, "filters", filters_result)

    print("\n=== Telegram folders ===")
    for filt in filters:
        if isinstance(filt, DialogFilter):
            print(f"id={filt.id} | title={filt.title}")
        elif hasattr(filt, "id") and hasattr(filt, "title"):
            # Some Telethon builds expose other dialog filter variants.
            print(f"id={filt.id} | title={filt.title} | type={type(filt).__name__}")
    await client.disconnect()


async def import_from_folder(
    config: Dict[str, Any], folder_query: str, apply: bool, config_path: Path, *, login_code_file: Path | None = None
) -> None:
    api_id, api_hash = require_telegram_api_credentials(config)
    client = TelegramClient(config["session_name"], api_id, api_hash)
    phone = str(config.get("phone") or "").strip()
    if not phone:
        raise ValueError("В конфиге не задан phone.")
    await start_telegram_client(client, phone, login_code_file)
    filters_result = await client(functions.messages.GetDialogFiltersRequest())
    filters = getattr(filters_result, "filters", filters_result)

    selected: Any = None
    query_lower = folder_query.strip().lower()
    for filt in filters:
        if not hasattr(filt, "id") or not hasattr(filt, "title"):
            continue
        if query_lower.isdigit() and int(query_lower) == int(filt.id):
            selected = filt
            break
        title = str(filt.title).lower()
        if query_lower in title:
            selected = filt
            break

    if not selected:
        print(f"Folder not found: {folder_query}")
        print("Tip: run with --list-folders to see available folder titles.")
        await client.disconnect()
        return

    resolved_items: list[Any] = []
    print(f"\n=== Folder chats: {selected.title} (id={selected.id}) ===")
    included_peers = getattr(selected, "include_peers", None) or []
    if included_peers:
        for peer in included_peers:
            try:
                entity = await client.get_entity(peer)
                username = getattr(entity, "username", None)
                title = getattr(entity, "title", None) or getattr(entity, "first_name", "") or ""
                if username:
                    value = f"@{username}"
                else:
                    ent_id = getattr(entity, "id", None)
                    if ent_id is None:
                        continue
                    # For channels/groups use -100 prefix representation.
                    value = int(f"-100{ent_id}")
                resolved_items.append(value)
                print(f"{value} | {title}")
            except Exception as exc:  # noqa: BLE001
                logger.warning("Cannot resolve folder peer: %s", exc)
    else:
        # Fallback for rule-based folders: collect dialogs assigned to folder_id.
        folder_id = int(selected.id)
        async for dialog in client.iter_dialogs(limit=500):
            if getattr(dialog, "folder_id", None) != folder_id:
                continue
            ent = dialog.entity
            username = getattr(ent, "username", None)
            title = dialog.name or getattr(ent, "title", "") or ""
            if username:
                value = f"@{username}"
            else:
                ent_id = getattr(ent, "id", None)
                if ent_id is None:
                    continue
                if dialog.is_group or dialog.is_channel:
                    value = int(f"-100{ent_id}")
                else:
                    value = int(ent_id)
            resolved_items.append(value)
            print(f"{value} | {title}")

    if not resolved_items:
        print("No resolvable chats found in this folder.")
        await client.disconnect()
        return

    if apply:
        raw_text = config_path.read_text(encoding="utf-8")
        cfg = json.loads(raw_text)
        validate_config(cfg)
        existing = LeadGenBot._normalize_target_chats(list(cfg.get("target_chats") or []))
        existing_set = {str(x) for x in existing}
        added = 0
        for item in resolved_items:
            if str(item) not in existing_set:
                existing.append(item)
                existing_set.add(str(item))
                added += 1
        cfg["target_chats"] = existing
        config_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nAdded {added} new chats to {config_path} target_chats.")
    else:
        print("\nPreview mode. Add --apply-import to save into config.json.")

    await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        print("\n[INFO] Bot stopped by user")
    except Exception:  # noqa: BLE001
        logger.exception("Fatal error during bot run")
        raise
