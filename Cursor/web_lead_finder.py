"""Лидоген по веб-сайтам.

Состав:
- ``serpapi_search_domains`` — поисковая выдача через SerpAPI (Google), возвращает
  уникальные домены и их URL/заголовки.
- ``discover_site_contacts`` — выкачивает главную и типичные «контактные» страницы
  одного домена и собирает контакты (email, телефон, t.me, wa.me, vk.com и т.п.).
- ``extract_contacts_from_html`` — низкоуровневый парсер HTML.

Все функции защищены таймаутами, лимитом на размер ответа и не падают на сетевых
ошибках — возвращают частичный результат + поле ``error``. Используются только
``httpx`` и стандартная библиотека.
"""

from __future__ import annotations

import html as _html
import re
import socket
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit

import httpx

DEFAULT_TIMEOUT_SEC = 12.0
MAX_RESPONSE_BYTES = 1_500_000  # 1.5 MB на страницу — больше парсить смысла нет
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; LeadgenBot/1.0; +https://github.com/) "
    "Like Gecko"
)

CONTACT_PATHS = (
    "/",
    "/contacts",
    "/contact",
    "/contact-us",
    "/kontakty",
    "/about",
    "/o-nas",
    "/about-us",
    "/help/contacts",
)

EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,24}\b"
)
PHONE_RE = re.compile(
    r"(?:\+?\d[\s\-().]?){7,18}\d"
)
TG_RE = re.compile(
    r"(?:https?://)?t\.me/(?:joinchat/|\+)?([A-Za-z0-9_]{3,64})",
    re.IGNORECASE,
)
WA_RE = re.compile(r"(?:https?://)?wa\.me/(\+?\d{6,16})", re.IGNORECASE)
VK_RE = re.compile(
    r"(?:https?://)?(?:m\.|www\.)?vk\.com/([A-Za-z0-9_.]{2,64})",
    re.IGNORECASE,
)

# Не считаем «контактом» очевидные служебные адреса/треш.
_EMAIL_BLOCKLIST_PARTS = (
    "noreply@",
    "no-reply@",
    "donotreply@",
    "example.com",
    "@sentry.io",
    "@wixpress.com",
    "@tilda.cc",
)
_PHONE_GARBAGE_RE = re.compile(r"[^\d+]+")


@dataclass
class SiteContacts:
    domain: str
    url: str = ""
    title: str = ""
    http_status: int = 0
    emails: list[str] = field(default_factory=list)
    phones: list[str] = field(default_factory=list)
    telegrams: list[str] = field(default_factory=list)
    whatsapps: list[str] = field(default_factory=list)
    vks: list[str] = field(default_factory=list)
    pages_visited: list[str] = field(default_factory=list)
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "url": self.url,
            "title": self.title,
            "http_status": self.http_status,
            "emails": self.emails,
            "phones": self.phones,
            "telegrams": self.telegrams,
            "whatsapps": self.whatsapps,
            "vks": self.vks,
            "pages_visited": self.pages_visited,
            "error": self.error,
        }

    @property
    def is_useful(self) -> bool:
        return bool(self.emails or self.phones or self.telegrams or self.whatsapps or self.vks)


def normalize_domain(value: str) -> str:
    """`https://www.Example.COM/path` → `example.com`. Пустые / IP / некорректные → ''."""
    s = (value or "").strip().lower()
    if not s:
        return ""
    if "://" not in s:
        s = "http://" + s
    try:
        sp = urlsplit(s)
    except ValueError:
        return ""
    host = (sp.hostname or "").strip(".").lower()
    if not host:
        return ""
    if host.startswith("www."):
        host = host[4:]
    if not re.match(r"^[a-z0-9.\-]+\.[a-z]{2,}$", host):
        return ""
    return host


def _is_private_host(host: str) -> bool:
    """Защита от SSRF: запрещаем localhost / приватные сети / link-local."""
    h = (host or "").strip().lower()
    if not h:
        return True
    if h in ("localhost", "127.0.0.1", "0.0.0.0", "::1"):
        return True
    try:
        infos = socket.getaddrinfo(h, None)
    except OSError:
        return True
    for info in infos:
        ip = info[4][0]
        if ip.startswith(("10.", "127.", "169.254.", "192.168.", "::1")):
            return True
        if ip.startswith("172."):
            try:
                second = int(ip.split(".")[1])
                if 16 <= second <= 31:
                    return True
            except (ValueError, IndexError):
                continue
    return False


def _safe_fetch(
    client: httpx.Client, url: str, *, timeout: float = DEFAULT_TIMEOUT_SEC
) -> tuple[int, str, str]:
    """Возвращает (http_status, final_url, html_text). При ошибке status=0."""
    sp = urlsplit(url)
    if sp.scheme not in ("http", "https"):
        return 0, url, ""
    if _is_private_host(sp.hostname or ""):
        return 0, url, ""
    try:
        with client.stream("GET", url, timeout=timeout, follow_redirects=True) as r:
            ctype = (r.headers.get("content-type") or "").lower()
            if "text/html" not in ctype and "application/xhtml" not in ctype:
                return r.status_code, str(r.url), ""
            chunks: list[bytes] = []
            received = 0
            for chunk in r.iter_bytes():
                received += len(chunk)
                if received > MAX_RESPONSE_BYTES:
                    break
                chunks.append(chunk)
            data = b"".join(chunks)
            try:
                text = data.decode(r.encoding or "utf-8", errors="replace")
            except (LookupError, UnicodeDecodeError):
                text = data.decode("utf-8", errors="replace")
            return r.status_code, str(r.url), text
    except httpx.HTTPError:
        return 0, url, ""
    except (OSError, ValueError):
        return 0, url, ""


def _decode_entities(s: str) -> str:
    try:
        return _html.unescape(s)
    except (TypeError, ValueError):
        return s


def _normalize_phone(raw: str) -> str:
    digits = _PHONE_GARBAGE_RE.sub("", raw)
    if not digits:
        return ""
    if digits.startswith("+"):
        plus = "+"
        digits = digits[1:]
    else:
        plus = ""
    if len(digits) < 7 or len(digits) > 16:
        return ""
    return plus + digits


def _is_blocked_email(email: str) -> bool:
    e = email.lower()
    return any(part in e for part in _EMAIL_BLOCKLIST_PARTS)


def _dedupe_keep_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        v = (it or "").strip()
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def extract_contacts_from_html(html_text: str, base_url: str = "") -> dict[str, list[str]]:
    """Извлекает контакты со страницы. Возвращает словарь со списками (порядок сохраняется)."""
    if not html_text:
        return {"emails": [], "phones": [], "telegrams": [], "whatsapps": [], "vks": [], "title": ""}
    decoded = _decode_entities(html_text)

    # mailto:/tel: имеют приоритет — это явная декларация контакта.
    mailtos = re.findall(r'mailto:([^"\'<>\s?]+)', decoded, re.IGNORECASE)
    tels = re.findall(r'tel:([^"\'<>\s]+)', decoded, re.IGNORECASE)

    # Удаляем теги <script>/<style>, остальное — текст для поиска.
    body = re.sub(r"<script\b[\s\S]*?</script>", " ", decoded, flags=re.IGNORECASE)
    body = re.sub(r"<style\b[\s\S]*?</style>", " ", body, flags=re.IGNORECASE)

    emails_raw = list(mailtos) + EMAIL_RE.findall(body)
    emails = [e.strip().rstrip(".,;:") for e in emails_raw if not _is_blocked_email(e)]

    phones_raw = list(tels) + PHONE_RE.findall(body)
    phones = [p for p in (_normalize_phone(s) for s in phones_raw) if p]

    tg = [m.lower() for m in TG_RE.findall(decoded) if m and m.lower() not in ("share", "iv")]
    wa = [m for m in WA_RE.findall(decoded)]
    vk = [m.lower() for m in VK_RE.findall(decoded) if m and not m.startswith("share")]

    title_match = re.search(r"<title[^>]*>([^<]{1,200})</title>", decoded, re.IGNORECASE)
    title = (title_match.group(1).strip() if title_match else "")[:200]

    return {
        "emails": _dedupe_keep_order(emails),
        "phones": _dedupe_keep_order(phones),
        "telegrams": _dedupe_keep_order(tg),
        "whatsapps": _dedupe_keep_order(wa),
        "vks": _dedupe_keep_order(vk),
        "title": title,
    }


def discover_site_contacts(
    domain_or_url: str,
    *,
    max_pages: int = 4,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
    user_agent: str = DEFAULT_USER_AGENT,
) -> SiteContacts:
    """Парсит домен: главная + 3 «контактные» страницы. Возвращает агрегат контактов.

    Параметр ``max_pages`` ограничивает суммарное число запросов на один домен.
    """
    domain = normalize_domain(domain_or_url)
    out = SiteContacts(domain=domain)
    if not domain:
        out.error = "invalid_domain"
        return out

    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5",
        "Accept-Language": "ru,en;q=0.7",
    }
    seen_urls: set[str] = set()
    visited: list[str] = []

    with httpx.Client(headers=headers, http2=False) as client:
        # 1) Главная — пробуем https, затем http.
        first_html = ""
        for scheme in ("https", "http"):
            url = f"{scheme}://{domain}/"
            status, final_url, html_text = _safe_fetch(client, url, timeout=timeout_sec)
            if status:
                out.http_status = status
                out.url = final_url
                visited.append(final_url)
                first_html = html_text
                seen_urls.add(_canonical_url(final_url))
                break
        if not first_html and out.http_status == 0:
            out.error = "unreachable"
            return out

        contacts = extract_contacts_from_html(first_html, out.url)
        if contacts["title"]:
            out.title = contacts["title"]

        # 2) Доп. контактные страницы: фиксированный список + ссылки c самой страницы,
        #    содержащие «contact»/«контакт».
        candidates = _candidate_contact_urls(out.url or f"https://{domain}/", first_html)
        for cu in candidates:
            if len(visited) >= max_pages:
                break
            ck = _canonical_url(cu)
            if ck in seen_urls:
                continue
            seen_urls.add(ck)
            status, final_url, html_text = _safe_fetch(client, cu, timeout=timeout_sec)
            if not status or not html_text:
                continue
            visited.append(final_url)
            extra = extract_contacts_from_html(html_text, final_url)
            for k in ("emails", "phones", "telegrams", "whatsapps", "vks"):
                contacts[k] = _dedupe_keep_order((contacts.get(k) or []) + extra[k])

    out.emails = contacts["emails"]
    out.phones = contacts["phones"]
    out.telegrams = contacts["telegrams"]
    out.whatsapps = contacts["whatsapps"]
    out.vks = contacts["vks"]
    out.pages_visited = visited
    return out


def _canonical_url(url: str) -> str:
    try:
        sp = urlsplit(url)
    except ValueError:
        return url
    return urlunsplit((sp.scheme.lower(), sp.netloc.lower(), sp.path or "/", "", ""))


def _candidate_contact_urls(base_url: str, html_text: str) -> list[str]:
    """Берём фиксированные пути + ссылки с подстрокой 'contact'/'контакт' (top-3)."""
    out: list[str] = []
    seen: set[str] = set()
    for path in CONTACT_PATHS:
        u = urljoin(base_url, path)
        ck = _canonical_url(u)
        if ck not in seen:
            seen.add(ck)
            out.append(u)
    if not html_text:
        return out
    # ищем <a href="...contact..."> или с русским «контакт»
    found = re.findall(
        r'<a[^>]+href=["\']([^"\']{1,300}(?:contact|kontakt|контакт)[^"\']{0,200})["\']',
        html_text,
        re.IGNORECASE,
    )
    for href in found[:6]:
        absu = urljoin(base_url, _decode_entities(unquote(href)))
        ck = _canonical_url(absu)
        if ck in seen:
            continue
        seen.add(ck)
        out.append(absu)
        if len(out) >= 8:
            break
    return out


def serpapi_search_domains(
    query: str,
    *,
    api_key: str,
    num: int = 20,
    gl: str = "ru",
    hl: str = "ru",
    timeout_sec: float = 20.0,
) -> dict[str, Any]:
    """Запрос в SerpAPI Google. Возвращает {ok, items:[{url,domain,title,snippet}], error}.

    `num` ограничен 1..50. Если ключ пустой — возвращаем `ok=False, error='no_serpapi_key'`.
    """
    q = (query or "").strip()
    if not q:
        return {"ok": False, "error": "empty_query", "items": []}
    if not (api_key or "").strip():
        return {"ok": False, "error": "no_serpapi_key", "items": []}
    n = max(1, min(50, int(num) if num else 20))
    params = {
        "engine": "google",
        "q": q,
        "num": n,
        "gl": (gl or "ru").lower()[:5],
        "hl": (hl or "ru").lower()[:5],
        "api_key": api_key.strip(),
        "no_cache": "true",
    }
    try:
        with httpx.Client(timeout=timeout_sec, headers={"User-Agent": DEFAULT_USER_AGENT}) as client:
            r = client.get("https://serpapi.com/search.json", params=params)
            if r.status_code == 401:
                return {"ok": False, "error": "unauthorized", "items": []}
            if r.status_code == 429:
                return {"ok": False, "error": "rate_limited", "items": []}
            if r.status_code >= 400:
                return {"ok": False, "error": f"http_{r.status_code}", "items": []}
            data = r.json()
    except httpx.HTTPError as exc:
        return {"ok": False, "error": f"http_error: {exc}", "items": []}
    except ValueError:
        return {"ok": False, "error": "bad_json", "items": []}

    items: list[dict[str, Any]] = []
    seen_domains: set[str] = set()
    for entry in data.get("organic_results", []) or []:
        url = str(entry.get("link") or "").strip()
        if not url:
            continue
        domain = normalize_domain(url)
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)
        items.append(
            {
                "url": url,
                "domain": domain,
                "title": str(entry.get("title") or "")[:300],
                "snippet": str(entry.get("snippet") or "")[:500],
            }
        )
    return {"ok": True, "items": items, "error": ""}


__all__ = [
    "DEFAULT_TIMEOUT_SEC",
    "SiteContacts",
    "discover_site_contacts",
    "extract_contacts_from_html",
    "normalize_domain",
    "serpapi_search_domains",
]
