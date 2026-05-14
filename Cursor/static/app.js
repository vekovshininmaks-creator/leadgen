/** @type {string | null} */
let leadgenCsrf = null;
/** @type {Promise<string> | null} */
let _csrfFetch = null;

/** @type {any[]} */
let adminUsersCache = [];
/** @type {any[]} */
let adminOrgsCache = [];

let chatsLlmFormLoaded = false;

/** @type {number | null} */
let currentUserId = null;

const THEME_KEY = "leadgen_theme";

/**
 * Clipboard API is only available in secure contexts (HTTPS or localhost).
 * Opening the app as http://<IP>/ leaves navigator.clipboard undefined.
 * @param {string} text
 */
async function copyTextToClipboard(text) {
  if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
    await navigator.clipboard.writeText(text);
    return;
  }
  const ta = document.createElement("textarea");
  ta.value = text;
  ta.setAttribute("readonly", "");
  ta.style.position = "fixed";
  ta.style.left = "-9999px";
  ta.style.top = "0";
  document.body.appendChild(ta);
  ta.focus();
  ta.select();
  ta.setSelectionRange(0, text.length);
  try {
    const ok = document.execCommand("copy");
    if (!ok) throw new Error("execCommand('copy') вернул false");
  } finally {
    document.body.removeChild(ta);
  }
}

function getCurrentTheme() {
  const t = document.documentElement.getAttribute("data-theme");
  return t === "light" || t === "dark" ? t : "dark";
}

function setThemeToggleUi(theme) {
  const btn = document.getElementById("themeToggleBtn");
  if (!btn) return;
  const isDark = theme === "dark";
  btn.setAttribute("aria-pressed", isDark ? "true" : "false");
  btn.setAttribute("aria-label", isDark ? "Переключить на светлую тему" : "Переключить на тёмную тему");
  btn.setAttribute("title", isDark ? "Светлая тема" : "Тёмная тема");
  btn.textContent = isDark ? "☀" : "🌙";
}

function closeMobileSidebar() {
  const s = document.getElementById("sidebar");
  const b = document.getElementById("sidebarBackdrop");
  if (s) s.classList.remove("sidebar--open");
  if (b) {
    b.classList.remove("is-visible");
    b.setAttribute("aria-hidden", "true");
  }
  document.body.classList.remove("sidebar-open");
}

function openMobileSidebar() {
  const s = document.getElementById("sidebar");
  const b = document.getElementById("sidebarBackdrop");
  if (s) s.classList.add("sidebar--open");
  if (b) {
    b.classList.add("is-visible");
    b.setAttribute("aria-hidden", "false");
  }
  document.body.classList.add("sidebar-open");
}

function bindThemeAndSidebar() {
  const themeBtn = document.getElementById("themeToggleBtn");
  if (themeBtn) {
    setThemeToggleUi(getCurrentTheme());
    themeBtn.addEventListener("click", () => {
      const cur = getCurrentTheme();
      const next = cur === "light" ? "dark" : "light";
      document.documentElement.setAttribute("data-theme", next);
      try {
        localStorage.setItem(THEME_KEY, next);
      } catch {
        /* ignore */
      }
      setThemeToggleUi(next);
      afterThemeApplied();
    });
  }

  const openBtn = document.getElementById("sidebarOpenBtn");
  const backdrop = document.getElementById("sidebarBackdrop");
  if (openBtn) {
    openBtn.addEventListener("click", () => {
      if (document.getElementById("sidebar")?.classList.contains("sidebar--open")) {
        closeMobileSidebar();
      } else {
        openMobileSidebar();
      }
    });
  }
  if (backdrop) {
    backdrop.addEventListener("click", () => {
      closeMobileSidebar();
    });
  }

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      if (e.defaultPrevented) return;
      const adPanel = document.getElementById("adminOrgDebugPanel");
      if (adPanel && !adPanel.classList.contains("hidden")) {
        closeAdminOrgDebugPanel();
        return;
      }
      const p = document.getElementById("leadgenAssistantPanel");
      if (p && !p.classList.contains("hidden")) {
        p.classList.add("hidden");
        const f = document.getElementById("leadgenAssistantFab");
        if (f) f.setAttribute("aria-expanded", "false");
        try {
          sessionStorage.setItem("leadgen_assistant_open", "0");
        } catch {
          /* ignore */
        }
        return;
      }
      closeMobileSidebar();
    }
  });

  let resizeT = 0;
  window.addEventListener("resize", () => {
    window.clearTimeout(resizeT);
    resizeT = window.setTimeout(() => {
      if (window.innerWidth > 1100) closeMobileSidebar();
    }, 150);
  });
}

function authHeaders() {
  // Авторизация — через HttpOnly cookie auth_token, выставляемую сервером.
  // X-Auth-Token больше не отправляем: localStorage уязвим к XSS.
  return {};
}

async function ensureCsrf() {
  if (leadgenCsrf) return leadgenCsrf;
  if (_csrfFetch) return _csrfFetch;
  _csrfFetch = (async () => {
    const r = await fetch("/api/auth/csrf");
    const d = await r.json();
    if (!d.csrf_token) throw new Error("Не удалось получить CSRF");
    leadgenCsrf = d.csrf_token;
    return leadgenCsrf;
  })().finally(() => {
    _csrfFetch = null;
  });
  return _csrfFetch;
}

/** Максимально подробный UI debug-лог (клики, запросы, ответы). */
const UI_DEBUG_MAX_LINES = 5000;
/** Старый общий ключ до разбиения по пользователям — переносится один раз в ключ текущего пользователя. */
const UI_DEBUG_LEGACY_LINES_KEY = "leadgen_ui_debug_lines_v1";
const _UI_DEBUG_ENABLED_KEY = "leadgen_ui_debug_enabled_v1";
const UI_DEBUG_PANEL_OPEN_KEY = "leadgen_ui_debug_panel_open";
window.__uiDebugLines = window.__uiDebugLines || [];

function _uiDebugLinesStorageKey() {
  if (currentUserId != null && Number.isFinite(Number(currentUserId))) {
    return `leadgen_ui_debug_lines_u_${currentUserId}`;
  }
  return "leadgen_ui_debug_lines_u_anon";
}

function _uiDebugEnabled() {
  try {
    const v = localStorage.getItem(_UI_DEBUG_ENABLED_KEY);
    if (v == null) return true;
    return v === "1" || v === "true" || v === "on";
  } catch {
    return true;
  }
}

function _uiDebugSetEnabled(on) {
  try {
    localStorage.setItem(_UI_DEBUG_ENABLED_KEY, on ? "1" : "0");
  } catch {
    /* ignore */
  }
}

function _redactSensitiveText(s) {
  const t = String(s || "");
  // грубая маскировка: пароли/ключи/токены
  return t
    .replace(/("api_key"\s*:\s*")([^"]+)(")/gi, '$1••••••••$3')
    .replace(/("password"\s*:\s*")([^"]+)(")/gi, '$1••••••••$3')
    .replace(/(authorization:\s*bearer\s+)([a-z0-9._-]+)/gi, "$1••••••••")
    .replace(/("csrf"\s*:\s*")([^"]+)(")/gi, '$1••••••••$3')
    .replace(/("csrf_token"\s*:\s*")([^"]+)(")/gi, '$1••••••••$3')
    .replace(/("token"\s*:\s*")([^"]+)(")/gi, '$1••••••••$3');
}

function _redactJsonLike(body) {
  try {
    if (body == null) return "";
    if (typeof body !== "string") return _redactSensitiveText(JSON.stringify(body));
    const s = body.trim();
    if (!s) return "";
    if (s.startsWith("{") || s.startsWith("[")) {
      return _redactSensitiveText(s);
    }
    return _redactSensitiveText(s);
  } catch {
    return "";
  }
}

function _trimUiDebugLines() {
  const max = UI_DEBUG_MAX_LINES;
  if (window.__uiDebugLines.length > max) {
    window.__uiDebugLines.splice(0, window.__uiDebugLines.length - max);
  }
}

/** Удалить все сохранённые буферы UI-debug из localStorage (не трогает флаг «Вкл» и открыта ли панель). */
function _uiDebugWipeStoredLineKeys() {
  try {
    const kill = [];
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i);
      if (!k) continue;
      if (k === UI_DEBUG_LEGACY_LINES_KEY || k.startsWith("leadgen_ui_debug_lines")) kill.push(k);
    }
    kill.forEach((k) => localStorage.removeItem(k));
  } catch {
    /* ignore */
  }
}

function _uiDebugPersist() {
  try {
    /* До известного user_id ключ был бы …u_anon — восстановление потом тянет …u_<id>, получается двойной буфер и «очистить» не вычищает anon. */
    if (currentUserId == null) return;
    const key = _uiDebugLinesStorageKey();
    localStorage.setItem(key, JSON.stringify(window.__uiDebugLines.slice(-UI_DEBUG_MAX_LINES)));
  } catch {
    /* ignore */
  }
}

function _uiDebugRestoreFromKey(key) {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) {
      window.__uiDebugLines = [];
      return;
    }
    const arr = JSON.parse(raw);
    window.__uiDebugLines = Array.isArray(arr) ? arr.map((x) => String(x)) : [];
  } catch {
    window.__uiDebugLines = [];
  }
}

/** После /api/auth/me: подгрузить лог для текущего user id и сохранить строки, накопленные до ответа. */
function _uiDebugRestoreForCurrentUser() {
  try {
    const pending = window.__uiDebugLines.slice();
    _uiDebugMigrateLegacyOnce();
    _uiDebugRestoreFromKey(_uiDebugLinesStorageKey());
    window.__uiDebugLines = window.__uiDebugLines.concat(pending);
    _trimUiDebugLines();
    _uiDebugPersist();
    _uiDebugRenderPre();
  } catch {
    /* ignore */
  }
}

function _uiDebugMigrateLegacyOnce() {
  try {
    const legacyRaw = localStorage.getItem(UI_DEBUG_LEGACY_LINES_KEY);
    if (!legacyRaw) return;
    const targetKey = _uiDebugLinesStorageKey();
    let takeLegacy = true;
    try {
      const cur = localStorage.getItem(targetKey);
      if (cur) {
        const arr = JSON.parse(cur);
        if (Array.isArray(arr) && arr.length > 0) takeLegacy = false;
      }
    } catch {
      /* ignore */
    }
    if (takeLegacy) localStorage.setItem(targetKey, legacyRaw);
    localStorage.removeItem(UI_DEBUG_LEGACY_LINES_KEY);
  } catch {
    /* ignore */
  }
}

function _uiDebugRenderPre() {
  const text = window.__uiDebugLines.join("\n");
  const pre = document.getElementById("uiDebugPre");
  const mirror = document.getElementById("botPageUiDebugMirror");
  if (mirror) mirror.textContent = text;
  const el = pre || mirror;
  if (!el) return;
  const panel = document.getElementById("uiDebugPanel");
  const panelOpen = !panel || !panel.classList.contains("hidden");
  const prevHeight = el.scrollHeight;
  const prevTop = el.scrollTop;
  const clientH = el.clientHeight;
  const nearBottom = prevHeight <= clientH + 12 || prevHeight - prevTop - clientH < 72;
  el.textContent = text;
  if (panelOpen && nearBottom) {
    el.scrollTop = el.scrollHeight;
  } else if (panelOpen) {
    const dh = el.scrollHeight - prevHeight;
    el.scrollTop = Math.max(0, prevTop + dh);
  }
}

function uiDebug(line) {
  try {
    if (!_uiDebugEnabled()) return;
    const ts = new Date().toISOString();
    const s = _redactSensitiveText(`${ts} | ${String(line || "").trim()}`);
    window.__uiDebugLines.push(s);
    _trimUiDebugLines();
    _uiDebugRenderPre();
    _uiDebugPersist();
  } catch {
    /* ignore */
  }
}

function exportUiDebugPlainText() {
  return window.__uiDebugLines.join("\n");
}

function exportUiDebugBundle() {
  let appJs = "";
  try {
    const scripts = Array.from(document.querySelectorAll("script[src]"));
    const s = scripts.map((x) => x.getAttribute("src") || "").find((x) => x.includes("/static/app.js"));
    appJs = s || "";
  } catch {
    appJs = "";
  }
  const page = document.body?.dataset?.page || null;
  const meta = {
    captured_at: new Date().toISOString(),
    page,
    org_role: window.__leadgenOrgRole || null,
    org_id: window.__leadgenOrgId || null,
    user_agent: navigator.userAgent,
    href: location.href,
    origin: window.location.origin,
    app_js: appJs || null,
    health_status: document.getElementById("healthMeta")?.textContent || null,
    viewport: {
      w: window.innerWidth,
      h: window.innerHeight,
      dpr: window.devicePixelRatio,
    },
    locale: {
      language: navigator.language,
      languages: Array.isArray(navigator.languages) ? navigator.languages : null,
      tz: Intl.DateTimeFormat().resolvedOptions().timeZone || null,
    },
    network: (() => {
      try {
        const c = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
        if (!c) return null;
        return {
          effectiveType: c.effectiveType,
          downlink: c.downlink,
          rtt: c.rtt,
          saveData: c.saveData,
        };
      } catch {
        return null;
      }
    })(),
    server: (() => {
      try {
        const h = window.__lastHealthz || null;
        if (!h) return null;
        const j = h.json || null;
        return {
          healthz_fetched_at: h.fetched_at || null,
          healthz_date_header: h.date_header || null,
          time: j && j.time ? j.time : null,
          env: j && j.env ? j.env : null,
          app_version: j && j.app_version ? j.app_version : null,
          build: j && j.build ? j.build : null,
          fetch_error: h.error || null,
        };
      } catch {
        return null;
      }
    })(),
  };
  return JSON.stringify({ meta, lines: window.__uiDebugLines.slice(-UI_DEBUG_MAX_LINES) }, null, 2);
}

function _uiDebugDescribeElement(el) {
  if (!el || el.nodeType !== 1) return "?";
  const tag = el.tagName.toLowerCase();
  const id = el.id ? `#${el.id}` : "";
  const role = el.getAttribute("role") ? `[role=${el.getAttribute("role")}]` : "";
  let cls = "";
  if (typeof el.className === "string" && el.className.trim()) {
    cls = `.${el.className.trim().split(/\s+/).slice(0, 4).join(".")}`;
  }
  const nm = el.getAttribute("name") ? `[name=${el.getAttribute("name")}]` : "";
  const typ = el.getAttribute("type") ? `[type=${el.getAttribute("type")}]` : "";
  let href = "";
  if (tag === "a") {
    const h = el.getAttribute("href");
    if (h) href = ` href=${String(h).slice(0, 80)}`;
  }
  const txt = el.textContent ? String(el.textContent).trim().replace(/\s+/g, " ").slice(0, 72) : "";
  return `${tag}${id}${role}${nm}${typ}${cls}${href}${txt ? ` "${txt}"` : ""}`;
}

/** Дедуп строк bot.log → UI debug (окно хвоста на сервере «едет» вместе с файлом). */
const _UI_DBG_BOT_LOG_SEEN_MAX = 800;
/** @type {string[]} */
let _uiDbgBotLogSeenOrder = [];
/** @type {Set<string>} */
let _uiDbgBotLogSeenSet = new Set();

function uiDebugResetBotLogSeen() {
  _uiDbgBotLogSeenOrder = [];
  _uiDbgBotLogSeenSet = new Set();
}

/**
 * Новые строки из хвоста tenants/.../bot.log (leadgen_bot в терминале). Без обёртки api() — не плодить строки «API → …».
 * @param {string} logTailRaw
 */
function maybeUiDebugAppendBotLogTail(logTailRaw) {
  if (!_uiDebugEnabled()) return;
  const tail = typeof logTailRaw === "string" ? String(logTailRaw).replace(/\r\n/g, "\n").trimEnd() : "";
  if (!tail) return;
  const lines = tail.split("\n");
  for (const raw of lines) {
    const ln = String(raw || "").trimEnd();
    if (!ln) continue;
    if (_uiDbgBotLogSeenSet.has(ln)) continue;
    _uiDbgBotLogSeenSet.add(ln);
    _uiDbgBotLogSeenOrder.push(ln);
    while (_uiDbgBotLogSeenOrder.length > _UI_DBG_BOT_LOG_SEEN_MAX) {
      const drop = _uiDbgBotLogSeenOrder.shift();
      if (drop != null) _uiDbgBotLogSeenSet.delete(drop);
    }
    uiDebug(`bot.log | ${ln.slice(0, 1400)}`);
  }
}

/** @type {number | null} */
let _uiDbgBotLogPollTimer = null;

async function pollBotLogTailForUiDebugOnce() {
  if (!_uiDebugEnabled()) return;
  try {
    const r = await fetch("/api/bot/scan-log", { credentials: "same-origin" });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) return;
    const tail = typeof d.log_tail === "string" ? d.log_tail : "";
    maybeUiDebugAppendBotLogTail(tail);
  } catch {
    /* ignore */
  }
}

/** На странице бота хвост уже тянет refreshBotScanLog — не дублируем запросы. */
function ensureUiDebugBotLogTailPolling() {
  if (_uiDbgBotLogPollTimer != null) return;
  _uiDbgBotLogPollTimer = window.setInterval(() => {
    if (!_uiDebugEnabled()) return;
    if (document.getElementById("botScanLog")) return;
    pollBotLogTailForUiDebugOnce().catch(() => {});
  }, 4500);
  window.setTimeout(() => {
    if (!_uiDebugEnabled()) return;
    if (document.getElementById("botScanLog")) return;
    pollBotLogTailForUiDebugOnce().catch(() => {});
  }, 900);
}

function bindUiDebugPanel() {
  const fab = document.getElementById("uiDebugFab");
  const panel = document.getElementById("uiDebugPanel");
  const en = document.getElementById("uiDebugEnabled");
  const close = document.getElementById("uiDebugClose");
  const clr = document.getElementById("uiDebugClear");
  const cp = document.getElementById("uiDebugCopy");
  const cpTxt = document.getElementById("uiDebugCopyText");
  const dl = document.getElementById("uiDebugDownload");
  if (!fab || !panel) return;

  const render = () => {
    _uiDebugRenderPre();
    if (en) en.checked = _uiDebugEnabled();
  };
  render();

  const toggle = (open) => {
    const want = open != null ? Boolean(open) : panel.classList.contains("hidden");
    panel.classList.toggle("hidden", !want);
    fab.setAttribute("aria-expanded", want ? "true" : "false");
    try {
      sessionStorage.setItem(UI_DEBUG_PANEL_OPEN_KEY, want ? "1" : "0");
    } catch {
      /* ignore */
    }
    render();
  };
  try {
    if (sessionStorage.getItem(UI_DEBUG_PANEL_OPEN_KEY) === "1") {
      panel.classList.remove("hidden");
      fab.setAttribute("aria-expanded", "true");
      render();
    }
  } catch {
    /* ignore */
  }
  fab.addEventListener(
    "click",
    (e) => {
      e.preventDefault();
      e.stopPropagation();
      toggle();
    },
    true,
  );
  if (close) close.addEventListener("click", () => toggle(false));
  if (en) {
    en.addEventListener("change", () => {
      _uiDebugSetEnabled(Boolean(en.checked));
      uiDebug(`UI debug enabled=${Boolean(en.checked)}`);
      render();
    });
  }
  if (clr) {
    clr.addEventListener("click", () => {
      window.__uiDebugLines = [];
      uiDebugResetBotLogSeen();
      _uiDebugWipeStoredLineKeys();
      _uiDebugPersist();
      render();
    });
  }
  if (cp) {
    cp.addEventListener("click", async () => {
      try {
        await copyTextToClipboard(exportUiDebugBundle());
        uiDebug("UI debug copied to clipboard (JSON)");
      } catch (e) {
        uiDebug(`UI debug copy failed: ${e.message || e}`);
      }
    });
  }
  if (cpTxt) {
    cpTxt.addEventListener("click", async () => {
      try {
        await copyTextToClipboard(exportUiDebugPlainText());
        uiDebug("UI debug copied to clipboard (plain text)");
      } catch (e) {
        uiDebug(`UI debug plain copy failed: ${e.message || e}`);
      }
    });
  }
  if (dl) {
    dl.addEventListener("click", async () => {
      try {
        // Make the bundle self-contained: refresh /healthz right before export.
        try {
          await refreshHealth();
        } catch {
          /* ignore */
        }
        const blob = new Blob([exportUiDebugBundle()], { type: "application/json;charset=utf-8" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = `leadgen-ui-debug-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.json`;
        a.click();
        setTimeout(() => URL.revokeObjectURL(a.href), 2000);
        uiDebug("UI debug downloaded");
      } catch (e) {
        uiDebug(`UI debug download failed: ${e.message || e}`);
      }
    });
  }
  ensureUiDebugBotLogTailPolling();
}

function bindGlobalUiDebugHooks() {
  uiDebug("UI debug init");

  window.addEventListener(
    "pageshow",
    (ev) => {
      try {
        uiDebug(`pageshow persisted=${Boolean(ev && ev.persisted)} path=${location.pathname}`);
      } catch {
        /* ignore */
      }
    },
    false,
  );

  window.addEventListener("error", (ev) => {
    try {
      const msg = ev?.message || "window.error";
      const src = ev?.filename ? `${ev.filename}:${ev.lineno || 0}:${ev.colno || 0}` : "";
      uiDebug(`JS error: ${msg}${src ? " @ " + src : ""}`);
      if (ev?.error && ev.error.stack) uiDebug(`JS stack: ${String(ev.error.stack).slice(0, 1500)}`);
    } catch {
      /* ignore */
    }
  });
  window.addEventListener("unhandledrejection", (ev) => {
    try {
      const r = ev?.reason;
      const msg = r && r.message ? r.message : String(r || "unhandledrejection");
      uiDebug(`Promise rejection: ${msg}`);
      if (r && r.stack) uiDebug(`Promise stack: ${String(r.stack).slice(0, 1500)}`);
    } catch {
      /* ignore */
    }
  });

  // Любой click: цепочка предков (до 8 узлов), без шума от самой панели отладки
  document.addEventListener(
    "click",
    (ev) => {
      try {
        const t = ev.target;
        if (!t || typeof t !== "object") return;
        if (t.closest && t.closest("#uiDebugHost")) return;
        const chain = [];
        let cur = t.nodeType === 1 ? t : t.parentElement;
        for (let i = 0; i < 8 && cur && cur.nodeType === 1; i++, cur = cur.parentElement) {
          chain.push(_uiDebugDescribeElement(cur));
        }
        uiDebug(`click ${chain.join(" < ")}`);
      } catch {
        /* ignore */
      }
    },
    true,
  );

  /* Отдельная строка для переходов по пунктам меню (понятнее, чем только цепочка DOM). */
  document.addEventListener(
    "click",
    (ev) => {
      try {
        const raw = ev.target;
        if (!raw || typeof raw !== "object") return;
        if (raw.closest && raw.closest("#uiDebugHost")) return;
        const a = raw.closest && raw.closest("a[href]");
        if (!a || a.nodeType !== 1) return;
        const href = a.getAttribute("href");
        if (!href || href.startsWith("#") || href.toLowerCase().startsWith("javascript:")) return;
        let u;
        try {
          u = new URL(href, location.href);
        } catch {
          return;
        }
        if (u.origin !== location.origin) {
          uiDebug(`nav external → ${u.href.slice(0, 160)}`);
          return;
        }
        uiDebug(`nav menu → ${u.pathname}${u.search}${u.hash}`);
      } catch {
        /* ignore */
      }
    },
    true,
  );

  document.addEventListener(
    "submit",
    (ev) => {
      try {
        const f = ev.target;
        if (!f || f.nodeType !== 1 || f.tagName !== "FORM") return;
        if (f.closest && f.closest("#uiDebugHost")) return;
        const id = f.id ? `#${f.id}` : "";
        const act = f.getAttribute("action") || "";
        const method = (f.getAttribute("method") || "get").toUpperCase();
        uiDebug(`submit form${id} method=${method} action=${String(act).slice(0, 120)}`);
      } catch {
        /* ignore */
      }
    },
    true,
  );

  let _uiDbgChangeLast = 0;
  document.addEventListener(
    "change",
    (ev) => {
      try {
        const t = ev.target;
        if (!t || t.nodeType !== 1) return;
        if (t.closest && t.closest("#uiDebugHost")) return;
        const tag = t.tagName.toLowerCase();
        if (!["input", "select", "textarea"].includes(tag)) return;
        const now = Date.now();
        if (now - _uiDbgChangeLast < 350) return;
        _uiDbgChangeLast = now;
        const id = t.id ? `#${t.id}` : "";
        const nm = t.getAttribute("name") || "";
        uiDebug(`change ${tag}${id} name=${nm}`);
      } catch {
        /* ignore */
      }
    },
    true,
  );

  // Mirror console errors
  try {
    const origErr = console.error?.bind(console);
    const origWarn = console.warn?.bind(console);
    console.error = (...args) => {
      uiDebug(`console.error: ${_redactSensitiveText(args.map((a) => String(a)).join(" ").slice(0, 800))}`);
      if (origErr) origErr(...args);
    };
    console.warn = (...args) => {
      uiDebug(`console.warn: ${_redactSensitiveText(args.map((a) => String(a)).join(" ").slice(0, 800))}`);
      if (origWarn) origWarn(...args);
    };
  } catch {
    /* ignore */
  }
}

/** Одна строка после загрузки страницы (раздел каталога / URL). */
function uiDebugLogPageEntry() {
  try {
    const p = document.body?.dataset?.page || "?";
    const path = location.pathname || "";
    let from = "";
    try {
      if (document.referrer) {
        const r = new URL(document.referrer);
        from = r.origin === location.origin ? ` from=${r.pathname}` : ` ext-ref=${r.origin}`;
      }
    } catch {
      /* ignore */
    }
    uiDebug(`page enter data-page=${p} path=${path}${from}`);
  } catch {
    /* ignore */
  }
}

async function api(url, opts = {}) {
  const method = (opts.method || "GET").toUpperCase();
  const h = { ...(opts.headers || {}) };
  if (["POST", "PUT", "DELETE", "PATCH"].includes(method)) {
    h["X-CSRF-Token"] = await ensureCsrf();
  }
  const headers = { ...h };
  // credentials: 'same-origin' — гарантируем, что HttpOnly cookie auth_token уйдёт с запросом.
  const t0 = (typeof performance !== "undefined" && performance.now) ? performance.now() : Date.now();
  const bodyHint = opts && Object.prototype.hasOwnProperty.call(opts, "body") ? _redactJsonLike(opts.body) : "";
  uiDebug(`API → ${method} ${url}${bodyHint ? ` | body=${bodyHint.slice(0, 600)}` : ""}`);
  let r;
  try {
    r = await fetch(url, { ...opts, headers, credentials: "same-origin" });
  } catch (e) {
    uiDebug(`API ✖ сеть ${method} ${url}: ${e.message || e}`);
    throw e;
  }
  let d = {};
  try {
    d = await r.json().catch(() => ({}));
  } catch {
    d = {};
  }
  const t1 = (typeof performance !== "undefined" && performance.now) ? performance.now() : Date.now();
  uiDebug(`API ← ${method} ${url} · ${r.status} · ${Math.round(t1 - t0)}мс`);
  if (!r.ok) {
    const fallback =
      r.status === 403
        ? "Доступ запрещён (403). Возможно, у учётной записи недостаточно прав — войдите под администратором организации."
        : `API error ${r.status}`;
    const msg = (d && d.message) || fallback;
    if (d && d.stdout_tail) uiDebug(`API stdout_tail: ${String(d.stdout_tail).slice(-400)}`);
    uiDebug(`API ✖ ${method} ${url}: ${msg}`);
    throw new Error(msg);
  }
  if (d && d.message) uiDebug(`API message: ${String(d.message).slice(0, 400)}`);
  return d;
}

/** Скачивание файла (GET-выгрузка) — токен берётся из cookie. */
async function downloadAuthedGet(url, fallbackName) {
  const r = await fetch(url, { method: "GET", credentials: "same-origin" });
  if (!r.ok) {
    const d = await r.json().catch(() => ({}));
    throw new Error(d.message || "Ошибка загрузки");
  }
  const blob = await r.blob();
  let fn = fallbackName || "download";
  const cd = r.headers.get("Content-Disposition") || "";
  const m = /filename="?([^";\n]+)"?/i.exec(cd);
  if (m) fn = m[1].trim();
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = fn;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 2000);
}

const LEADS_TABLE_COLS = [
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
];

const LEAD_TAG_LABELS = {
  lead: "Лид",
  junk: "Шлак",
  in_progress: "В работе",
  wrote: "Написали",
  partner: "Партнёр",
};

const LEAD_TAG_SELECT_ORDER = ["lead", "junk", "in_progress", "wrote", "partner"];

const LEADS_COL_TITLE = {
  timestamp: "Написано",
  username: "User",
  user_id: "ID",
  source_chat: "Чат",
  message_id: "msg",
  message: "Текст",
  stage: "Этап",
  status: "Статус",
  matched_keyword: "Триггер",
  lead_tag: "Метка",
};

const SHORT_STAGE = {
  stage1: "1",
  stage2: "2",
  stage3: "3",
  dry_run: "тест",
  blacklist: "ЧС",
  unknown: "?",
};

const SHORT_STATUS = {
  queued: "очер.",
  pending_approval: "согл.",
  reply_not_qualified: "не квал.",
  reply_not_interested: "не инт.",
  reply_gate: "гейт",
  skip_daily_limit_reached: "лимит♦",
  skip_monthly_limit_reached: "мес.♦",
  user_rejected: "отказ",
  dry_run_detected_no_dm: "тест",
  dry_run_ignore_private: "тест◊",
};

/** @type {Set<string>} */
const leadsSelectedIds = new Set();

let leadsDataSignature = "";

function normalizeLeadTagKey(raw) {
  const t = String(raw || "").trim();
  return t || "lead";
}

function shortStageLabel(s) {
  const x = String(s || "").trim().toLowerCase();
  if (!x) return "—";
  if (SHORT_STAGE[x]) return SHORT_STAGE[x];
  const m = /^stage(\d+)$/.exec(x);
  if (m) return m[1];
  return x.length > 12 ? `${x.slice(0, 10)}…` : x;
}

function shortStatusLabel(s) {
  const x = String(s || "").trim();
  if (!x) return "—";
  if (SHORT_STATUS[x]) return SHORT_STATUS[x];
  if (x.length <= 14) return x;
  return `${x.slice(0, 12)}…`;
}

function formatLeadWrittenTime(s) {
  const t = String(s || "").trim();
  if (t.length >= 19) return t.slice(0, 19).replace("T", " ");
  if (t.length >= 16) return t.slice(0, 16).replace("T", " ");
  return t || "—";
}

function leadsColumnTitle(colKey) {
  const k = String(colKey || "");
  return LEADS_COL_TITLE[k] || k;
}

function buildLeadTagSelectHtml(rid, rawTag) {
  const cur = normalizeLeadTagKey(rawTag);
  const safeRid = escapeHtmlCell(rid);
  const opts = LEAD_TAG_SELECT_ORDER.map((v) => {
    const sel = v === cur ? " selected" : "";
    return `<option value="${escapeHtmlCell(v)}"${sel}>${escapeHtmlCell(LEAD_TAG_LABELS[v] || v)}</option>`;
  }).join("");
  return `<select class="leads-tag-select" data-lead-id="${safeRid}" aria-label="Метка лида">${opts}</select>`;
}

const LEADS_MSG_PREVIEW_LEN = 120;

/** Класс колонки для фиксированных ширин (без спецсимволов). */
function leadsTableColClass(colKey) {
  return `leads-col-${String(colKey || "").replace(/[^\w-]/g, "_")}`;
}

/** Публичная ссылка на сообщение: t.me/username/id или t.me/c/internal/id (чат — сегмент до |). */
function leadsTelegramMessageUrl(sourceChatRaw, messageIdRaw) {
  const mid = parseInt(String(messageIdRaw ?? "").trim(), 10);
  if (!Number.isFinite(mid) || mid < 1) return null;
  const raw = String(sourceChatRaw ?? "").trim();
  if (!raw || /^private$/i.test(raw.split("|")[0].trim())) return null;
  const first = raw.split("|")[0].trim();
  const m100 = first.match(/^-100(\d+)$/);
  if (m100) {
    return `https://t.me/c/${m100[1]}/${mid}`;
  }
  const u = first.replace(/^@+/, "").trim();
  if (/^[a-zA-Z][a-zA-Z0-9_]{3,}$/.test(u)) {
    return `https://t.me/${u}/${mid}`;
  }
  return null;
}

function formatLeadsMessageFoldHtml(text) {
  const t = String(text ?? "");
  const plain = escapeHtmlCell(t);
  if (t.length <= LEADS_MSG_PREVIEW_LEN) {
    return `<span class="leads-msg-plain">${plain}</span>`;
  }
  const snippet = escapeHtmlCell(`${t.slice(0, LEADS_MSG_PREVIEW_LEN).trimEnd()}…`);
  return `<details class="leads-msg-fold"><summary class="leads-msg-sum" title="Развернуть полный текст">${snippet}</summary><div class="leads-msg-body">${plain}</div></details>`;
}

/** @type {Record<string, string>[]} */
let leadsRowsCache = [];

let llmPresetsUiReady = false;

function escapeHtmlCell(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
const escapeHtml = escapeHtmlCell;

const LEADS_COL_WIDTHS_KEY = "leadgen_leads_col_widths_v2";

function loadLeadsColWidths(n) {
  try {
    const raw = localStorage.getItem(LEADS_COL_WIDTHS_KEY);
    if (!raw) return null;
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr) || arr.length !== n) return null;
    return arr.map((x) => (typeof x === "number" && Number.isFinite(x) ? x : null));
  } catch {
    return null;
  }
}

function saveLeadsColWidthsFromTable(table) {
  const cg = table.querySelector("colgroup");
  if (!cg) return;
  const arr = [];
  for (const col of cg.children) {
    const w = col.style.width;
    const m = w && String(w).match(/^(\d+(?:\.\d+)?)px$/);
    arr.push(m ? parseFloat(m[1]) : null);
  }
  try {
    localStorage.setItem(LEADS_COL_WIDTHS_KEY, JSON.stringify(arr));
  } catch {
    /* ignore */
  }
}

function attachLeadsColResize(table) {
  const thead = table.querySelector("thead");
  if (!thead) return;
  thead.querySelectorAll(".leads-col-resize-handle").forEach((handle) => {
    handle.addEventListener("mousedown", (downEv) => {
      downEv.preventDefault();
      downEv.stopPropagation();
      const idx = parseInt(handle.getAttribute("data-col-idx"), 10);
      const cg = table.querySelector("colgroup");
      if (!cg || !cg.children[idx]) return;
      const col = cg.children[idx];
      const rect = col.getBoundingClientRect();
      let startW = rect.width > 8 ? rect.width : 120;
      const sw = col.style.width;
      const mp = sw && String(sw).match(/^(\d+(?:\.\d+)?)px$/);
      if (mp) startW = parseFloat(mp[1]);
      const startX = downEv.clientX;
      const minW = idx === 0 ? 36 : 48;

      const move = (e) => {
        const dw = e.clientX - startX;
        const next = Math.max(minW, Math.round(startW + dw));
        col.style.width = `${next}px`;
      };
      const up = () => {
        document.removeEventListener("mousemove", move);
        document.removeEventListener("mouseup", up);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
        saveLeadsColWidthsFromTable(table);
      };
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
      document.addEventListener("mousemove", move);
      document.addEventListener("mouseup", up);
    });
  });
}

/** Разрешённые href для ячеек лидов (без произвольного URL из CSV). */
function leadsTrustedHref(href) {
  if (typeof href !== "string") return null;
  if (href.startsWith("https://t.me/")) return href;
  if (/^tg:\/\/user\?id=\d+$/.test(href)) return href;
  return null;
}

function leadsAnchorWeb(href, label) {
  const h = leadsTrustedHref(href);
  if (!h || !h.startsWith("https://")) return escapeHtmlCell(label);
  return `<a href="${escapeHtmlCell(h)}" target="_blank" rel="noopener noreferrer">${escapeHtmlCell(label)}</a>`;
}

function leadsAnchorTgUser(href, label) {
  const h = leadsTrustedHref(href);
  if (!h) return escapeHtmlCell(label);
  return `<a href="${escapeHtmlCell(h)}">${escapeHtmlCell(label)}</a>`;
}

/**
 * source_chat: -100…, @name, name, или -100…|…comments_for=@user
 * @param {string} raw
 */
function formatSourceChatLeadsHtml(raw) {
  const full = String(raw ?? "").trim();
  if (!full) return escapeHtmlCell(full);

  const prefixMatch = full.match(/^(-100\d+)([\s\S]*)$/);
  if (prefixMatch) {
    const bigId = prefixMatch[1];
    const rest = prefixMatch[2];
    const internal = bigId.slice(4);
    const hrefChat = `https://t.me/c/${internal}/1`;
    let html = leadsAnchorWeb(hrefChat, bigId);
    if (rest) {
      const userM = rest.match(/^([\s\S]*?)comments_for=@?([a-zA-Z][a-zA-Z0-9_]{3,31})([\s\S]*)$/i);
      if (userM) {
        const [, before, uname, after] = userM;
        const hrefUser = `https://t.me/${uname}`;
        html += `${escapeHtmlCell(before)}${leadsAnchorWeb(hrefUser, `@${uname}`)}${escapeHtmlCell(after)}`;
      } else {
        html += escapeHtmlCell(rest);
      }
    }
    return html;
  }

  const onlyComments = full.match(/^(comments_for=)@?([a-zA-Z][a-zA-Z0-9_]{3,31})\s*$/i);
  if (onlyComments) {
    return `${escapeHtmlCell(onlyComments[1])}${leadsAnchorWeb(`https://t.me/${onlyComments[2]}`, `@${onlyComments[2]}`)}`;
  }

  const t = full.replace(/^@+/, "").trim();
  if (/^[a-zA-Z][a-zA-Z0-9_]{3,}$/.test(t)) {
    return leadsAnchorWeb(`https://t.me/${t}`, full);
  }
  return escapeHtmlCell(full);
}

/**
 * HTML одной ячейки таблицы лидов (только доверенные ссылки t.me / tg://user?id=).
 * @param {string} colKey
 * @param {unknown} raw
 * @param {Record<string, string>} [row]
 */
function formatLeadsCellAsHtml(colKey, raw, row) {
  const s = String(raw ?? "");
  const k = String(colKey || "")
    .toLowerCase()
    .replace(/\s/g, "_");
  if (k === "username" || k === "lead_username") {
    const u = s.replace(/^@+/, "").trim();
    if (/^[a-zA-Z][a-zA-Z0-9_]{3,31}$/.test(u)) {
      const label = s.trim().startsWith("@") ? `@${u}` : u;
      return leadsAnchorWeb(`https://t.me/${u}`, label);
    }
    return escapeHtmlCell(s);
  }
  if (k === "user_id" || k === "lead_user_id") {
    const id = s.replace(/\s/g, "");
    if (/^\d{4,}$/.test(id)) {
      return leadsAnchorTgUser(`tg://user?id=${id}`, s);
    }
    return escapeHtmlCell(s);
  }
  if (k === "source_chat") {
    return formatSourceChatLeadsHtml(s);
  }
  if (k === "message_id") {
    const url = row ? leadsTelegramMessageUrl(row.source_chat, s) : null;
    if (url) {
      const label = s.trim() || "→";
      return leadsAnchorWeb(url, label);
    }
    return s.trim() ? escapeHtmlCell(s) : "—";
  }
  if (k === "message") {
    return formatLeadsMessageFoldHtml(s);
  }
  if (k === "lead_tag") {
    const rid = row && row._id ? String(row._id) : "";
    if (!rid) return "—";
    return buildLeadTagSelectHtml(rid, s);
  }
  if (k === "stage") {
    return escapeHtmlCell(shortStageLabel(s));
  }
  if (k === "status") {
    return escapeHtmlCell(shortStatusLabel(s));
  }
  if (k === "timestamp") {
    return escapeHtmlCell(formatLeadWrittenTime(s));
  }
  return escapeHtmlCell(s);
}

function highlightKeywordFirstHtml(text, keyword) {
  const t = String(text ?? "");
  const kw = String(keyword ?? "").trim();
  if (!kw) return escapeHtmlCell(t);
  const lower = t.toLowerCase();
  const kl = kw.toLowerCase();
  const parts = [];
  let pos = 0;
  let idx = lower.indexOf(kl, pos);
  if (idx < 0) return escapeHtmlCell(t);
  while (idx >= 0) {
    if (idx > pos) parts.push(escapeHtmlCell(t.slice(pos, idx)));
    parts.push(`<mark class="lead-trigger">${escapeHtmlCell(t.slice(idx, idx + kw.length))}</mark>`);
    pos = idx + kw.length;
    idx = lower.indexOf(kl, pos);
  }
  if (pos < t.length) parts.push(escapeHtmlCell(t.slice(pos)));
  return parts.join("");
}

function renderLeadsTable() {
  const table = document.getElementById("leadsTable");
  const thead = document.getElementById("leadsThead");
  const tbody = document.getElementById("leadsTbody");
  if (!table || !thead || !tbody) return;
  const q = (document.getElementById("leadsSearch")?.value || "").trim().toLowerCase();
  let rows = leadsRowsCache;
  if (q) {
    rows = leadsRowsCache.filter((row) => Object.values(row).join(" ").toLowerCase().includes(q));
  }
  const tagF = (document.getElementById("leadsTagFilter")?.value || "").trim();
  if (tagF) {
    rows = rows.filter((row) => normalizeLeadTagKey(row.lead_tag) === tagF);
  }
  const rawKeys =
    leadsRowsCache.length > 0 ? Object.keys(leadsRowsCache[0]) : [...LEADS_TABLE_COLS];
  const keys = rawKeys.filter((k) => k !== "_id");
  const n = keys.length + 1;
  let widths = loadLeadsColWidths(n);
  if (!widths) widths = new Array(n).fill(null);

  table.querySelectorAll("colgroup").forEach((c) => c.remove());
  const cg = document.createElement("colgroup");
  for (let i = 0; i < n; i++) {
    const col = document.createElement("col");
    if (widths[i] != null && widths[i] >= 40) col.style.width = `${widths[i]}px`;
    cg.appendChild(col);
  }
  table.insertBefore(cg, thead);

  const thRow = [
    `<th class="leads-check-col"><span class="leads-th-label"><input type="checkbox" id="leadsSelectAll" title="Выбрать все на экране" aria-label="Выбрать все строки"></span><span class="leads-col-resize-handle" data-col-idx="0" aria-hidden="true"></span></th>`,
    ...keys.map(
      (k, i) =>
        `<th class="${leadsTableColClass(k)}"><span class="leads-th-label">${escapeHtmlCell(leadsColumnTitle(k))}</span><span class="leads-col-resize-handle" data-col-idx="${i + 1}" aria-hidden="true"></span></th>`,
    ),
  ];
  thead.innerHTML = `<tr>${thRow.join("")}</tr>`;

  tbody.innerHTML = rows
    .map((row) => {
      const rid = String(row._id || "").trim();
      const tagKey = normalizeLeadTagKey(row.lead_tag);
      const trClass = rid ? `lead-row-tag-${tagKey}` : "";
      const cb = rid
        ? `<td class="leads-check-col"><input type="checkbox" class="leads-row-check" data-lead-id="${escapeHtmlCell(rid)}"></td>`
        : `<td class="leads-check-col"></td>`;
      const cells = keys.map((k) => {
        const tg = k === "username" || k === "user_id" || k === "source_chat" || k === "message_id";
        const cls = `${leadsTableColClass(k)}${tg ? " tg-cell" : ""}`;
        return `<td class="${cls}">${formatLeadsCellAsHtml(k, row[k] ?? "", row)}</td>`;
      });
      return `<tr class="${escapeHtmlCell(trClass)}">${cb}${cells.join("")}</tr>`;
    })
    .join("");

  document.querySelectorAll("input.leads-row-check").forEach((cbx) => {
    const id = cbx.getAttribute("data-lead-id");
    if (id && leadsSelectedIds.has(id)) cbx.checked = true;
  });

  const selAll = document.getElementById("leadsSelectAll");
  if (selAll) {
    selAll.addEventListener("change", () => {
      document.querySelectorAll("input.leads-row-check").forEach((cbx) => {
        cbx.checked = selAll.checked;
        const id = cbx.getAttribute("data-lead-id");
        if (!id) return;
        if (selAll.checked) leadsSelectedIds.add(id);
        else leadsSelectedIds.delete(id);
      });
    });
  }
  attachLeadsColResize(table);
}

async function loadLeadsPage() {
  const tbody = document.getElementById("leadsTbody");
  if (!tbody) return;
  const errEl = document.getElementById("leadsError");
  const hint = document.getElementById("leadsRowHint");
  if (errEl) {
    errEl.textContent = "";
    errEl.classList.add("hidden");
  }
  try {
    const d = await api("/api/bot/leads?limit=500");
    const rows = Array.isArray(d.rows) ? d.rows : [];
    const sig = JSON.stringify(rows.map((r) => `${r._id || ""}\u241e${r.lead_tag || ""}\u241e${r.timestamp || ""}`));
    if (sig === leadsDataSignature && leadsRowsCache.length > 0) {
      return;
    }
    leadsDataSignature = sig;
    leadsRowsCache = rows;
    if (hint) hint.textContent = `Показано строк: ${leadsRowsCache.length} (последние из журнала)`;
    renderLeadsTable();
  } catch (e) {
    if (errEl) {
      errEl.textContent = e.message || String(e);
      errEl.classList.remove("hidden");
    }
  }
}

function bindLeadsTableDelegationOnce() {
  const table = document.getElementById("leadsTable");
  if (!table || table.dataset.leadsDeleg) return;
  table.dataset.leadsDeleg = "1";
  table.addEventListener("change", (ev) => {
    const t = ev.target;
    if (!t || !t.classList) return;
    if (t.classList.contains("leads-row-check")) {
      const id = t.getAttribute("data-lead-id");
      if (!id) return;
      if (t.checked) leadsSelectedIds.add(id);
      else leadsSelectedIds.delete(id);
      return;
    }
    if (t.classList.contains("leads-tag-select")) {
      const id = t.getAttribute("data-lead-id");
      if (!id) return;
      const tag = t.value;
      (async () => {
        try {
          await api("/api/leads/tag", {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ id, lead_tag: tag }),
          });
          const row = leadsRowsCache.find((r) => String(r._id) === id);
          if (row) row.lead_tag = tag === "lead" ? "" : tag;
          leadsDataSignature = "";
          renderLeadsTable();
        } catch (e) {
          alert(e.message || String(e));
        }
      })();
    }
  });
}

/** Старый «шлак» только в браузере → однократно пишем lead_tag=junk в CSV организации. */
const LEGACY_BOT_JUNK_STORAGE_KEY = "leadgen_bot_junk_lead_ids";

async function migrateLegacyBotJunkToServerOnce() {
  if (!hasAuthToken()) return;
  const oid = window.__leadgenOrgId;
  if (oid == null) return;
  const doneKey = `leadgen_junk_migrated_from_ls_v1_${oid}`;
  try {
    if (localStorage.getItem(doneKey) === "1") return;
    const raw = localStorage.getItem(LEGACY_BOT_JUNK_STORAGE_KEY);
    if (!raw || !raw.trim()) {
      localStorage.setItem(doneKey, "1");
      return;
    }
    let a;
    try {
      a = JSON.parse(raw);
    } catch {
      localStorage.removeItem(LEGACY_BOT_JUNK_STORAGE_KEY);
      localStorage.setItem(doneKey, "1");
      return;
    }
    if (!Array.isArray(a) || a.length === 0) {
      localStorage.removeItem(LEGACY_BOT_JUNK_STORAGE_KEY);
      localStorage.setItem(doneKey, "1");
      return;
    }
    for (const id of a) {
      const rid = String(id).trim();
      if (!rid) continue;
      try {
        await api("/api/leads/tag", {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ id: rid, lead_tag: "junk" }),
        });
      } catch {
        /* строки с этим id уже нет */
      }
    }
    localStorage.removeItem(LEGACY_BOT_JUNK_STORAGE_KEY);
    localStorage.setItem(doneKey, "1");
    leadsDataSignature = "";
    if (document.getElementById("leadsTbody")) await loadLeadsPage().catch(() => {});
    if (document.getElementById("botLeadsTable")) await refreshBotLeads().catch(() => {});
  } catch {
    /* ignore */
  }
}

function wireBotMonitorSettingsDialog() {
  const dlg = document.getElementById("botMonitorSettingsDialog");
  const openBtn = document.getElementById("openBotMonitorSettingsBtn");
  const cancel = document.getElementById("botMonitorSettingsCancelBtn");
  const save = document.getElementById("botMonitorSettingsSaveBtn");
  const msg = document.getElementById("botMonitorSettingsMsg");
  if (!dlg || !openBtn) return;
  openBtn.addEventListener("click", async () => {
    if (msg) {
      msg.textContent = "";
      msg.classList.add("hidden");
    }
    try {
      const cfg = await api("/api/config");
      const lim = cfg.limits || {};
      const sch = lim.schedule || {};
      const ge = (id) => document.getElementById(id);
      if (ge("botDlgMonitorSec")) ge("botDlgMonitorSec").value = lim.monitor_interval_sec ?? 30;
      if (ge("botDlgMaxPasses")) ge("botDlgMaxPasses").value = lim.max_monitor_passes != null ? String(lim.max_monitor_passes) : "0";
      if (ge("botDlgSchEn")) ge("botDlgSchEn").checked = Boolean(sch.enabled);
      if (ge("botDlgSchTz")) ge("botDlgSchTz").value = sch.timezone != null ? String(sch.timezone) : "Europe/Moscow";
      if (Array.isArray(sch.active_hours) && sch.active_hours.length >= 2) {
        if (ge("botDlgSchStart")) ge("botDlgSchStart").value = sch.active_hours[0] ?? 9;
        if (ge("botDlgSchEnd")) ge("botDlgSchEnd").value = sch.active_hours[1] ?? 21;
      } else {
        if (ge("botDlgSchStart")) ge("botDlgSchStart").value = 9;
        if (ge("botDlgSchEnd")) ge("botDlgSchEnd").value = 21;
      }
      dlg.showModal();
    } catch (e) {
      showBotActionMessage(e.message || String(e), "error");
    }
  });
  if (cancel) cancel.addEventListener("click", () => dlg.close());
  if (save) {
    save.addEventListener("click", async () => {
      if (msg) {
        msg.textContent = "";
        msg.classList.add("hidden");
      }
      const ge = (id) => document.getElementById(id);
      try {
        const prev = await api("/api/config");
        const lim = { ...(prev.limits || {}) };
        lim.monitor_interval_sec = Math.max(1, toInt(ge("botDlgMonitorSec")?.value, 30));
        lim.max_monitor_passes = Math.max(0, toInt(ge("botDlgMaxPasses")?.value, 0));
        lim.schedule = {
          enabled: Boolean(ge("botDlgSchEn")?.checked),
          timezone: String(ge("botDlgSchTz")?.value || "Europe/Moscow").trim() || "Europe/Moscow",
          active_hours: [toFloat(ge("botDlgSchStart")?.value, 9), toFloat(ge("botDlgSchEnd")?.value, 21)],
        };
        const next = { ...prev, limits: { ...prev.limits, ...lim } };
        await api("/api/config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(next),
        });
        dlg.close();
        await refreshBotInfo();
        showBotActionMessage("Настройки мониторинга сохранены", "ok");
      } catch (e) {
        if (msg) {
          msg.textContent = e.message || String(e);
          msg.classList.remove("hidden");
        }
      }
    });
  }
}

async function ensureLlmPresetsUi() {
  const sel = document.getElementById("llmPresetSelect");
  if (!sel || llmPresetsUiReady) return;
  llmPresetsUiReady = true;
  /** @type {Record<string, { base_url: string; model: string }>} */
  const map = {};
  try {
    const d = await api("/api/llm/presets");
    const presets = Array.isArray(d.presets) ? d.presets : [];
    for (const p of presets) {
      const id = String(p.id || "");
      if (!id) continue;
      map[id] = {
        base_url: String(p.base_url || "").trim(),
        model: String(p.model || "").trim(),
      };
      const o = document.createElement("option");
      o.value = id;
      o.textContent = String(p.label || id);
      sel.appendChild(o);
    }
    sel.addEventListener("change", () => {
      const pr = map[sel.value];
      if (!pr || !pr.base_url) return;
      const bu = document.getElementById("llmBaseUrl");
      const mo = document.getElementById("llmModel");
      if (bu) bu.value = pr.base_url;
      if (mo) mo.value = pr.model;
    });
  } catch {
    /* ignore */
  }
}

async function refreshHomeDashboard() {
  if (!document.querySelector("[data-home-card]")) return;
  const setStatus = (key, text, mode) => {
    const c = document.querySelector(`[data-home-card="${key}"] [data-home-status]`);
    if (!c) return;
    c.textContent = text;
    c.classList.remove("tool-card-status--ok", "tool-card-status--warn");
    if (mode === "ok") c.classList.add("tool-card-status--ok");
    else if (mode === "warn") c.classList.add("tool-card-status--warn");
  };
  try {
    const me = await api("/api/auth/me");
    if (!me.authenticated) return;
    const role = window.__leadgenOrgRole;
    const funnelTools =
      role === "admin" || role === "manager" || role === "client" || role === "tester";
    const staffOutreach = role === "admin" || role === "manager" || role === "tester";
    setStatus(
      "discover",
      funnelTools ? "Поиск каналов: доступен" : "Поиск: нет доступа к воронке для этой роли",
      funnelTools ? "ok" : "warn",
    );
    const [info, st, ov] = await Promise.all([
      api("/api/bot/info").catch(() => null),
      api("/api/stats/summary").catch(() => null),
      api("/api/chats/overview").catch(() => null),
    ]);
    if (info) {
      const r = info.running ? "запущен" : "остановлен";
      const ch = info.target_chats != null ? `${info.target_chats} чатов` : "—";
      const test = info.dry_run ? " · тест без лички" : "";
      setStatus("contacts", `Бот ${r} · ${ch}${test}`, info.running ? "ok" : "warn");
    } else {
      setStatus("contacts", "Статус бота недоступен", "warn");
    }
    if (ov && typeof ov.llm_ready === "boolean") {
      setStatus(
        "inbox",
        ov.llm_ready ? "LLM настроен · лиды и черновики ЛС" : "LLM не настроен (ключ задаёт админ)",
        ov.llm_ready ? "ok" : "warn",
      );
    } else if (st) {
      setStatus("inbox", `Лидов в таблице: ${st.leads_rows_total != null ? st.leads_rows_total : "—"}`, "ok");
    } else {
      setStatus("inbox", "Переписка", "ok");
    }
    if (staffOutreach) {
      try {
        const oq = await api("/api/outreach?status=pending");
        const n = Array.isArray(oq.items) ? oq.items.length : 0;
        setStatus("offers", n ? `На согласовании: ${n}` : "Очередь согласования пуста", n ? "warn" : "ok");
      } catch {
        setStatus("offers", "Очередь согласования (админ / менеджер / тестировщик org)", "ok");
      }
    } else {
      setStatus(
        "offers",
        funnelTools
          ? "Согласование — для администратора, менеджера или тестировщика организации"
          : "Офферы — нет доступа к воронке",
        "ok",
      );
    }
    try {
      const cl = await api("/api/calls");
      const n = Array.isArray(cl.items) ? cl.items.length : 0;
      setStatus("calls", `Записей о созвонах: ${n}`, "ok");
    } catch {
      setStatus("calls", "Созвоны", "ok");
    }
  } catch {
    document.querySelectorAll("[data-home-status]").forEach((el) => {
      el.textContent = "—";
    });
  }
}

function setFormError(el, message) {
  if (!el) return;
  if (!message) {
    el.textContent = "";
    el.classList.add("hidden");
    return;
  }
  el.textContent = message;
  el.classList.remove("hidden");
}

async function authPost(url, body) {
  // Логин/регистрация — сервер выставляет cookie auth_token + auth_present.
  // CSRF на /api/auth/login и /api/auth/register не требуется (нет сессии).
  const r = await fetch(url, {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const d = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new Error(d.message || "Ошибка запроса");
  }
  return d;
}

function bindPasswordToggles() {
  document.querySelectorAll(".pwd-toggle[data-pwd-target]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = btn.getAttribute("data-pwd-target");
      const input = id ? document.getElementById(id) : null;
      if (!input) return;
      if (input.type === "password") {
        input.type = "text";
        btn.textContent = "Скрыть";
        btn.setAttribute("aria-label", "Скрыть пароль");
      } else {
        input.type = "password";
        btn.textContent = "Показать";
        btn.setAttribute("aria-label", "Показать пароль");
      }
    });
  });
}

function updateHeaderAuth(me) {
  const loginBtn = document.getElementById("headerLoginBtn");
  if (loginBtn) {
    if (!me || !me.authenticated) loginBtn.classList.remove("hidden");
    else loginBtn.classList.add("hidden");
  }
  const authed = Boolean(me && me.authenticated);
  const prev = document.getElementById("profileAvatarPreview");
  if (prev && authed && me.user_id && me.avatar_url) {
    prev.src = `${me.avatar_url}?v=${encodeURIComponent(String(me.user_id))}`;
  } else if (prev && !authed) {
    prev.removeAttribute("src");
  }
}

async function refreshHealth() {
  let d = null;
  try {
    const r = await fetch("/healthz", { method: "GET", credentials: "same-origin" });
    const dateHeader = r.headers ? r.headers.get("date") : null;
    d = await r.json().catch(() => ({}));
    window.__lastHealthz = {
      fetched_at: new Date().toISOString(),
      date_header: dateHeader,
      json: d,
    };
  } catch (e) {
    d = { status: "error" };
    window.__lastHealthz = {
      fetched_at: new Date().toISOString(),
      date_header: null,
      json: null,
      error: String(e && e.message ? e.message : e),
    };
  }
  const healthMeta = document.getElementById("healthMeta");
  const serverMeta = document.getElementById("serverMeta");
  if (healthMeta) healthMeta.textContent = `Health: ${d.status}`;
  if (serverMeta) serverMeta.textContent = `Сервер: ${window.location.origin}`;
}

async function refreshAuthState() {
  const d = await api("/api/auth/me");
  const state = document.getElementById("authState");
  if (!d.authenticated) {
    currentUserId = null;
    window.__leadgenOrgRole = null;
    window.__leadgenOrgId = null;
    if (state) state.textContent = "Не авторизован";
    applyRoleUI(null);
    applyChatsLlmPanelRole(null);
    setSidebarAccount(null);
    applyAuthNav(false);
    updateHeaderAuth(null);
    enforceAuthRedirect();
    syncAdminOrgDebugVisibility();
    _uiDebugRestoreForCurrentUser();
    return;
  }
  currentUserId = typeof d.user_id === "number" ? d.user_id : null;
  window.__leadgenOrgRole = d.org_role || null;
  window.__leadgenUserPlatformAdmin = d.role === "admin";
  window.__leadgenOrgId =
    d.org_id != null && d.org_id !== undefined && Number.isFinite(Number(d.org_id)) ? Number(d.org_id) : null;
  if (state) state.textContent = `Вход выполнен: ${d.email} (${d.org_role}) • тариф: ${d.plan_id} • подписка: ${d.subscription_status}`;
  applyRoleUI(d.org_role);
  applyChatsLlmPanelRole(d.org_role);
  setSidebarAccount(d);
  applyAuthNav(true);
  updateHeaderAuth(d);
  await refreshSidebarAccountExtras();
  refreshPendingOutreachBadge().catch(() => {});
  if (d.org_role === "admin") {
    await refreshAdmin();
  }
  syncAdminOrgDebugVisibility();
  _uiDebugRestoreForCurrentUser();
}

function applyAuthNav(isAuthed) {
  document.querySelectorAll(".nav-auth-required").forEach((el) => {
    if (el.hasAttribute("data-sidebar-account")) {
      el.style.display = isAuthed ? "flex" : "none";
    } else if (el.id === "leadgenAssistantHost") {
      el.style.display = "";
      el.classList.toggle("hidden", !isAuthed);
      el.setAttribute("aria-hidden", isAuthed ? "false" : "true");
    } else {
      el.style.display = isAuthed ? "block" : "none";
    }
  });
}

function enforceAuthRedirect() {
  // if user is not authed, keep only /auth accessible in UI and redirect from other pages
  const page = document.body && document.body.dataset ? document.body.dataset.page : "";
  if (page && page !== "auth" && page !== "base" && page !== "legal") {
    window.location.href = "/auth";
  }
}

function setSidebarAccount(me) {
  const email = document.getElementById("sbUserEmail");
  const r = document.getElementById("sbOrgRole");
  const pl = document.getElementById("sbPlanLine");
  const av = document.getElementById("sbAvatar");
  const lo = document.getElementById("sbLogoutBtn");
  if (!email || !r) return;
  const orgHint = document.getElementById("sbOrgHint");
  if (!me) {
    if (pl) pl.textContent = "Тариф: —";
    if (orgHint) {
      orgHint.textContent = "";
      orgHint.classList.add("hidden");
    }
    email.textContent = "—";
    email.removeAttribute("title");
    r.textContent = "Роль: —";
    if (av) {
      av.classList.add("hidden");
      av.removeAttribute("src");
    }
    if (lo) lo.classList.add("hidden");
    return;
  }
  email.textContent = me.email || "—";
  if (me.email) email.setAttribute("title", String(me.email));
  let roleLine = `Роль: ${me.org_role || "—"}`;
  if (me.org_id != null) {
    roleLine += ` · org #${me.org_id}`;
    if (me.org_name) {
      roleLine += ` (${me.org_name})`;
    }
  }
  r.textContent = roleLine;
  if (orgHint) {
    if (me.manager_on_default_org) {
      orgHint.textContent =
        "Менеджер в Default org: в Admin → Пользователи назначьте org клиента.";
      orgHint.classList.remove("hidden");
    } else {
      orgHint.textContent = "";
      orgHint.classList.add("hidden");
    }
  }
  if (pl) pl.textContent = `Тариф: ${me.plan_id || "—"} · ${me.subscription_status || "—"}`;
  if (av) {
    if (me.user_id && me.avatar_url) {
      av.src = `${me.avatar_url}?v=${encodeURIComponent(String(me.user_id))}`;
      av.classList.remove("hidden");
    } else {
      av.classList.add("hidden");
    }
  }
  if (lo) lo.classList.remove("hidden");
}

async function refreshSidebarAccountExtras() {
  if (!hasAuthToken()) return;
  const limC = document.getElementById("sbLimChats");
  if (!limC) return;
  try {
    const st = await api("/api/stats/summary");
    const lim = st.limits || {};
    const dayR = Array.isArray(lim.daily_limit_range) ? lim.daily_limit_range.join("–") : "—";
    limC.textContent = `В мониторинге чатов: ${st.target_chats != null ? st.target_chats : "—"}`;
    const dEl = document.getElementById("sbLimDay");
    if (dEl) dEl.textContent = `ЛС/день: ${dayR} (сегодня ${st.daily_sent_count != null ? st.daily_sent_count : "—"})`;
    const mEl = document.getElementById("sbLimMonth");
    if (mEl) mEl.textContent = `ЛС/мес: ${lim.max_dm_month != null ? lim.max_dm_month : "—"}`;
    const iEl = document.getElementById("sbLimInt");
    if (iEl) iEl.textContent = `Интервал монит.: ${lim.monitor_interval_sec != null ? lim.monitor_interval_sec : "—"} с`;
  } catch {
    /* ignore */
  }
  const llmLine = document.getElementById("sbLlmLine");
  if (!llmLine) return;
  try {
    const o = await api("/api/chats/overview");
    if (o.llm_ready) {
      llmLine.textContent = "LLM: включён (ключ + включено в настройках)";
    } else {
      llmLine.textContent = "LLM: нет (Настройки → Подключения)";
    }
  } catch {
    llmLine.textContent = "LLM: —";
  }
}

function applyRoleUI(orgRole) {
  document.querySelectorAll(".nav-admin").forEach((b) => b.classList.toggle("hidden", orgRole !== "admin"));
  const showManager = orgRole === "admin" || orgRole === "manager" || orgRole === "tester";
  document.querySelectorAll(".nav-manager").forEach((b) => b.classList.toggle("hidden", !showManager));
  applyChannelSearchQualityPanelRole(orgRole);
}

/** Блок «Поиск каналов: качество»: админ редактирует; tester видит read-only; остальным скрыт. */
function applyChannelSearchQualityPanelRole(orgRole) {
  const p = document.getElementById("channelSearchQualityPanel");
  if (!p) return;
  const show = orgRole === "admin" || orgRole === "tester";
  const isAdmin = orgRole === "admin";
  p.classList.toggle("hidden", !show);
  p.querySelectorAll("input, select, textarea, button").forEach((el) => {
    el.disabled = !isAdmin;
  });
}

/** Блок LLM в настройках («Подключения»): только админ организации может редактировать. */
function applyChatsLlmPanelRole(orgRole) {
  const isAdmin = orgRole === "admin";
  const hint = document.getElementById("llmReadonlyHint");
  const block = document.getElementById("llmAdminBlock");
  const promptSection = document.getElementById("llmPromptsSection");
  if (hint) {
    hint.classList.toggle("is-shown", !isAdmin);
  }
  if (promptSection) {
    promptSection.classList.toggle("hidden", !isAdmin);
  }
  if (block) {
    block.querySelectorAll("input, button, select, textarea").forEach((el) => {
      if (promptSection && promptSection.contains(el)) return;
      el.disabled = !isAdmin;
    });
  }
}

function isDocumentVisible() {
  return document.visibilityState === "visible" && !document.hidden;
}

function hasAuthToken() {
  // Проверяем non-HttpOnly маркер `auth_present`, выставляемый сервером
  // при логине рядом с HttpOnly `auth_token`. Сам токен из JS не виден.
  try {
    const m = document.cookie.match(/(?:^|;\s*)auth_present=([^;]+)/);
    return !!(m && m[1]);
  } catch {
    return false;
  }
}

/**
 * Защита от наслоения: запускает fn() только если предыдущий запуск завершился.
 * Возвращает функцию, которую безопасно дёргать в setInterval.
 */
function makePoller(fn) {
  let inFlight = false;
  return async () => {
    if (inFlight) return;
    if (!isDocumentVisible()) return;
    inFlight = true;
    try {
      await fn();
    } catch {
      /* поллинг не должен падать наверх */
    } finally {
      inFlight = false;
    }
  };
}

async function refreshPendingOutreachBadge() {
  const el = document.getElementById("navPendingOutreachBadge");
  if (!el) return;
  if (!hasAuthToken()) {
    el.classList.add("hidden");
    return;
  }
  const role = window.__leadgenOrgRole;
  if (role !== "admin" && role !== "manager" && role !== "tester") {
    el.classList.add("hidden");
    return;
  }
  try {
    const d = await api("/api/outreach/pending-count");
    const n = Number(d.pending || 0);
    if (n > 0) {
      el.textContent = String(n);
      el.classList.remove("hidden");
    } else {
      el.classList.add("hidden");
    }
  } catch {
    el.classList.add("hidden");
  }
}

function bindLeadgenAssistant() {
  const host = document.getElementById("leadgenAssistantHost");
  const fab = document.getElementById("leadgenAssistantFab");
  const panel = document.getElementById("leadgenAssistantPanel");
  const closeBtn = document.getElementById("leadgenAssistantClose");
  const sendBtn = document.getElementById("leadgenAssistantSend");
  const input = document.getElementById("leadgenAssistantInput");
  const messages = document.getElementById("leadgenAssistantMessages");
  if (!host || !fab || !panel || !messages || !input || !sendBtn) return;
  if (host.dataset.leadgenAssistantBound) return;
  host.dataset.leadgenAssistantBound = "1";

  const scrollBottom = () => {
    messages.scrollTop = messages.scrollHeight;
  };

  const appendBubble = (text, variant) => {
    const div = document.createElement("div");
    div.className = `leadgen-assistant-msg leadgen-assistant-msg--${variant}`;
    div.textContent = text;
    messages.appendChild(div);
    scrollBottom();
  };

  const setOpen = (open) => {
    panel.classList.toggle("hidden", !open);
    fab.setAttribute("aria-expanded", open ? "true" : "false");
    try {
      sessionStorage.setItem("leadgen_assistant_open", open ? "1" : "0");
    } catch {
      /* ignore */
    }
    if (open) {
      input.focus();
    }
  };

  fab.addEventListener("click", () => {
    const willOpen = panel.classList.contains("hidden");
    setOpen(willOpen);
  });
  if (closeBtn) closeBtn.addEventListener("click", () => setOpen(false));

  const send = async () => {
    const q = String(input.value || "").trim();
    if (q.length < 2) {
      alert("Введите вопрос не короче 2 символов.");
      return;
    }
    appendBubble(q, "user");
    input.value = "";
    appendBubble("…", "hint");
    const hintEl = messages.lastChild;
    try {
      const page = (document.body && document.body.dataset && document.body.dataset.page) || "";
      const d = await api("/api/assistant/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: q, page }),
      });
      if (hintEl && hintEl.parentNode === messages) messages.removeChild(hintEl);
      const ans = String(d.answer || d.message || "").trim() || "(пустой ответ)";
      const tag = d.fallback ? "hint" : "bot";
      appendBubble(ans, tag);
    } catch (e) {
      if (hintEl && hintEl.parentNode === messages) messages.removeChild(hintEl);
      appendBubble(e.message || String(e), "hint");
    }
  };
  sendBtn.addEventListener("click", () => {
    send().catch((err) => alert(err.message || String(err)));
  });
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      send().catch((err) => alert(err.message || String(err)));
    }
  });

  try {
    if (sessionStorage.getItem("leadgen_assistant_open") === "1") {
      setOpen(true);
    }
  } catch {
    /* ignore */
  }
}

function bindConfigPageTabs() {
  const main = document.getElementById("configPanelMain");
  const conn = document.getElementById("configPanelConn");
  if (!main || !conn) return;
  const buttons = document.querySelectorAll(".config-page-tabs [data-config-tab]");
  const activate = (name) => {
    const n = name === "conn" ? "conn" : "main";
    buttons.forEach((b) => {
      const tab = b.getAttribute("data-config-tab") || "main";
      const on = tab === n;
      b.classList.toggle("active", on);
      b.setAttribute("aria-selected", on ? "true" : "false");
    });
    main.classList.toggle("hidden", n !== "main");
    conn.classList.toggle("hidden", n !== "conn");
  };
  buttons.forEach((b) => {
    b.addEventListener("click", () => activate(b.getAttribute("data-config-tab") || "main"));
  });
  if (location.hash === "#connections") {
    activate("conn");
  }
}

/**
 * @param {object} [options]
 * @param {"merge"|"replace"} [options.mode]
 * @param {HTMLElement|null} [options.msgEl]
 */
async function syncChatsFromAccount(options = {}) {
  const mode = options.mode === "replace" ? "replace" : "merge";
  const limit = Number(options.limit) > 0 ? Number(options.limit) : 500;
  const fromSearch = getSearchTelegramAccountPayload();
  return api("/api/chats/sync-dialogs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mode, limit, ...fromSearch }),
  });
}

function showBotActionMessage(text, variant) {
  const el = document.getElementById("botActionMsg");
  if (!el) return;
  if (!text) {
    el.textContent = "";
    el.className = "bot-inline-msg hidden";
    el.removeAttribute("role");
    return;
  }
  el.textContent = text;
  const v = variant === "error" ? "err" : "ok";
  el.className = `bot-inline-msg bot-inline-msg--${v}`;
  el.setAttribute("role", "status");
}

/**
 * Блокировка кнопки на время долгого запроса (поиск, синхронизация).
 * @param {HTMLButtonElement | null} btn
 * @param {boolean} busy
 * @param {{ busyLabel?: string }} [opts]
 */
function setButtonBusy(btn, busy, opts = {}) {
  if (!btn || btn.tagName !== "BUTTON") return;
  const busyLabel = opts.busyLabel;
  if (busy) {
    if (busyLabel != null && btn.dataset._idleBtnLabel == null) {
      btn.dataset._idleBtnLabel = btn.textContent;
      btn.textContent = busyLabel;
    }
    btn.disabled = true;
    btn.setAttribute("aria-busy", "true");
  } else {
    if (btn.dataset._idleBtnLabel != null) {
      btn.textContent = btn.dataset._idleBtnLabel;
      delete btn.dataset._idleBtnLabel;
    }
    btn.disabled = false;
    btn.setAttribute("aria-busy", "false");
  }
}

/** Лог bot.log организации: только admin в org; overlay поверх UI. */
const ADMIN_DEBUG_RECT_KEY = "leadgen_admin_debug_panel_rect";

let adminOrgDebugUiBound = false;
/** @type {number | null} */
let adminOrgDebugPoll = null;
/** @type {ResizeObserver | null} */
let adminOrgDebugResizeObs = null;
let adminOrgDebugDragBound = false;
/** Пользователь нажал «Очистить вид» — не тянуть лог с сервера в фоне, пока не «Обновить» или не открыли панель заново. */
let adminOrgDebugManualClear = false;

function adminOrgDebugDefaultRect() {
  const margin = 16;
  const w = Math.min(920, Math.max(260, window.innerWidth - margin * 2));
  const h = Math.min(760, Math.max(160, Math.floor(window.innerHeight * 0.82)));
  const l = Math.round((window.innerWidth - w) / 2);
  const t = Math.round((window.innerHeight - h) / 2);
  return { l, t, w, h };
}

function adminOrgDebugClampRect(r) {
  const margin = 8;
  let { l, t, w, h } = r;
  w = Math.max(260, Math.min(w, window.innerWidth - margin));
  h = Math.max(160, Math.min(h, window.innerHeight - margin));
  l = Math.min(Math.max(margin, l), Math.max(margin, window.innerWidth - w - margin));
  t = Math.min(Math.max(margin, t), Math.max(margin, window.innerHeight - h - margin));
  return { l, t, w, h };
}

function adminOrgDebugLoadSavedRect() {
  try {
    const raw = sessionStorage.getItem(ADMIN_DEBUG_RECT_KEY);
    if (!raw) return null;
    const j = JSON.parse(raw);
    if (
      typeof j.l !== "number" ||
      typeof j.t !== "number" ||
      typeof j.w !== "number" ||
      typeof j.h !== "number"
    )
      return null;
    return adminOrgDebugClampRect(j);
  } catch {
    return null;
  }
}

function adminOrgDebugApplyRect(panel, r) {
  const rr = adminOrgDebugClampRect(r);
  panel.style.left = `${rr.l}px`;
  panel.style.top = `${rr.t}px`;
  panel.style.width = `${rr.w}px`;
  panel.style.height = `${rr.h}px`;
}

function adminOrgDebugSaveRect(panel) {
  try {
    const bb = panel.getBoundingClientRect();
    const rr = adminOrgDebugClampRect({
      l: bb.left,
      t: bb.top,
      w: bb.width,
      h: bb.height,
    });
    sessionStorage.setItem(ADMIN_DEBUG_RECT_KEY, JSON.stringify(rr));
  } catch {
    /* ignore */
  }
}

function adminOrgDebugScheduleSaveRect(panel) {
  if (panel._adminDebugSaveTimer) window.clearTimeout(panel._adminDebugSaveTimer);
  panel._adminDebugSaveTimer = window.setTimeout(() => {
    panel._adminDebugSaveTimer = null;
    adminOrgDebugSaveRect(panel);
  }, 400);
}

function adminOrgDebugEnsurePanelPosition(panel) {
  const saved = adminOrgDebugLoadSavedRect();
  const r = saved ?? adminOrgDebugDefaultRect();
  adminOrgDebugApplyRect(panel, r);
}

/** Не затирать текст при автообновлении, если пользователь читает/копирует из поля лога. */
function adminOrgDebugShouldFreezeContentReplace() {
  const pre = document.getElementById("adminOrgDebugPre");
  if (!pre) return false;
  if (document.activeElement === pre) return true;
  const sel = window.getSelection();
  if (!sel || sel.isCollapsed) return false;
  try {
    const an = sel.anchorNode;
    if (an && pre.contains(an)) return true;
  } catch {
    /* ignore */
  }
  return false;
}

function adminOrgDebugObserveResize(panel) {
  if (typeof ResizeObserver === "undefined") return;
  if (adminOrgDebugResizeObs) adminOrgDebugResizeObs.disconnect();
  adminOrgDebugResizeObs = new ResizeObserver(() => {
    adminOrgDebugScheduleSaveRect(panel);
  });
  adminOrgDebugResizeObs.observe(panel);
}

function adminOrgDebugBindDrag(panel) {
  const dragEl = document.getElementById("adminOrgDebugDrag");
  if (!dragEl || adminOrgDebugDragBound) return;
  adminOrgDebugDragBound = true;
  let dragState = null;
  dragEl.addEventListener("mousedown", (e) => {
    if (e.button !== 0) return;
    if (e.target.closest("button, input, label")) return;
    const bb = panel.getBoundingClientRect();
    dragState = { sx: e.clientX, sy: e.clientY, l: bb.left, t: bb.top, w: bb.width, h: bb.height };
    e.preventDefault();
  });
  window.addEventListener("mousemove", (e) => {
    if (!dragState) return;
    const dx = e.clientX - dragState.sx;
    const dy = e.clientY - dragState.sy;
    adminOrgDebugApplyRect(panel, {
      l: dragState.l + dx,
      t: dragState.t + dy,
      w: dragState.w,
      h: dragState.h,
    });
  });
  window.addEventListener("mouseup", () => {
    if (!dragState) return;
    dragState = null;
    adminOrgDebugSaveRect(panel);
  });
}

function restartAdminOrgDebugPoll() {
  if (adminOrgDebugPoll != null) {
    window.clearInterval(adminOrgDebugPoll);
    adminOrgDebugPoll = null;
  }
  const panel = document.getElementById("adminOrgDebugPanel");
  const live = document.getElementById("adminOrgDebugLive");
  if (!panel || panel.classList.contains("hidden")) return;
  if (live && !live.checked) return;
  adminOrgDebugPoll = window.setInterval(() => {
    refreshAdminOrgDebugLog({}).catch(() => {});
  }, 3500);
}

function syncAdminOrgDebugVisibility() {
  const host = document.getElementById("adminOrgDebugHost");
  if (!host) return;
  const show = hasAuthToken() && (window.__leadgenOrgRole === "admin" || window.__leadgenOrgRole === "tester");
  host.classList.toggle("hidden", !show);
  host.setAttribute("aria-hidden", show ? "false" : "true");
  if (!show) closeAdminOrgDebugPanel();
}

function closeAdminOrgDebugPanel() {
  const panel = document.getElementById("adminOrgDebugPanel");
  const fab = document.getElementById("adminOrgDebugFab");
  if (panel) panel.classList.add("hidden");
  if (fab) fab.setAttribute("aria-expanded", "false");
  if (adminOrgDebugPoll != null) {
    window.clearInterval(adminOrgDebugPoll);
    adminOrgDebugPoll = null;
  }
  if (adminOrgDebugResizeObs) {
    adminOrgDebugResizeObs.disconnect();
    adminOrgDebugResizeObs = null;
  }
}

/** @param {{ force?: boolean }} [opts] — force: всегда заменить текст (кнопка «Обновить», открытие панели). */
async function refreshAdminOrgDebugLog(opts = {}) {
  const force = opts.force === true;
  if (force) {
    adminOrgDebugManualClear = false;
  }
  const pre = document.getElementById("adminOrgDebugPre");
  const hint = document.getElementById("adminOrgDebugHint");
  if (!pre) return;
  if (!force && adminOrgDebugManualClear) {
    return;
  }
  const skipReplace = !force && adminOrgDebugShouldFreezeContentReplace();
  try {
    const d = await api("/api/org-admin/debug-log?bytes=160000");
    const t = d.text != null ? String(d.text) : "";
    const next =
      t.trim() !== "" ? t : "(пусто или файл лога ещё не создан)";
    if (!skipReplace) pre.textContent = next;
    if (hint && d.log_hint != null) hint.textContent = String(d.log_hint);
  } catch (e) {
    if (!skipReplace) pre.textContent = e.message || String(e);
    if (hint) hint.textContent = "";
  }
}

function openAdminOrgDebugPanel() {
  const panel = document.getElementById("adminOrgDebugPanel");
  const fab = document.getElementById("adminOrgDebugFab");
  if (!panel || !fab) return;
  panel.classList.remove("hidden");
  fab.setAttribute("aria-expanded", "true");
  adminOrgDebugEnsurePanelPosition(panel);
  adminOrgDebugObserveResize(panel);
  adminOrgDebugBindDrag(panel);
  refreshAdminOrgDebugLog({ force: true }).catch(() => {});
  restartAdminOrgDebugPoll();
}

function bindAdminOrgDebugUiOnce() {
  if (adminOrgDebugUiBound) return;
  adminOrgDebugUiBound = true;
  const fab = document.getElementById("adminOrgDebugFab");
  const close = document.getElementById("adminOrgDebugClose");
  const refBtn = document.getElementById("adminOrgDebugRefresh");
  const clearBtn = document.getElementById("adminOrgDebugClear");
  const copyBtn = document.getElementById("adminOrgDebugCopy");
  const liveCb = document.getElementById("adminOrgDebugLive");
  if (fab) {
    fab.addEventListener("click", () => {
      const panel = document.getElementById("adminOrgDebugPanel");
      if (panel && !panel.classList.contains("hidden")) closeAdminOrgDebugPanel();
      else openAdminOrgDebugPanel();
    });
  }
  if (close) close.addEventListener("click", () => closeAdminOrgDebugPanel());
  if (refBtn)
    refBtn.addEventListener("click", () => refreshAdminOrgDebugLog({ force: true }).catch(() => {}));
  if (clearBtn) {
    clearBtn.addEventListener("click", () => {
      adminOrgDebugManualClear = true;
      const pre = document.getElementById("adminOrgDebugPre");
      const hint = document.getElementById("adminOrgDebugHint");
      if (pre) pre.textContent = "";
      if (hint)
        hint.textContent =
          "Экран очищен: автоподгрузка отключена до «Обновить» или закрытия панели. Файл bot.log на сервере не меняется.";
    });
  }
  if (copyBtn) {
    copyBtn.addEventListener("click", async () => {
      const pre = document.getElementById("adminOrgDebugPre");
      if (!pre) return;
      const text = pre.textContent || "";
      try {
        await copyTextToClipboard(text);
      } catch {
        /* ignore */
      }
    });
  }
  if (liveCb) liveCb.addEventListener("change", () => restartAdminOrgDebugPoll());
}

function setCards(container, cards) {
  if (!container) return;
  container.innerHTML = "";
  for (const c of cards) {
    const el = document.createElement("div");
    el.className = "card-mini";
    const k = document.createElement("div");
    k.className = "k";
    k.textContent = String(c.k ?? "");
    const v = document.createElement("div");
    v.className = "v";
    v.textContent = String(c.v ?? "");
    el.appendChild(k);
    el.appendChild(v);
    container.appendChild(el);
  }
}

function setKv(container, rows) {
  if (!container) return;
  container.innerHTML = "";
  for (const r of rows) {
    const k = document.createElement("div");
    k.className = "k";
    k.textContent = r.k;
    const v = document.createElement("div");
    v.className = "v";
    v.textContent = String(r.v ?? "");
    container.appendChild(k);
    container.appendChild(v);
  }
}

function setPlansTable(container, plans) {
  if (!container) return;
  container.innerHTML = "";
  for (const p of plans) {
    const tr = document.createElement("div");
    tr.className = "tr";
    const title = document.createElement("div");
    title.className = "title";
    title.textContent = String(p.title ?? "");
    const desc = document.createElement("div");
    desc.textContent = String(p.desc ?? "");
    tr.appendChild(title);
    tr.appendChild(desc);
    container.appendChild(tr);
  }
}

async function refreshAdminRestartPanel() {
  const kv = document.getElementById("adminRestartKv");
  const btn = document.getElementById("adminRestartBtn");
  if (!kv) return;
  let info;
  try {
    info = await api("/api/admin/restart-info");
  } catch (e) {
    kv.innerHTML = "";
    setKv(kv, [{ k: "Статус", v: e.message || String(e) }]);
    if (btn) btn.disabled = true;
    return;
  }
  const methodRu = {
    none: "не настроен",
    command: "команда (recommended)",
    touch: "touch-файл (gunicorn --reload-extra-file)",
    parent_signal: `сигнал родителю (${info.parent_signal || "HUP"})`,
    self_exec: "self-exec (одиночный процесс)",
  };
  const rows = [
    { k: "Метод перезапуска", v: methodRu[info.method] || info.method },
  ];
  if (info.command) rows.push({ k: "Команда", v: info.command });
  if (info.touch_file) rows.push({ k: "Touch-файл", v: info.touch_file });
  if (info.parent_signal) rows.push({ k: "Сигнал", v: info.parent_signal });
  if (info.self_exec_allowed) rows.push({ k: "Self-exec", v: "разрешён (LEADGEN_ALLOW_SELF_EXEC=1)" });
  if (!info.available) {
    rows.push({
      k: "Подсказка",
      v: "Перезапуск не настроен на сервере. Раскройте раздел «Как настроить» ниже.",
    });
  }
  setKv(kv, rows);
  if (btn) btn.disabled = !info.available;
}

async function adminRequestRestart() {
  const btn = document.getElementById("adminRestartBtn");
  const msg = document.getElementById("adminRestartMsg");
  if (!window.confirm("Перезапустить сайт сейчас? Соединения активных пользователей оборвутся на 5–20 секунд.")) {
    return;
  }
  if (msg) msg.textContent = "";
  setButtonBusy(btn, true, { busyLabel: "Готовлю перезапуск…" });
  try {
    const d = await api("/api/admin/restart", { method: "POST" });
    if (msg) msg.textContent = d.message || "Запрос отправлен";
    // через 4–8 секунд после ответа сервера дёргаем health, пока не оживёт
    let attempts = 0;
    const tick = async () => {
      attempts += 1;
      try {
        const r = await fetch("/healthz", { credentials: "same-origin" });
        if (r.ok) {
          if (msg) msg.textContent = `Готово: сервер снова отвечает (попыток ${attempts}). Обновите страницу.`;
          return;
        }
      } catch {
        /* ignored: пока перезапускается */
      }
      if (attempts < 30) setTimeout(tick, 1500);
      else if (msg) msg.textContent = "Сервер не ответил за 45 с. Проверьте логи сервиса.";
    };
    setTimeout(tick, 4000);
  } catch (e) {
    if (msg) msg.textContent = e.message || String(e);
  } finally {
    setButtonBusy(btn, false);
  }
}

async function refreshAdmin() {
  if (!document.getElementById("adminCards")) return;
  await refreshAdminRestartPanel().catch(() => {});
  const d = await api("/api/admin/overview");
  const cards = [
    { k: "Пользователей (аккаунты)", v: d.users_total },
    { k: "Организаций с подпиской", v: d.orgs_with_subscription },
    { k: "Активных организаций (active/trial)", v: d.active_orgs },
    { k: "Выручка / мес, оценка (руб.)", v: d.revenue_rub_month },
    { k: "Событий в CSV всего (строк)", v: d.events_total_csv_rows },
  ];
  setCards(document.getElementById("adminCards"), cards);

  const subKv = document.getElementById("adminSubStatusKv");
  if (subKv) {
    const subStatus = d.subscriptions_by_status || {};
    const keys = Object.keys(subStatus).sort();
    const rows = keys.map((k) => ({ k, v: subStatus[k] }));
    if (rows.length === 0) setKv(subKv, [{ k: "—", v: "Нет данных" }]);
    else setKv(subKv, rows);
  }

  const tEl = document.getElementById("adminTenantsTable");
  if (tEl) {
    const tenants = Array.isArray(d.tenants) ? d.tenants : [];
    tEl.innerHTML = "";
    if (tenants.length === 0) {
      const row = document.createElement("div");
      row.className = "tr";
      const t1 = document.createElement("div");
      t1.className = "title";
      t1.textContent = "—";
      const t2 = document.createElement("div");
      t2.textContent = "Нет данных по tenant-папкам org_*";
      row.appendChild(t1);
      row.appendChild(t2);
      tEl.appendChild(row);
    } else {
      for (const t of tenants) {
        const row = document.createElement("div");
        row.className = "tr wide";
        const t1 = document.createElement("div");
        t1.className = "title";
        t1.textContent = `org ${String(t.org_id ?? "")}`;
        const t2 = document.createElement("div");
        const lbl = document.createTextNode("Строк в sent_leads.csv: ");
        const strong = document.createElement("strong");
        strong.textContent = String(t.csv_rows ?? "");
        t2.appendChild(lbl);
        t2.appendChild(strong);
        row.appendChild(t1);
        row.appendChild(t2);
        tEl.appendChild(row);
      }
    }
  }

  const leg = document.getElementById("adminLegendKv");
  if (leg) {
    setKv(leg, [
      {
        k: "Пользователи",
        v: "users_total — сколько зарегистрированных аккаунтов в базе (таблица users).",
      },
      {
        k: "Подписка у org",
        v: "orgs_with_subscription — сколько организаций имеют запись в subscriptions (тариф привязан к org).",
      },
      {
        k: "Кто может работать",
        v: "active_orgs — org со статусом active или trial; обычно им разрешён запуск бота.",
      },
      {
        k: "Выручка",
        v: "revenue_rub_month — сумма price_rub_month по active/trial org; это оценка MRR, не факт оплаты.",
      },
      {
        k: "Статусы",
        v: "subscriptions_by_status — сколько org в каждом статусе (active/trial/paused/expired/banned).",
      },
      {
        k: "CSV всего",
        v: "events_total_csv_rows — сумма строк (без заголовка) во всех tenant sent_leads.csv; 0 если бот не писал или файлов нет.",
      },
      {
        k: "CSV по org",
        v: "tenants[].csv_rows — разрез по каждой org: сколько строк в её sent_leads.csv.",
      },
    ]);
  }

  await refreshUsers();
  await loadAdminPlansEditor();
}

async function loadAdminPlansEditor() {
  const table = document.getElementById("adminPlansTable");
  if (!table) return;
  let plans;
  try {
    const r = await api("/api/plans");
    plans = Array.isArray(r.plans) ? r.plans : [];
  } catch {
    table.innerHTML = "<p class=\"muted\">Не удалось загрузить планы.</p>";
    return;
  }
  table.innerHTML = "";
  for (const p of plans) {
    const id = String(p.id || "");
    if (!id) continue;
    const wrap = document.createElement("div");
    wrap.className = "panel-sub admin-plan-block";
    wrap.style.marginTop = "10px";
    const h = document.createElement("div");
    h.className = "meta strong";
    h.textContent = `План: ${id}`;
    wrap.appendChild(h);
    const grid = document.createElement("div");
    grid.className = "form-grid";
    const add = (label, key, val, type = "number") => {
      const lb = document.createElement("span");
      lb.className = "muted";
      lb.textContent = label;
      const inp = document.createElement("input");
      inp.className = "admin-plan-inp";
      inp.id = `adminPlan_${id}_${key}`;
      inp.type = type;
      inp.setAttribute("data-plan-field", key);
      inp.value = val != null && val !== undefined ? String(val) : "";
      if (type === "number") inp.setAttribute("inputmode", "numeric");
      grid.appendChild(lb);
      grid.appendChild(inp);
    };
    add("Название", "title", p.title, "text");
    add("Цена ₽/мес", "price_rub_month", p.price_rub_month);
    add("Макс. чатов", "max_chats", p.max_chats);
    add("ЛС/день (лимит тарифа)", "max_dm_day", p.max_dm_day);
    add("ЛС/мес (лимит тарифа)", "max_dm_month", p.max_dm_month);
    add("Мин. интервал монит. (сек)", "monitor_interval_min_sec", p.monitor_interval_min_sec);
    add("Макс. TG-аккаунтов", "max_telegram_accounts", p.max_telegram_accounts);
    wrap.appendChild(grid);
    const rowBtn = document.createElement("div");
    rowBtn.className = "row";
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "secondary";
    btn.textContent = "Сохранить";
    btn.setAttribute("data-admin-plan-save", id);
    rowBtn.appendChild(btn);
    wrap.appendChild(rowBtn);
    table.appendChild(wrap);
  }
}

async function saveAdminPlan(planId) {
  const id = String(planId || "").trim();
  if (!id) return;
  const fields = [
    "title",
    "price_rub_month",
    "max_chats",
    "max_dm_day",
    "max_dm_month",
    "monitor_interval_min_sec",
    "max_telegram_accounts",
  ];
  /** @type {Record<string, any>} */
  const body = {};
  for (const f of fields) {
    const el = document.getElementById(`adminPlan_${id}_${f}`);
    if (!el) continue;
    if (f === "title") {
      body[f] = String(el.value || "").trim();
    } else {
      const n = parseInt(String(el.value || "0"), 10);
      if (Number.isNaN(n)) {
        throw new Error(`Некорректное число: ${f}`);
      }
      body[f] = n;
    }
  }
  const d = await api(`/api/admin/plans/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  await loadAdminPlansEditor();
  return d;
}

function _adminUserFilterState() {
  const q = (document.getElementById("adminUserEmailSearch")?.value || "").trim().toLowerCase();
  const fromVal = document.getElementById("adminUserRegFrom")?.value || "";
  const toVal = document.getElementById("adminUserRegTo")?.value || "";
  /** @type {number | null} */
  let fromTs = null;
  /** @type {number | null} */
  let toTs = null;
  if (fromVal) fromTs = new Date(`${fromVal}T00:00:00`).getTime();
  if (toVal) toTs = new Date(`${toVal}T23:59:59.999`).getTime();
  return { q, fromTs, toTs };
}

function _userMatchesAdminFilters(u, f) {
  const email = String(u.email || "").toLowerCase();
  if (f.q && !email.includes(f.q)) return false;
  const created = u.created_at ? new Date(u.created_at).getTime() : NaN;
  const hasDateFilter = f.fromTs != null || f.toTs != null;
  if (hasDateFilter) {
    if (Number.isNaN(created)) return false;
    if (f.fromTs != null && created < f.fromTs) return false;
    if (f.toTs != null && created > f.toTs) return false;
  }
  return true;
}

function renderUsersTable() {
  const table = document.getElementById("usersTable");
  if (!table) return;
  const f = _adminUserFilterState();
  const all = adminUsersCache;
  const users = all.filter((u) => _userMatchesAdminFilters(u, f));
  const hint = document.getElementById("adminUsersFilterHint");
  if (hint) hint.textContent = `Показано: ${users.length} из ${all.length}`;

  table.innerHTML = "";

  let orgWarn = document.getElementById("adminMembershipOrgWarn");
  if (!orgWarn && table.parentElement) {
    orgWarn = document.createElement("p");
    orgWarn.id = "adminMembershipOrgWarn";
    orgWarn.className = "muted";
    orgWarn.style.marginBottom = "10px";
    table.parentElement.insertBefore(orgWarn, table);
  }
  if (orgWarn) {
    if (adminOrgsCache.length === 0) {
      orgWarn.textContent =
        "Список организаций пуст или не загрузился — блок «Организация и роль в org» ниже не показывается. Роль тестировщика задаётся там (memberships), а не в списке user/manager/admin.";
      orgWarn.classList.remove("hidden");
    } else {
      orgWarn.textContent = "";
      orgWarn.classList.add("hidden");
    }
  }

  for (const u of users) {
    const row = document.createElement("div");
    row.className = "tr wide";

    const left = document.createElement("div");
    left.className = "title admin-user-title";
    const avatar = document.createElement("img");
    avatar.className = "admin-user-avatar";
    avatar.width = 40;
    avatar.height = 40;
    avatar.alt = "";
    avatar.src = `/api/users/${u.id}/avatar`;
    const idSpan = document.createElement("span");
    idSpan.textContent = `#${u.id}`;
    left.appendChild(avatar);
    left.appendChild(idSpan);

    const right = document.createElement("div");
    right.style.display = "grid";
    right.style.gap = "8px";

    const top = document.createElement("div");
    const avHint = u.avatar_filename ? " • аватар" : "";
    top.textContent = `${u.email} • created: ${u.created_at || "—"}${avHint}`;

    const controls = document.createElement("div");
    controls.className = "row";

    const platLbl = document.createElement("span");
    platLbl.className = "muted";
    platLbl.title =
      "Глобальная роль аккаунта (таблица users). Не путать с ролью в организации — см. блок «Организация и роль в org» ниже (там есть tester).";
    platLbl.textContent = "Платформа:";

    const sel = document.createElement("select");
    sel.setAttribute("aria-label", "Глобальная роль пользователя (users.role)");
    for (const opt of ["user", "manager", "admin"]) {
      const o = document.createElement("option");
      o.value = opt;
      o.textContent = opt;
      sel.appendChild(o);
    }
    sel.value = u.role || "user";

    const applyRoleBtn = document.createElement("button");
    applyRoleBtn.className = "secondary";
    applyRoleBtn.textContent = "Роль";
    applyRoleBtn.addEventListener("click", async () => {
      const d = await api("/api/admin/user/role", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: u.id, role: sel.value }),
      });
      alert(d.message || "Готово");
      await refreshUsers();
    });

    const banBtn = document.createElement("button");
    banBtn.textContent = "Забанить";
    banBtn.addEventListener("click", async () => {
      const d = await api("/api/admin/user/block", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: u.id, status: "banned" }),
      });
      alert(d.message || "banned");
      await refreshUsers();
    });

    const unbanBtn = document.createElement("button");
    unbanBtn.className = "secondary";
    unbanBtn.textContent = "Разбанить";
    unbanBtn.addEventListener("click", async () => {
      const d = await api("/api/admin/user/block", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: u.id, status: "active" }),
      });
      alert(d.message || "active");
      await refreshUsers();
    });

    controls.appendChild(platLbl);
    controls.appendChild(sel);
    controls.appendChild(applyRoleBtn);
    controls.appendChild(banBtn);
    controls.appendChild(unbanBtn);

    const subLine = document.createElement("div");
    subLine.className = "row admin-subscription-row";
    const orgNote = document.createElement("span");
    orgNote.className = "muted";
    orgNote.textContent = u.org_id != null ? `org ${u.org_id}` : "org —";
    const planSub = document.createElement("select");
    for (const pid of ["free", "pro", "pro_plus"]) {
      const o = document.createElement("option");
      o.value = pid;
      o.textContent = pid;
      planSub.appendChild(o);
    }
    planSub.value = (u.sub_plan_id && String(u.sub_plan_id)) || "free";
    const stSub = document.createElement("select");
    for (const stv of ["active", "trial", "paused", "expired", "banned"]) {
      const o = document.createElement("option");
      o.value = stv;
      o.textContent = stv;
      stSub.appendChild(o);
    }
    stSub.value = (u.sub_status && String(u.sub_status)) || "active";
    const applySubBtn = document.createElement("button");
    applySubBtn.className = "secondary";
    applySubBtn.textContent = "Применить тариф";
    applySubBtn.addEventListener("click", async () => {
      const d = await api("/api/admin/user/subscription", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: u.id,
          plan_id: planSub.value,
          status: stSub.value,
        }),
      });
      alert(d.message || "Готово");
      await refreshUsers();
      await refreshAdmin();
    });
    subLine.appendChild(orgNote);
    subLine.appendChild(planSub);
    subLine.appendChild(stSub);
    subLine.appendChild(applySubBtn);

    const delBtn = document.createElement("button");
    delBtn.className = "secondary";
    delBtn.textContent = "Удалить";
    delBtn.addEventListener("click", async () => {
      if (currentUserId != null && u.id === currentUserId) {
        alert("Нельзя удалить собственный аккаунт");
        return;
      }
      if (!window.confirm(`Удалить пользователя ${u.email}? Это действие необратимо.`)) return;
      try {
        const d = await api("/api/admin/user/delete", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ user_id: u.id }),
        });
        alert(d.message || "Удалено");
        await refreshUsers();
      } catch (e) {
        alert(e.message || String(e));
      }
    });
    controls.appendChild(delBtn);

    right.appendChild(top);
    right.appendChild(controls);
    right.appendChild(subLine);

    if (adminOrgsCache.length > 0) {
      const orgRow = document.createElement("div");
      orgRow.className = "row admin-manager-org-row";
      orgRow.style.flexWrap = "wrap";
      orgRow.style.alignItems = "center";
      orgRow.style.gap = "8px";
      const orgLbl = document.createElement("span");
      orgLbl.className = "muted";
      orgLbl.textContent = "Роль в организации (есть tester):";
      const orgSel = document.createElement("select");
      orgSel.setAttribute("aria-label", "Организация пользователя");
      for (const o of adminOrgsCache) {
        const opt = document.createElement("option");
        const oid = o.id != null ? String(o.id) : "";
        opt.value = oid;
        const mc = o.members_count != null ? o.members_count : "—";
        opt.textContent = `${oid} — ${o.name || "org"} (участников: ${mc})`;
        orgSel.appendChild(opt);
      }
      if (u.org_id != null) {
        orgSel.value = String(u.org_id);
      }
      const roleSel = document.createElement("select");
      roleSel.setAttribute("aria-label", "Роль в организации");
      for (const r of ["client", "manager", "tester", "admin"]) {
        const ro = document.createElement("option");
        ro.value = r;
        ro.textContent = r;
        roleSel.appendChild(ro);
      }
      const curMem = String(u.org_membership_role || "client").trim();
      if (["client", "manager", "tester", "admin"].includes(curMem)) {
        roleSel.value = curMem;
      }
      const assignOrgBtn = document.createElement("button");
      assignOrgBtn.type = "button";
      assignOrgBtn.className = "secondary";
      assignOrgBtn.textContent = "Сохранить роль в org";
      assignOrgBtn.addEventListener("click", async () => {
        const oid = parseInt(orgSel.value, 10);
        if (Number.isNaN(oid) || oid <= 0) {
          alert("Выберите организацию");
          return;
        }
        try {
          const d = await api("/api/admin/membership/set", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ user_id: u.id, org_id: oid, role: roleSel.value }),
          });
          alert(d.message || "Готово");
          await refreshUsers();
          await refreshAdmin();
        } catch (e) {
          alert(e.message || String(e));
        }
      });
      orgRow.appendChild(orgLbl);
      orgRow.appendChild(orgSel);
      orgRow.appendChild(roleSel);
      orgRow.appendChild(assignOrgBtn);
      right.appendChild(orgRow);
    }

    row.appendChild(left);
    row.appendChild(right);
    table.appendChild(row);
  }
}

async function refreshUsers() {
  const table = document.getElementById("usersTable");
  if (!table) return;
  const [ur, or] = await Promise.all([
    api("/api/admin/users").catch(() => ({ users: [] })),
    api("/api/admin/orgs").catch(() => ({ orgs: [] })),
  ]);
  adminUsersCache = Array.isArray(ur.users) ? ur.users : [];
  adminOrgsCache = Array.isArray(or.orgs) ? or.orgs : [];
  renderUsersTable();
}

async function loadConfig() {
  if (!document.getElementById("configEditor")) return;
  const d = await api("/api/config");
  document.getElementById("configEditor").value = JSON.stringify(d, null, 2);
}

async function saveConfig() {
  const raw = document.getElementById("configEditor").value;
  const payload = JSON.parse(raw);
  const d = await api("/api/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  alert(d.message);
}

function linesToList(text) {
  return String(text || "")
    .split("\n")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

function listToLines(list) {
  if (!Array.isArray(list)) return "";
  return list.map((x) => String(x)).join("\n");
}

/** @type {any[] | null} */
window.__leadgenTgAccounts = null;
/** @type {any | null} */
window.__leadgenTgLastPrevCfg = null;
let tgAccUiBound = false;

/** Пересобрать пункты «Редактировать» и «Активный» из window.__leadgenTgAccounts (актуальные подписи). */
function rebuildTgAccountSelectOptions(opts = {}) {
  const accs = window.__leadgenTgAccounts;
  const edit = document.getElementById("cfgTgEdit");
  const act = document.getElementById("cfgTgActive");
  if (!Array.isArray(accs) || !accs.length || (!edit && !act)) return;
  const ids = new Set(accs.map((a) => a && a.id).filter(Boolean));
  const wantEdit =
    opts.editValue != null && ids.has(opts.editValue)
      ? opts.editValue
      : edit && ids.has(edit.value)
        ? edit.value
        : accs[0].id;
  const wantAct =
    opts.actValue != null && ids.has(opts.actValue)
      ? opts.actValue
      : act && ids.has(act.value)
        ? act.value
        : accs[0].id;
  for (const sel of [edit, act]) {
    if (!sel) continue;
    sel.innerHTML = "";
    for (const a of accs) {
      if (!a || !a.id) continue;
      const o = document.createElement("option");
      o.value = a.id;
      const lb = String(a.label || "").trim();
      o.textContent = lb ? `${lb} (${a.id})` : a.id;
      sel.appendChild(o);
    }
  }
  if (edit) edit.value = wantEdit;
  if (act) act.value = wantAct;
}

function updateTgAccountSyncHint() {
  const el = document.getElementById("cfgTgSyncHint");
  const edit = document.getElementById("cfgTgEdit");
  const act = document.getElementById("cfgTgActive");
  if (!el || !edit || !act) return;
  if (edit.value === act.value) {
    el.textContent = "";
    el.classList.add("hidden");
    return;
  }
  el.textContent =
    "Поля api_id / hash / телефон сейчас относятся к «Редактировать», а не к «Активный (бот)». Сохранение запишет их в выбранный в «Редактировать» аккаунт.";
  el.classList.remove("hidden");
}

/**
 * Сохранить поля формы TG в буфер аккаунта.
 * @param {string | null} targetAccountId — если задан, пишем в этот id (при смене select к моменту
 *   события change у селекта уже новый value, а поля формы ещё от предыдущего аккаунта).
 */
function saveCurrentTgFormToBuffer(targetAccountId = null) {
  const editSel = document.getElementById("cfgTgEdit");
  if (!editSel || !window.__leadgenTgAccounts) return;
  const id =
    targetAccountId != null && String(targetAccountId).trim() !== ""
      ? String(targetAccountId)
      : editSel.value;
  const acc = window.__leadgenTgAccounts.find((a) => a && a.id === id);
  if (!acc) return;
  const g = (i) => document.getElementById(i);
  acc.label = String(g("cfgTgLabel")?.value || acc.label || "").trim() || acc.id;
  const apiIdStr = String(g("cfgApiId")?.value ?? "").trim();
  acc.api_id = apiIdStr === "" ? "" : toInt(apiIdStr, 0);
  const hashRaw = String(g("cfgApiHash")?.value || "").trim();
  if (hashRaw && hashRaw !== "••••••••") {
    acc.api_hash = hashRaw;
  }
  acc.phone = String(g("cfgPhone")?.value || "").trim();
}

function loadTgAccountIntoForm(id, prevCfg) {
  const g = (i) => document.getElementById(i);
  const acc = (window.__leadgenTgAccounts || []).find((a) => a && a.id === id);
  if (!acc) return;
  if (g("cfgTgLabel")) g("cfgTgLabel").value = acc.label || "";
  if (g("cfgApiId")) {
    g("cfgApiId").value =
      acc.api_id !== undefined && acc.api_id !== null && String(acc.api_id).trim() !== ""
        ? String(acc.api_id)
        : "";
  }
  const pA =
    prevCfg && Array.isArray(prevCfg.telegram_accounts)
      ? prevCfg.telegram_accounts.find((x) => x && x.id === acc.id)
      : null;
  const hasHash = Boolean(
    String(acc.api_hash || "").trim() || (pA && String(pA.api_hash || "").trim()),
  );
  if (g("cfgApiHash")) g("cfgApiHash").value = hasHash ? "••••••••" : "";
  if (g("cfgPhone")) g("cfgPhone").value = acc.phone || "";
  const h = g("cfgTgSessionHint");
  if (h) {
    const st = String(acc.session_stem || "").trim();
    h.textContent = st
      ? `Файл сессии: sessions/${st}.session`
      : "Файл сессии: будет сгенерирован при сохранении (отдельный файл на аккаунт).";
  }
  const ed = document.getElementById("cfgTgEdit");
  if (ed) ed.dataset.leadgenPrevAcc = String(id);
}

function initTgAccountsPanel(cfg, prevCfg) {
  if (!document.getElementById("cfgTgEdit")) return;
  let accs = Array.isArray(cfg.telegram_accounts) ? cfg.telegram_accounts : null;
  if (!accs || !accs.length) {
    accs = [
      {
        id: "default",
        label: "Основной",
        api_id: cfg.api_id,
        api_hash: cfg.api_hash || "",
        phone: cfg.phone || "",
        session_stem: "",
      },
    ];
  }
  window.__leadgenTgAccounts = accs.map((a) => ({ ...a }));
  const pAccs =
    prevCfg && Array.isArray(prevCfg.telegram_accounts) ? prevCfg.telegram_accounts : [];
  for (const a of window.__leadgenTgAccounts) {
    const p = pAccs.find((x) => x && x.id === a.id);
    if (p && String(p.api_hash || "").trim() && !String(a.api_hash || "").trim()) {
      a.api_hash = p.api_hash;
    }
  }
  const actIdRaw = cfg.active_telegram_account || (window.__leadgenTgAccounts[0] && window.__leadgenTgAccounts[0].id);
  const ids = new Set(window.__leadgenTgAccounts.map((a) => a && a.id).filter(Boolean));
  const actId = actIdRaw && ids.has(actIdRaw) ? actIdRaw : window.__leadgenTgAccounts[0].id;
  rebuildTgAccountSelectOptions({ editValue: actId, actValue: actId });
  loadTgAccountIntoForm(actId, prevCfg);
  updateTgAccountSyncHint();
}

function bindTgAccountUiOnce() {
  if (tgAccUiBound) return;
  const edit = document.getElementById("cfgTgEdit");
  const add = document.getElementById("cfgTgAddBtn");
  const rem = document.getElementById("cfgTgRemoveBtn");
  if (!edit && !add) return;
  tgAccUiBound = true;
  if (edit) {
    edit.addEventListener("change", () => {
      const prevAcc = String(edit.dataset.leadgenPrevAcc || "").trim();
      const nextAcc = edit.value;
      if (prevAcc && prevAcc !== nextAcc) {
        saveCurrentTgFormToBuffer(prevAcc);
      } else {
        saveCurrentTgFormToBuffer(nextAcc);
      }
      rebuildTgAccountSelectOptions({ editValue: nextAcc, actValue: document.getElementById("cfgTgActive")?.value });
      loadTgAccountIntoForm(nextAcc, window.__leadgenTgLastPrevCfg);
      updateTgAccountSyncHint();
    });
  }
  const actSel = document.getElementById("cfgTgActive");
  if (actSel) {
    actSel.addEventListener("change", () => {
      saveCurrentTgFormToBuffer();
      const newActive = actSel.value;
      const editEl = document.getElementById("cfgTgEdit");
      if (editEl) editEl.value = newActive;
      rebuildTgAccountSelectOptions({ editValue: newActive, actValue: newActive });
      loadTgAccountIntoForm(newActive, window.__leadgenTgLastPrevCfg);
      updateTgAccountSyncHint();
    });
  }
  const lblInp = document.getElementById("cfgTgLabel");
  if (lblInp) {
    let lblTimer = 0;
    lblInp.addEventListener("input", () => {
      window.clearTimeout(lblTimer);
      lblTimer = window.setTimeout(() => {
        saveCurrentTgFormToBuffer();
        const ed = document.getElementById("cfgTgEdit");
        const ac = document.getElementById("cfgTgActive");
        rebuildTgAccountSelectOptions({ editValue: ed && ed.value, actValue: ac && ac.value });
        updateTgAccountSyncHint();
      }, 160);
    });
  }
  if (add) {
    add.addEventListener("click", () => {
      saveCurrentTgFormToBuffer();
      const newId = `acc${Math.random().toString(36).slice(2, 9)}`;
      window.__leadgenTgAccounts = window.__leadgenTgAccounts || [];
      window.__leadgenTgAccounts.push({
        id: newId,
        label: "Доп. аккаунт",
        api_id: "",
        api_hash: "",
        phone: "",
        session_stem: "",
      });
      const prev = window.__leadgenTgLastPrevCfg;
      const actEl = document.getElementById("cfgTgActive");
      const curActive = actEl && actEl.value ? actEl.value : newId;
      initTgAccountsPanel(
        { telegram_accounts: window.__leadgenTgAccounts, active_telegram_account: curActive },
        prev,
      );
      rebuildTgAccountSelectOptions({ editValue: newId, actValue: curActive });
      loadTgAccountIntoForm(newId, prev);
      updateTgAccountSyncHint();
    });
  }
  if (rem) {
    rem.addEventListener("click", () => {
      if (!window.__leadgenTgAccounts || window.__leadgenTgAccounts.length <= 1) {
        alert("Нужен хотя бы один аккаунт.");
        return;
      }
      saveCurrentTgFormToBuffer();
      const e2 = document.getElementById("cfgTgEdit");
      const id = e2 && e2.value;
      window.__leadgenTgAccounts = window.__leadgenTgAccounts.filter((a) => a && a.id !== id);
      const prev = window.__leadgenTgLastPrevCfg;
      const actEl = document.getElementById("cfgTgActive");
      let av = actEl && actEl.value ? actEl.value : "";
      if (!window.__leadgenTgAccounts.some((a) => a.id === av)) {
        av = window.__leadgenTgAccounts[0].id;
      }
      initTgAccountsPanel(
        { telegram_accounts: window.__leadgenTgAccounts, active_telegram_account: av },
        prev,
      );
      updateTgAccountSyncHint();
    });
  }
}

function mergeTgHashesFromPrev(cfg, prevCfg) {
  if (!prevCfg || !Array.isArray(cfg.telegram_accounts)) return;
  const prevAccs = Array.isArray(prevCfg.telegram_accounts) ? prevCfg.telegram_accounts : [];
  for (const a of cfg.telegram_accounts) {
    if (!a || !a.id) continue;
    const pr = prevAccs.find((x) => x && x.id === a.id);
    const h = String(a.api_hash || "").trim();
    if ((!h || h === "••••••••") && pr && String(pr.api_hash || "").trim()) {
      a.api_hash = pr.api_hash;
    }
  }
}

function toInt(v, fallback = 0) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.trunc(n);
}

function toFloat(v, fallback = 0) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

function parseChats(text) {
  const out = [];
  for (const line of linesToList(text)) {
    if (/^-?\d+$/.test(line)) out.push(parseInt(line, 10));
    else out.push(line);
  }
  return out;
}

/** Нормализация одной строки чата как в target_chats (число или строка). */
function normalizeTargetChatToken(line) {
  const s = String(line ?? "").trim();
  if (!s) return null;
  if (/^-?\d+$/.test(s)) return parseInt(s, 10);
  return s;
}

/** Варианты ключа для пересечения с channel_search_exclude (как в web_app._channel_search_exclude_key_variants). */
function channelSearchExcludeVariantKeys(val) {
  const out = new Set();
  let s =
    typeof val === "number" && Number.isFinite(val)
      ? String(Math.trunc(val))
      : String(val ?? "").trim();
  if (!s) return out;
  out.add(s);
  const low = s.toLowerCase();
  if (s.startsWith("@")) {
    out.add(low);
    out.add(low.slice(1));
  } else if (/^-?\d+$/.test(s)) {
    out.add(s);
    if (s.startsWith("-100")) out.add(s.slice(4));
  } else {
    out.add(low);
    out.add(`@${low}`);
  }
  return out;
}

/** Варианты результата поиска (_search_result_item_key_variants на сервере). */
function searchChannelRowVariantKeys(ch) {
  const out = new Set();
  const pid = ch.id;
  if (pid != null && String(pid).trim()) {
    for (const k of channelSearchExcludeVariantKeys(String(pid).trim())) out.add(k);
  }
  const u = (ch.username || "").trim();
  if (u) {
    const ref = u.startsWith("@") ? u : `@${u}`;
    for (const k of channelSearchExcludeVariantKeys(ref)) out.add(k);
  }
  return out;
}

function searchChannelRowMatchesExclude(ch, exList) {
  const rk = searchChannelRowVariantKeys(ch);
  for (const e of exList || []) {
    const ek = channelSearchExcludeVariantKeys(typeof e === "number" ? e : e);
    for (const x of rk) {
      if (ek.has(x)) return true;
    }
  }
  return false;
}

async function mergeRefsIntoChannelSearchExclude(refs) {
  const uniq = [...new Set((refs || []).map((r) => String(r).trim()).filter(Boolean))];
  if (!uniq.length) return null;
  const prev = await api("/api/config");
  const cur = Array.isArray(prev.channel_search_exclude) ? [...prev.channel_search_exclude] : [];
  const seen = new Set();
  for (const existing of cur) seen.add(monitorChatRefKey(existing));
  for (const r of uniq) {
    const tok = normalizeTargetChatToken(r);
    if (tok == null) continue;
    const k = monitorChatRefKey(tok);
    if (seen.has(k)) continue;
    seen.add(k);
    cur.push(tok);
  }
  const d = await api("/api/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...prev, channel_search_exclude: cur }),
  });
  window.__monitorChatsExcludeList = cur;
  searchChannelsMaster = searchChannelsMaster.filter((row) => !searchChannelRowMatchesExclude(row, cur));
  renderMonitorChatsExcludeTable();
  renderSearchChannelsTable();
  return d;
}

function monitorChatRefKey(c) {
  if (typeof c === "number" && Number.isFinite(c)) return `n:${c}`;
  const s = String(c).trim();
  if (/^-?\d+$/.test(s)) return `n:${parseInt(s, 10)}`;
  const u = s.startsWith("@") ? s.slice(1).toLowerCase() : s.toLowerCase();
  return `s:${u}`;
}

function monitorExcludeSetFromList(exList) {
  const out = new Set();
  for (const x of exList || []) out.add(monitorChatRefKey(x));
  return out;
}

function isMonitorChatExcluded(chatRef, exSet) {
  return exSet.has(monitorChatRefKey(chatRef));
}

async function refreshMonitorChatsExcludePanel() {
  const wrap = document.getElementById("monitorChatsExcludeTable");
  if (!wrap) return;
  let cfg;
  try {
    cfg = await api("/api/config");
  } catch {
    return;
  }
  window.__monitorChatsTargetList = Array.isArray(cfg.target_chats) ? cfg.target_chats : [];
  window.__monitorChatsExcludeList = Array.isArray(cfg.channel_search_exclude) ? cfg.channel_search_exclude : [];
  renderMonitorChatsExcludeTable();
}

function renderMonitorChatsExcludeTable() {
  const wrap = document.getElementById("monitorChatsExcludeTable");
  const emptyEl = document.getElementById("monitorChatsExcludeEmpty");
  const filtEl = document.getElementById("monitorChatsFilter");
  const msgEl = document.getElementById("monitorChatsExcludeMsg");
  if (!wrap || !emptyEl) return;
  const chats = window.__monitorChatsTargetList || [];
  const exSet = monitorExcludeSetFromList(window.__monitorChatsExcludeList || []);
  const q = (filtEl && filtEl.value.trim().toLowerCase()) || "";
  if (msgEl) {
    msgEl.textContent = "";
    msgEl.classList.add("hidden");
  }
  wrap.innerHTML = "";
  if (!chats.length) {
    wrap.classList.add("hidden");
    emptyEl.classList.remove("hidden");
    return;
  }
  emptyEl.classList.add("hidden");
  wrap.classList.remove("hidden");
  const header = document.createElement("div");
  header.className = "tr monitor-exclude-row";
  const h1 = document.createElement("div");
  h1.className = "title";
  h1.textContent = "Чат";
  const h2 = document.createElement("div");
  h2.className = "title";
  h2.textContent = "В поиске";
  header.appendChild(h1);
  header.appendChild(h2);
  wrap.appendChild(header);
  let visible = 0;
  for (const ref of chats) {
    const disp = String(ref);
    if (q && !disp.toLowerCase().includes(q)) continue;
    visible += 1;
    const row = document.createElement("div");
    row.className = "tr monitor-exclude-row";
    row.dataset.monitorChat = typeof ref === "number" ? String(ref) : String(ref);
    const c1 = document.createElement("div");
    c1.setAttribute("dir", "auto");
    c1.textContent = disp;
    const c2 = document.createElement("div");
    const lab = document.createElement("label");
    lab.className = "check-row";
    const inp = document.createElement("input");
    inp.type = "checkbox";
    inp.className = "checkbox";
    inp.title = "Не показывать этот канал/чат в результатах глобального поиска на странице";
    inp.checked = isMonitorChatExcluded(ref, exSet);
    lab.appendChild(inp);
    lab.appendChild(document.createTextNode(" исключить"));
    c2.appendChild(lab);
    row.appendChild(c1);
    row.appendChild(c2);
    wrap.appendChild(row);
  }
  if (!visible) {
    const hint = document.createElement("div");
    hint.className = "tr monitor-exclude-row muted";
    const one = document.createElement("div");
    one.style.gridColumn = "1 / -1";
    one.textContent = q ? "Нет чатов, совпадающих с фильтром." : "";
    hint.appendChild(one);
    wrap.appendChild(hint);
  }
}

async function saveMonitorChatsExcludePanel() {
  const wrap = document.getElementById("monitorChatsExcludeTable");
  const msg = document.getElementById("monitorChatsExcludeMsg");
  if (!wrap) return;
  const chats = window.__monitorChatsTargetList || [];
  const prevExSet = monitorExcludeSetFromList(window.__monitorChatsExcludeList || []);
  /** @type {Map<string, Element>} */
  const rowByAttr = new Map();
  wrap.querySelectorAll(".tr.monitor-exclude-row[data-monitor-chat]").forEach((row) => {
    const raw = row.getAttribute("data-monitor-chat");
    if (raw != null) rowByAttr.set(raw, row);
  });
  const excluded = [];
  for (const ref of chats) {
    const attrKey =
      typeof ref === "number" && Number.isFinite(ref) ? String(Math.trunc(ref)) : String(ref ?? "").trim();
    const row = rowByAttr.get(attrKey);
    let on = false;
    if (row) {
      const chk = row.querySelector('input[type="checkbox"]');
      on = !!(chk && chk.checked);
    } else {
      on = prevExSet.has(monitorChatRefKey(ref));
    }
    if (!on) continue;
    const norm = normalizeTargetChatToken(ref);
    if (norm != null) excluded.push(norm);
  }
  try {
    const prev = await api("/api/config");
    const d = await api("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...prev, channel_search_exclude: excluded }),
    });
    window.__monitorChatsExcludeList = excluded;
    if (msg) {
      msg.textContent = d.message || "Сохранено";
      msg.classList.remove("hidden");
    }
    renderMonitorChatsExcludeTable();
  } catch (e) {
    if (msg) {
      msg.textContent = e.message || String(e);
      msg.classList.remove("hidden");
    } else alert(e.message || e);
  }
}

/** Подставляет тексты промптов LLM из GET /api/config (слитые умолчания + overrides). */
function fillLlmPromptTextareas(cfg) {
  const lp = cfg.llm_prompts;
  if (!lp || typeof lp !== "object") return;
  document.querySelectorAll("textarea[data-prompt-key]").forEach((el) => {
    const k = el.getAttribute("data-prompt-key");
    if (!k) return;
    const v = lp[k];
    el.value = typeof v === "string" ? v : "";
  });
}

/** Только admin: все поля промптов уходят на сервер для нормализации относительно дефолтов. */
function collectLlmPromptTextareasInto(cfg) {
  if (window.__leadgenOrgRole !== "admin") return;
  const nodes = document.querySelectorAll("textarea[data-prompt-key]");
  if (!nodes.length) return;
  const next = {};
  nodes.forEach((el) => {
    const k = el.getAttribute("data-prompt-key");
    if (!k) return;
    next[k] = String(el.value ?? "");
  });
  cfg.llm_prompts = next;
}

function fillConfigForm(cfg) {
  const g = (id) => document.getElementById(id);
  if (g("cfgDryRun")) g("cfgDryRun").checked = Boolean(cfg.dry_run);
  if (g("cfgHumanApproval")) g("cfgHumanApproval").checked = Boolean(cfg.human_approval_for_dm);
  const ha = cfg.human_approval_stages;
  const stageOn = (k, defVal) =>
    ha && typeof ha === "object" && Object.prototype.hasOwnProperty.call(ha, k)
      ? Boolean(ha[k])
      : defVal;
  if (g("cfgApprStage1")) g("cfgApprStage1").checked = stageOn("stage1", true);
  if (g("cfgApprStage2")) g("cfgApprStage2").checked = stageOn("stage2", true);
  if (g("cfgApprStage3")) g("cfgApprStage3").checked = stageOn("stage3", true);
  if (g("cfgPartnerName")) g("cfgPartnerName").value = cfg.partner_name || "";

  if (document.getElementById("cfgTgEdit")) {
    initTgAccountsPanel(cfg, window.__leadgenTgLastPrevCfg);
  } else {
    if (g("cfgApiId")) {
      g("cfgApiId").value =
        cfg.api_id !== undefined && cfg.api_id !== null && String(cfg.api_id).trim() !== ""
          ? String(cfg.api_id)
          : "";
    }
    if (g("cfgApiHash")) g("cfgApiHash").value = cfg.api_hash ? "••••••••" : "";
    if (g("cfgPhone")) g("cfgPhone").value = cfg.phone || "";
    if (g("cfgSessionName")) g("cfgSessionName").value = cfg.session_name || "";
  }

  if (g("cfgTargetChats")) g("cfgTargetChats").value = listToLines(cfg.target_chats || []);

  const kw = cfg.keywords || {};
  if (g("kwHot")) g("kwHot").value = listToLines(kw.hot_lead || []);
  if (g("kwNegative")) g("kwNegative").value = listToLines(kw.negative || []);
  if (g("kwExclude")) g("kwExclude").value = listToLines(kw.exclude_hot_lead || []);
  if (g("kwIntent")) g("kwIntent").value = listToLines(kw.required_intent_hot_lead || []);
  if (g("kwQual")) g("kwQual").value = listToLines(kw.qualification || []);
  if (g("kwInterested")) g("kwInterested").value = listToLines(kw.interested || []);
  if (g("kwBioBlock")) g("kwBioBlock").value = listToLines(kw.bio_block || []);

  const tpl = cfg.templates || {};
  if (g("tplStage1")) g("tplStage1").value = tpl.stage1 || "";
  if (g("tplStage2")) g("tplStage2").value = tpl.stage2 || "";
  if (g("tplStage3")) g("tplStage3").value = tpl.stage3 || "";

  const lim = cfg.limits || {};
  if (g("limMonitor")) g("limMonitor").value = lim.monitor_interval_sec ?? "";
  if (g("limFetch")) g("limFetch").value = lim.fetch_limit_per_chat ?? "";
  if (g("limDmHourChat")) g("limDmHourChat").value = lim.max_dm_per_hour_per_chat ?? "";
  if (Array.isArray(lim.daily_limit_range)) {
    if (g("limDayMin")) g("limDayMin").value = lim.daily_limit_range[0] ?? "";
    if (g("limDayMax")) g("limDayMax").value = lim.daily_limit_range[1] ?? "";
  }
  if (g("limMonth")) g("limMonth").value = lim.max_dm_month ?? "";
  if (g("limMaxPasses")) g("limMaxPasses").value = lim.max_monitor_passes != null ? String(lim.max_monitor_passes) : "0";
  if (Array.isArray(lim.typing_delay_sec)) {
    if (g("limTypingMin")) g("limTypingMin").value = lim.typing_delay_sec[0] ?? "";
    if (g("limTypingMax")) g("limTypingMax").value = lim.typing_delay_sec[1] ?? "";
  }
  if (Array.isArray(lim.per_chat_scan_delay_sec)) {
    if (g("limScanMin")) g("limScanMin").value = lim.per_chat_scan_delay_sec[0] ?? "";
    if (g("limScanMax")) g("limScanMax").value = lim.per_chat_scan_delay_sec[1] ?? "";
  }
  const sd = lim.stage_delays_sec || {};
  if (Array.isArray(sd.stage1)) {
    if (g("limS1Min")) g("limS1Min").value = sd.stage1[0] ?? "";
    if (g("limS1Max")) g("limS1Max").value = sd.stage1[1] ?? "";
  }
  if (Array.isArray(sd.stage2)) {
    if (g("limS2Min")) g("limS2Min").value = sd.stage2[0] ?? "";
    if (g("limS2Max")) g("limS2Max").value = sd.stage2[1] ?? "";
  }
  if (Array.isArray(sd.stage3)) {
    if (g("limS3Min")) g("limS3Min").value = sd.stage3[0] ?? "";
    if (g("limS3Max")) g("limS3Max").value = sd.stage3[1] ?? "";
  }

  const sch = lim.schedule || {};
  if (g("schEnabled")) g("schEnabled").checked = Boolean(sch.enabled);
  if (g("schTz")) g("schTz").value = sch.timezone != null ? String(sch.timezone) : "Europe/Moscow";
  if (Array.isArray(sch.active_hours)) {
    if (g("schStart")) g("schStart").value = sch.active_hours[0] ?? "";
    if (g("schEnd")) g("schEnd").value = sch.active_hours[1] ?? "";
  } else {
    if (g("schStart")) g("schStart").value = 9;
    if (g("schEnd")) g("schEnd").value = 21;
  }

  const fh = lim.stage_followup_hours || {};
  const fillFh = (stageKey, minId, maxId) => {
    const p = fh[stageKey];
    if (Array.isArray(p) && p.length >= 2) {
      if (g(minId)) g(minId).value = p[0] ?? "";
      if (g(maxId)) g(maxId).value = p[1] ?? "";
    } else {
      if (g(minId)) g(minId).value = "";
      if (g(maxId)) g(maxId).value = "";
    }
  };
  fillFh("stage1", "limFh1Min", "limFh1Max");
  fillFh("stage2", "limFh2Min", "limFh2Max");
  fillFh("stage3", "limFh3Min", "limFh3Max");

  const llm = cfg.llm || {};
  const keyConfigured = cfg.llm_key_configured === true;
  const isAdmin = window.__leadgenOrgRole === "admin";
  if (g("llmEnabled")) g("llmEnabled").checked = Boolean(llm.enabled);
  if (g("llmBaseUrl")) g("llmBaseUrl").value = llm.base_url || "https://api.openai.com/v1";
  if (g("llmModel")) g("llmModel").value = llm.model || "gpt-4o-mini";
  if (g("llmApiKey")) {
    if (isAdmin) {
      g("llmApiKey").value = llm.api_key ? "••••••••" : "";
    } else {
      g("llmApiKey").value = keyConfigured ? "••••••••" : "";
    }
  }
  const csq =
    cfg.channel_search_quality && typeof cfg.channel_search_quality === "object" ? cfg.channel_search_quality : {};
  const csqWin = String(csq.window_sec != null ? csq.window_sec : 86400);
  const winOk = csqWin === "3600" || csqWin === "86400" || csqWin === "604800";
  if (g("csqEnabled")) g("csqEnabled").checked = Boolean(csq.enabled);
  if (g("csqDebugMetrics")) g("csqDebugMetrics").checked = Boolean(csq.debug_metrics);
  if (g("csqForceRequireDiscussion")) g("csqForceRequireDiscussion").checked = Boolean(csq.force_require_discussion);
  if (g("csqWindowSec")) g("csqWindowSec").value = winOk ? csqWin : "86400";
  if (g("csqSampleMax")) g("csqSampleMax").value = csq.sample_messages_max != null ? String(csq.sample_messages_max) : "200";
  if (g("csqPerRowDelay"))
    g("csqPerRowDelay").value =
      csq.per_row_delay_sec != null && csq.per_row_delay_sec !== "" ? String(csq.per_row_delay_sec) : "0.18";
  if (g("csqMinAboutLen")) g("csqMinAboutLen").value = csq.min_about_len != null ? String(csq.min_about_len) : "12";
  if (g("csqMinAuthors"))
    g("csqMinAuthors").value = csq.discussion_min_unique_authors != null ? String(csq.discussion_min_unique_authors) : "0";
  if (g("csqMaxTopAuthorPct"))
    g("csqMaxTopAuthorPct").value =
      csq.discussion_max_top_author_pct != null ? String(csq.discussion_max_top_author_pct) : "100";
  if (g("csqMinMessages"))
    g("csqMinMessages").value = csq.discussion_min_messages != null ? String(csq.discussion_min_messages) : "0";
  if (g("csqMinMeanLen"))
    g("csqMinMeanLen").value = csq.min_mean_message_len != null ? String(csq.min_mean_message_len) : "0";
  if (g("csqMaxDupPct"))
    g("csqMaxDupPct").value = csq.max_duplicate_text_pct != null ? String(csq.max_duplicate_text_pct) : "100";
  if (g("csqMaxPromoPct"))
    g("csqMaxPromoPct").value = csq.max_promo_ratio_pct != null ? String(csq.max_promo_ratio_pct) : "100";
  if (g("csqMaxForwardPct"))
    g("csqMaxForwardPct").value = csq.max_forward_ratio_pct != null ? String(csq.max_forward_ratio_pct) : "100";
  if (g("csqMinQuestionPct"))
    g("csqMinQuestionPct").value = csq.min_question_ratio_pct != null ? String(csq.min_question_ratio_pct) : "0";
  if (g("csqMinReplyPct"))
    g("csqMinReplyPct").value = csq.min_reply_ratio_pct != null ? String(csq.min_reply_ratio_pct) : "0";
  if (g("csqMinCyrillicPct"))
    g("csqMinCyrillicPct").value = csq.min_cyrillic_ratio_pct != null ? String(csq.min_cyrillic_ratio_pct) : "0";
  if (g("csqMinKeywordHits"))
    g("csqMinKeywordHits").value = csq.min_keyword_hits != null ? String(csq.min_keyword_hits) : "1";
  if (g("csqMinQualityScore"))
    g("csqMinQualityScore").value = csq.min_quality_score != null ? String(csq.min_quality_score) : "0";
  if (g("csqExcludeEmptyAbout")) g("csqExcludeEmptyAbout").checked = Boolean(csq.exclude_empty_about);
  if (g("csqExcludeNoContact")) g("csqExcludeNoContact").checked = Boolean(csq.exclude_no_contact_in_about);
  if (g("csqExcludeSingleAuthor")) g("csqExcludeSingleAuthor").checked = Boolean(csq.exclude_single_author_discussion);
  if (g("csqExcludeForwardHeavy")) g("csqExcludeForwardHeavy").checked = Boolean(csq.exclude_forward_heavy);
  if (g("csqNicheOnly")) g("csqNicheOnly").checked = Boolean(csq.niche_keywords_only);
  if (g("csqRussianOnly")) g("csqRussianOnly").checked = Boolean(csq.russian_only);
  const srcs = new Set(Array.isArray(csq.niche_keyword_sources) ? csq.niche_keyword_sources.map(String) : []);
  if (srcs.size === 0) {
    srcs.add("hot_lead");
    srcs.add("required_intent_hot_lead");
  }
  const setKw = (id, val) => {
    const el = g(id);
    if (el) el.checked = srcs.has(val);
  };
  setKw("csqKwHot", "hot_lead");
  setKw("csqKwIntent", "required_intent_hot_lead");
  setKw("csqKwQual", "qualification");
  setKw("csqKwInterested", "interested");
  if (g("csqExtraKeywords")) g("csqExtraKeywords").value = String(csq.extra_keywords || "");

  fillLlmPromptTextareas(cfg);
}

function buildConfigFromForm(prevCfg) {
  const g = (id) => document.getElementById(id);
  const cfg = { ...(prevCfg || {}) };
  cfg.dry_run = Boolean(g("cfgDryRun")?.checked);
  cfg.human_approval_for_dm = Boolean(g("cfgHumanApproval")?.checked);
  cfg.human_approval_stages = {
    stage1: Boolean(g("cfgApprStage1")?.checked),
    stage2: Boolean(g("cfgApprStage2")?.checked),
    stage3: Boolean(g("cfgApprStage3")?.checked),
  };
  cfg.partner_name = String(g("cfgPartnerName")?.value || "").trim();

  if (g("cfgTgEdit") && window.__leadgenTgAccounts) {
    saveCurrentTgFormToBuffer();
    cfg.telegram_accounts = JSON.parse(JSON.stringify(window.__leadgenTgAccounts));
    const actEl = g("cfgTgActive");
    cfg.active_telegram_account =
      (actEl && actEl.value) || cfg.active_telegram_account || cfg.telegram_accounts[0].id;
    mergeTgHashesFromPrev(cfg, prevCfg);
    const actId = cfg.active_telegram_account;
    const cur = cfg.telegram_accounts.find((x) => x && x.id === actId) || cfg.telegram_accounts[0];
    if (cur) {
      const apiIdStr = String(cur.api_id ?? "").trim();
      cfg.api_id = apiIdStr === "" ? "" : toInt(apiIdStr, 0);
      cfg.api_hash = String(cur.api_hash || "");
      cfg.phone = String(cur.phone || "").trim();
      cfg.session_name = String(cur.session_stem || "").trim();
    }
  } else {
    const apiIdStr = String(g("cfgApiId")?.value ?? "").trim();
    cfg.api_id = apiIdStr === "" ? "" : toInt(apiIdStr, 0);
    const hashRaw = String(g("cfgApiHash")?.value || "").trim();
    if (hashRaw && hashRaw !== "••••••••") {
      cfg.api_hash = hashRaw;
    } else if (prevCfg.api_hash) {
      cfg.api_hash = prevCfg.api_hash;
    } else {
      cfg.api_hash = "";
    }
    cfg.phone = String(g("cfgPhone")?.value || "").trim();
    cfg.session_name = String(g("cfgSessionName")?.value || "").trim();
  }

  const tcEl = g("cfgTargetChats");
  if (tcEl) {
    cfg.target_chats = parseChats(tcEl.value || "");
  } else if (Array.isArray(prevCfg.target_chats)) {
    cfg.target_chats = prevCfg.target_chats;
  } else {
    cfg.target_chats = [];
  }

  cfg.keywords = cfg.keywords || {};
  cfg.keywords.hot_lead = linesToList(g("kwHot")?.value || "");
  cfg.keywords.negative = linesToList(g("kwNegative")?.value || "");
  cfg.keywords.exclude_hot_lead = linesToList(g("kwExclude")?.value || "");
  cfg.keywords.required_intent_hot_lead = linesToList(g("kwIntent")?.value || "");
  cfg.keywords.qualification = linesToList(g("kwQual")?.value || "");
  cfg.keywords.interested = linesToList(g("kwInterested")?.value || "");
  cfg.keywords.bio_block = linesToList(g("kwBioBlock")?.value || "");

  cfg.templates = cfg.templates || {};
  cfg.templates.stage1 = String(g("tplStage1")?.value || "");
  cfg.templates.stage2 = String(g("tplStage2")?.value || "");
  cfg.templates.stage3 = String(g("tplStage3")?.value || "");

  cfg.limits = cfg.limits || {};
  cfg.limits.monitor_interval_sec = toInt(g("limMonitor")?.value, 10);
  cfg.limits.fetch_limit_per_chat = toInt(g("limFetch")?.value, 50);
  cfg.limits.max_dm_per_hour_per_chat = toInt(g("limDmHourChat")?.value, 5);
  cfg.limits.daily_limit_range = [toInt(g("limDayMin")?.value, 10), toInt(g("limDayMax")?.value, 10)];
  cfg.limits.max_dm_month = toInt(g("limMonth")?.value, 10000000);
  cfg.limits.max_monitor_passes = Math.max(0, toInt(g("limMaxPasses")?.value, 0));
  cfg.limits.typing_delay_sec = [toInt(g("limTypingMin")?.value, 2), toInt(g("limTypingMax")?.value, 5)];
  cfg.limits.per_chat_scan_delay_sec = [toFloat(g("limScanMin")?.value, 0.35), toFloat(g("limScanMax")?.value, 0.9)];

  cfg.limits.stage_delays_sec = cfg.limits.stage_delays_sec || {};
  cfg.limits.stage_delays_sec.stage1 = [toInt(g("limS1Min")?.value, 30), toInt(g("limS1Max")?.value, 60)];
  cfg.limits.stage_delays_sec.stage2 = [toInt(g("limS2Min")?.value, 20), toInt(g("limS2Max")?.value, 40)];
  cfg.limits.stage_delays_sec.stage3 = [toInt(g("limS3Min")?.value, 15), toInt(g("limS3Max")?.value, 30)];

  cfg.limits.schedule = {
    enabled: Boolean(g("schEnabled")?.checked),
    timezone: String(g("schTz")?.value || "Europe/Moscow").trim() || "Europe/Moscow",
    active_hours: [toFloat(g("schStart")?.value, 9), toFloat(g("schEnd")?.value, 21)],
  };

  const fhObj = {};
  const addFh = (key, minId, maxId) => {
    const a = toFloat(g(minId)?.value, 0);
    const b = toFloat(g(maxId)?.value, 0);
    if (a > 0 || b > 0) {
      const lo = a && b ? Math.min(a, b) : a || b;
      const hi = a && b ? Math.max(a, b) : a || b;
      fhObj[key] = [lo, hi];
    }
  };
  addFh("stage1", "limFh1Min", "limFh1Max");
  addFh("stage2", "limFh2Min", "limFh2Max");
  addFh("stage3", "limFh3Min", "limFh3Max");
  if (Object.keys(fhObj).length) cfg.limits.stage_followup_hours = fhObj;
  else delete cfg.limits.stage_followup_hours;

  if (g("llmEnabled")) {
    cfg.llm = cfg.llm || {};
    if (window.__leadgenOrgRole === "admin") {
      const llmKeyRaw = String(g("llmApiKey")?.value || "").trim();
      cfg.llm.enabled = Boolean(g("llmEnabled")?.checked);
      cfg.llm.base_url = String(g("llmBaseUrl")?.value || "").trim() || "https://api.openai.com/v1";
      cfg.llm.model = String(g("llmModel")?.value || "").trim() || "gpt-4o-mini";
      if (llmKeyRaw && llmKeyRaw !== "••••••••") {
        cfg.llm.api_key = llmKeyRaw;
      } else if (prevCfg.llm && prevCfg.llm.api_key) {
        cfg.llm.api_key = prevCfg.llm.api_key;
      } else {
        cfg.llm.api_key = "";
      }
    } else {
      cfg.llm = prevCfg.llm && typeof prevCfg.llm === "object" ? { ...prevCfg.llm } : {};
    }
  }

  collectLlmPromptTextareasInto(cfg);

  if (window.__leadgenOrgRole === "admin") {
    const g = (id) => document.getElementById(id);
    const kwSrc = [];
    document.querySelectorAll(".csq-kw-src:checked").forEach((el) => {
      if (el.value) kwSrc.push(el.value);
    });
    cfg.channel_search_quality = {
      enabled: Boolean(g("csqEnabled")?.checked),
      debug_metrics: Boolean(g("csqDebugMetrics")?.checked),
      force_require_discussion: Boolean(g("csqForceRequireDiscussion")?.checked),
      window_sec: toInt(g("csqWindowSec")?.value, 86400),
      sample_messages_max: toInt(g("csqSampleMax")?.value, 200),
      per_row_delay_sec: toFloat(g("csqPerRowDelay")?.value, 0.18),
      min_about_len: toInt(g("csqMinAboutLen")?.value, 12),
      discussion_min_unique_authors: toInt(g("csqMinAuthors")?.value, 0),
      discussion_max_top_author_pct: toFloat(g("csqMaxTopAuthorPct")?.value, 100),
      discussion_min_messages: toInt(g("csqMinMessages")?.value, 0),
      min_mean_message_len: toInt(g("csqMinMeanLen")?.value, 0),
      max_duplicate_text_pct: toFloat(g("csqMaxDupPct")?.value, 100),
      max_promo_ratio_pct: toFloat(g("csqMaxPromoPct")?.value, 100),
      max_forward_ratio_pct: toFloat(g("csqMaxForwardPct")?.value, 100),
      min_question_ratio_pct: toFloat(g("csqMinQuestionPct")?.value, 0),
      min_reply_ratio_pct: toFloat(g("csqMinReplyPct")?.value, 0),
      min_cyrillic_ratio_pct: toFloat(g("csqMinCyrillicPct")?.value, 0),
      min_keyword_hits: toInt(g("csqMinKeywordHits")?.value, 1),
      min_quality_score: toInt(g("csqMinQualityScore")?.value, 0),
      exclude_empty_about: Boolean(g("csqExcludeEmptyAbout")?.checked),
      exclude_no_contact_in_about: Boolean(g("csqExcludeNoContact")?.checked),
      exclude_single_author_discussion: Boolean(g("csqExcludeSingleAuthor")?.checked),
      exclude_forward_heavy: Boolean(g("csqExcludeForwardHeavy")?.checked),
      niche_keywords_only: Boolean(g("csqNicheOnly")?.checked),
      russian_only: Boolean(g("csqRussianOnly")?.checked),
      niche_keyword_sources: kwSrc.length ? kwSrc : ["hot_lead", "required_intent_hot_lead"],
      extra_keywords: String(g("csqExtraKeywords")?.value || ""),
    };
  } else if (prevCfg.channel_search_quality && typeof prevCfg.channel_search_quality === "object") {
    cfg.channel_search_quality = prevCfg.channel_search_quality;
  }

  return cfg;
}

async function refreshConfigPlanPanel() {
  const kv = document.getElementById("configEffectiveKv");
  if (!kv) return;
  try {
    const st = await api("/api/stats/summary");
    const cfg = await api("/api/config");
    const plan = st.plan;
    const lim = st.limits || {};
    const tc = Array.isArray(cfg.target_chats) ? cfg.target_chats.length : 0;
    const dr = Array.isArray(lim.daily_limit_range) ? lim.daily_limit_range.join("–") : "—";
    const rows = [];
    if (plan) {
      rows.push({ k: "Тариф (план)", v: plan.id });
      rows.push({ k: "Потолок чатов", v: plan.max_chats });
      rows.push({ k: "Чатов в target_chats сейчас", v: tc });
      rows.push({ k: "ЛС/день (лимит тарифа, шт.)", v: plan.max_dm_day });
      rows.push({ k: "ЛС/день в конфиге (срезано)", v: dr });
      rows.push({ k: "ЛС/мес (потолок тарифа)", v: plan.max_dm_month });
      rows.push({ k: "ЛС/мес в конфиге", v: lim.max_dm_month != null ? lim.max_dm_month : "—" });
      rows.push({ k: "Мин. интервал монит. (тариф, сек)", v: plan.monitor_interval_min_sec });
      rows.push({ k: "Интервал в конфиге (сек)", v: lim.monitor_interval_sec != null ? lim.monitor_interval_sec : "—" });
      if (plan.max_telegram_accounts != null) {
        rows.push({ k: "Telegram-аккаунтов (лимит тарифа)", v: plan.max_telegram_accounts });
      }
    } else {
      rows.push({ k: "План", v: "не найден" });
    }
    setKv(kv, rows);
  } catch (e) {
    setKv(kv, [{ k: "Ошибка", v: e.message || String(e) }]);
  }
}

async function loadConfigPage() {
  if (!document.getElementById("saveConfigBtn")) return;
  window.__configPageHydrated = false;
  try {
    await ensureLlmPresetsUi();
    const cfg = await api("/api/config");
    window.__leadgenTgLastPrevCfg = JSON.parse(JSON.stringify(cfg));
    bindTgAccountUiOnce();
    fillConfigForm(cfg);
    applyChatsLlmPanelRole(window.__leadgenOrgRole);
    applyChannelSearchQualityPanelRole(window.__leadgenOrgRole);
    const editor = document.getElementById("configEditor");
    if (editor) {
      const forEditor = { ...cfg };
      delete forEditor.llm_key_configured;
      editor.value = JSON.stringify(forEditor, null, 2);
    }
    await refreshConfigPlanPanel();
    await loadPresetsPanel().catch(() => {});
    window.__configPageHydrated = true;
  } catch {
    window.__configPageHydrated = false;
  }
}

// ─── Пресеты настроек: панель, превью diff, CRUD ───────────────────────────
let __presetsCache = [];
let __presetMgrCurrentId = null;

function _presetsCanEditOrg() {
  return window.__leadgenOrgRole === "admin";
}
function _presetsIsPlatformAdmin() {
  return Boolean(window.__leadgenUserPlatformAdmin);
}

function _applyAdminOnlyVisibility() {
  // Скрываем admin-only кнопки (создание/редактирование пресетов) от не-админов.
  const canEdit = _presetsCanEditOrg() || _presetsIsPlatformAdmin();
  document.querySelectorAll('.admin-only[data-admin-only="org"]').forEach((el) => {
    el.classList.toggle("hidden", !canEdit);
  });
}

async function loadPresetsPanel() {
  const sel = document.getElementById("presetSelect");
  if (!sel) return;
  _applyAdminOnlyVisibility();
  bindPresetsUiOnce();
  try {
    const d = await api("/api/presets");
    __presetsCache = Array.isArray(d.items) ? d.items : [];
  } catch {
    __presetsCache = [];
  }
  const prev = sel.value;
  sel.innerHTML = "";
  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = __presetsCache.length ? "— выбрать пресет —" : "— пресетов пока нет —";
  sel.appendChild(opt0);
  const group = (kind, label) => {
    const items = __presetsCache.filter((p) => p.kind === kind);
    if (!items.length) return;
    const og = document.createElement("optgroup");
    og.label = label;
    items.forEach((p) => {
      const opt = document.createElement("option");
      opt.value = String(p.id);
      opt.textContent = p.name;
      og.appendChild(opt);
    });
    sel.appendChild(og);
  };
  group("system", "Системные (от платформы)");
  group("org", "Моя организация");
  if (prev && __presetsCache.some((p) => String(p.id) === String(prev))) sel.value = prev;
  const info = document.getElementById("presetInfoMsg");
  if (info) {
    if (!__presetsCache.length) {
      info.textContent = _presetsCanEditOrg()
        ? "Пресетов нет. Нажмите «Сохранить текущие как пресет», чтобы создать первый."
        : "Пресетов пока нет — попросите администратора создать.";
    } else {
      info.textContent = "";
    }
  }
}

function _presetFormatValue(v) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "boolean") return v ? "true" : "false";
  if (typeof v === "string") {
    if (v.length > 120) return v.slice(0, 120) + "…";
    return v;
  }
  try {
    const s = JSON.stringify(v);
    return s.length > 200 ? s.slice(0, 200) + "…" : s;
  } catch {
    return String(v);
  }
}

async function presetShowPreview() {
  const sel = document.getElementById("presetSelect");
  const dlg = document.getElementById("presetPreviewDialog");
  if (!sel || !dlg) return;
  const id = Number(sel.value || 0);
  if (!id) {
    alert("Выберите пресет в дропдауне.");
    return;
  }
  const titleEl = document.getElementById("presetPreviewTitle");
  const subEl = document.getElementById("presetPreviewSubtitle");
  const tbody = document.getElementById("presetDiffBody");
  const errEl = document.getElementById("presetPreviewError");
  if (errEl) errEl.classList.add("hidden");
  if (tbody) tbody.innerHTML = "";
  if (subEl) subEl.textContent = "Загрузка превью…";
  try {
    const d = await api(`/api/presets/${id}/diff`);
    const preset = d.preset || {};
    if (titleEl) titleEl.textContent = `Превью: «${preset.name || "?"}»`;
    if (subEl) {
      const kindLabel = preset.kind === "system" ? "системный" : "org";
      subEl.textContent = `${kindLabel}. Изменений: ${d.diffs_count || 0}.`;
    }
    const diffs = Array.isArray(d.diffs) ? d.diffs : [];
    if (!diffs.length) {
      const tr = document.createElement("tr");
      tr.className = "preset-diff-empty-row";
      const td = document.createElement("td");
      td.colSpan = 3;
      td.textContent = "Изменений нет — текущие настройки уже соответствуют пресету.";
      tr.appendChild(td);
      tbody.appendChild(tr);
    } else {
      for (const it of diffs) {
        const tr = document.createElement("tr");
        const tdP = document.createElement("td");
        const code = document.createElement("code");
        code.className = "preset-diff-path";
        code.textContent = String(it.path || "");
        tdP.appendChild(code);
        const tdB = document.createElement("td");
        tdB.className = "preset-diff-before";
        tdB.textContent = _presetFormatValue(it.before);
        const tdA = document.createElement("td");
        tdA.className = "preset-diff-after";
        tdA.textContent = _presetFormatValue(it.after);
        tr.appendChild(tdP);
        tr.appendChild(tdB);
        tr.appendChild(tdA);
        tbody.appendChild(tr);
      }
    }
    dlg.__presetId = id;
    if (typeof dlg.showModal === "function") dlg.showModal();
    else dlg.setAttribute("open", "open");
  } catch (e) {
    if (errEl) {
      errEl.textContent = e.message || String(e);
      errEl.classList.remove("hidden");
    }
    if (typeof dlg.showModal === "function") dlg.showModal();
  }
}

async function presetApplyConfirmed() {
  const dlg = document.getElementById("presetPreviewDialog");
  if (!dlg) return;
  const id = Number(dlg.__presetId || 0);
  if (!id) return;
  const errEl = document.getElementById("presetPreviewError");
  if (errEl) errEl.classList.add("hidden");
  try {
    const d = await api(`/api/presets/${id}/apply`, {
      method: "POST",
      body: JSON.stringify({ confirm: true }),
    });
    if (typeof dlg.close === "function") dlg.close();
    else dlg.removeAttribute("open");
    const info = document.getElementById("presetInfoMsg");
    if (info) info.textContent = d.message || "Применено";
    // Перезагружаем страницу настроек, чтобы UI отразил новые значения.
    await loadConfigPage().catch(() => {});
  } catch (e) {
    if (errEl) {
      errEl.textContent = e.message || String(e);
      errEl.classList.remove("hidden");
    }
  }
}

async function presetCreateFromCurrent() {
  if (!_presetsCanEditOrg() && !_presetsIsPlatformAdmin()) {
    alert("Только администратор организации может создавать пресеты.");
    return;
  }
  const name = window.prompt("Название нового пресета (2–80 символов):", "");
  if (!name) return;
  const description = window.prompt("Описание (опц.):", "") || "";
  try {
    const d = await api("/api/presets", {
      method: "POST",
      body: JSON.stringify({ name: name.trim(), description: description.trim(), kind: "org" }),
    });
    const info = document.getElementById("presetInfoMsg");
    if (info) info.textContent = d.message || "Пресет создан";
    await loadPresetsPanel();
    const sel = document.getElementById("presetSelect");
    if (sel && d.id) sel.value = String(d.id);
  } catch (e) {
    alert(e.message || e);
  }
}

async function presetManageOpen() {
  const dlg = document.getElementById("presetManageDialog");
  if (!dlg) return;
  // Скрываем опцию kind=system для не-платформенных админов
  const kindEl = document.getElementById("presetMgrKind");
  if (kindEl) {
    const sysOpt = Array.from(kindEl.options).find((o) => o.value === "system");
    if (sysOpt) sysOpt.disabled = !_presetsIsPlatformAdmin();
  }
  await presetManageRefresh();
  presetManageSelect(null);
  if (typeof dlg.showModal === "function") dlg.showModal();
  else dlg.setAttribute("open", "open");
}

async function presetManageRefresh() {
  const ul = document.getElementById("presetMgrList");
  if (!ul) return;
  await loadPresetsPanel();
  ul.innerHTML = "";
  for (const p of __presetsCache) {
    const li = document.createElement("li");
    li.dataset.id = String(p.id);
    const a = document.createElement("div");
    a.className = "preset-mgr-name";
    a.textContent = p.name;
    const m = document.createElement("div");
    m.className = "preset-mgr-meta";
    const kindCls = p.kind === "system" ? "preset-mgr-kind-system" : "preset-mgr-kind-org";
    const span = document.createElement("span");
    span.className = kindCls;
    span.textContent = p.kind;
    m.appendChild(span);
    m.appendChild(document.createTextNode(` • #${p.id} • ${(p.updated_at || p.created_at || "").slice(0, 16)}`));
    li.appendChild(a);
    li.appendChild(m);
    li.addEventListener("click", () => presetManageSelect(p.id));
    if (__presetMgrCurrentId === p.id) li.classList.add("active");
    ul.appendChild(li);
  }
}

async function presetManageSelect(id) {
  __presetMgrCurrentId = id || null;
  const ul = document.getElementById("presetMgrList");
  if (ul) {
    ul.querySelectorAll("li").forEach((li) => {
      li.classList.toggle("active", Number(li.dataset.id || 0) === Number(id || 0));
    });
  }
  const nameEl = document.getElementById("presetMgrName");
  const descEl = document.getElementById("presetMgrDesc");
  const kindEl = document.getElementById("presetMgrKind");
  const msgEl = document.getElementById("presetMgrMsg");
  if (msgEl) msgEl.textContent = "";
  if (!id) {
    if (nameEl) nameEl.value = "";
    if (descEl) descEl.value = "";
    if (kindEl) kindEl.value = "org";
    return;
  }
  try {
    const p = await api(`/api/presets/${id}`);
    if (nameEl) nameEl.value = p.name || "";
    if (descEl) descEl.value = p.description || "";
    if (kindEl) kindEl.value = p.kind || "org";
  } catch (e) {
    if (msgEl) msgEl.textContent = e.message || String(e);
  }
}

async function presetMgrSave() {
  const nameEl = document.getElementById("presetMgrName");
  const descEl = document.getElementById("presetMgrDesc");
  const kindEl = document.getElementById("presetMgrKind");
  const msgEl = document.getElementById("presetMgrMsg");
  const name = (nameEl && nameEl.value || "").trim();
  const description = (descEl && descEl.value || "").trim();
  const kind = (kindEl && kindEl.value || "org");
  if (!name || name.length < 2) {
    if (msgEl) msgEl.textContent = "Название от 2 символов.";
    return;
  }
  try {
    if (__presetMgrCurrentId) {
      await api(`/api/presets/${__presetMgrCurrentId}`, {
        method: "PUT",
        body: JSON.stringify({ name, description }),
      });
      if (msgEl) msgEl.textContent = "Сохранено.";
    } else {
      const d = await api("/api/presets", {
        method: "POST",
        body: JSON.stringify({ name, description, kind }),
      });
      __presetMgrCurrentId = d.id;
      if (msgEl) msgEl.textContent = d.message || "Создано.";
    }
    await presetManageRefresh();
  } catch (e) {
    if (msgEl) msgEl.textContent = e.message || String(e);
  }
}

async function presetMgrSnapshot() {
  const msgEl = document.getElementById("presetMgrMsg");
  if (!__presetMgrCurrentId) {
    if (msgEl) msgEl.textContent = "Сначала выберите/создайте пресет.";
    return;
  }
  if (!window.confirm("Перезаписать данные пресета снимком из текущих настроек?")) return;
  try {
    const cfg = await api("/api/config");
    await api(`/api/presets/${__presetMgrCurrentId}`, {
      method: "PUT",
      body: JSON.stringify({ data: cfg }),
    });
    if (msgEl) msgEl.textContent = "Данные пресета обновлены из текущих настроек.";
  } catch (e) {
    if (msgEl) msgEl.textContent = e.message || String(e);
  }
}

async function presetMgrDelete() {
  const msgEl = document.getElementById("presetMgrMsg");
  if (!__presetMgrCurrentId) return;
  if (!window.confirm("Удалить выбранный пресет? Это нельзя отменить.")) return;
  try {
    await api(`/api/presets/${__presetMgrCurrentId}`, { method: "DELETE" });
    __presetMgrCurrentId = null;
    if (msgEl) msgEl.textContent = "Удалено.";
    await presetManageRefresh();
    presetManageSelect(null);
  } catch (e) {
    if (msgEl) msgEl.textContent = e.message || String(e);
  }
}

function bindPresetsUiOnce() {
  const root = document.getElementById("configPresetsPanel");
  if (!root || root.dataset.bound === "1") return;
  root.dataset.bound = "1";
  const previewBtn = document.getElementById("presetPreviewBtn");
  if (previewBtn) previewBtn.addEventListener("click", () => presetShowPreview().catch((e) => alert(e.message || e)));
  const saveAsBtn = document.getElementById("presetSaveAsBtn");
  if (saveAsBtn) saveAsBtn.addEventListener("click", () => presetCreateFromCurrent().catch((e) => alert(e.message || e)));
  const mgrBtn = document.getElementById("presetManageBtn");
  if (mgrBtn) mgrBtn.addEventListener("click", () => presetManageOpen().catch((e) => alert(e.message || e)));
  const applyBtn = document.getElementById("presetApplyConfirmBtn");
  if (applyBtn) applyBtn.addEventListener("click", () => presetApplyConfirmed().catch((e) => alert(e.message || e)));
  const mgrSave = document.getElementById("presetMgrSaveBtn");
  if (mgrSave) mgrSave.addEventListener("click", () => presetMgrSave().catch(() => {}));
  const mgrSnap = document.getElementById("presetMgrSnapshotBtn");
  if (mgrSnap) mgrSnap.addEventListener("click", () => presetMgrSnapshot().catch(() => {}));
  const mgrDel = document.getElementById("presetMgrDeleteBtn");
  if (mgrDel) mgrDel.addEventListener("click", () => presetMgrDelete().catch(() => {}));
  const mgrNew = document.getElementById("presetMgrNewBtn");
  if (mgrNew) mgrNew.addEventListener("click", () => presetManageSelect(null));
  const mgrReload = document.getElementById("presetMgrReloadBtn");
  if (mgrReload) mgrReload.addEventListener("click", () => presetManageRefresh());
}

async function saveConfigPage(options = {}) {
  const quiet = Boolean(options.quiet);
  const skipLoadAfter = Boolean(options.skipLoadAfter);
  const advanced = document.getElementById("advancedBox");
  const editor = document.getElementById("configEditor");
  const prev = await api("/api/config");
  let payload = null;

  if (advanced && !advanced.classList.contains("hidden") && editor) {
    // If user edits raw JSON in Advanced mode, respect it.
    payload = JSON.parse(editor.value);
  } else {
    payload = buildConfigFromForm(prev);
  }
  const d = await api("/api/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!quiet) alert(d.message);
  if (!skipLoadAfter) await loadConfigPage();
}

function toggleAdvanced() {
  const box = document.getElementById("advancedBox");
  if (!box) return;
  box.classList.toggle("hidden");
}

async function refreshBotProgress() {
  const bar = document.getElementById("botProgressBar");
  const label = document.getElementById("botProgressLabel");
  const statusEl = document.getElementById("botStatus");
  if (!bar || !label) return;
  let d;
  try {
    d = await api("/api/bot/progress");
  } catch {
    return;
  }
  const total = Number(d.pass_total || 0);
  const idx = Number(d.pass_index || 0);
  const cur = d.current_chat != null ? String(d.current_chat) : "—";
  const phase = String(d.phase || "idle");
  const isScanning = phase === "scanning";
  const isSchedulePaused = phase === "schedule_paused";
  const isConnecting = phase === "connecting";
  const isAwaitingCode = phase === "awaiting_code";
  const isAwaitingPassword = phase === "awaiting_password";
  const isIdle = phase === "idle" || isSchedulePaused;
  const isStale = !!d.stale;
  let pct = 0;
  if (isScanning && total > 0) {
    pct = Math.min(100, Math.round((idx / total) * 100));
  } else if (isConnecting || isAwaitingCode || isAwaitingPassword) {
    pct = isAwaitingCode ? 35 : isAwaitingPassword ? 40 : 15;
  }
  bar.classList.toggle(
    "progress-bar--waiting",
    (isIdle || isConnecting || isAwaitingCode || isAwaitingPassword) &&
      d.running &&
      (total > 0 || isConnecting || isAwaitingCode || isAwaitingPassword)
  );
  bar.style.width = (isIdle && d.running && total > 0) ? "" : `${pct}%`;
  let run = d.running ? "цикл активен" : "бот остановлен";
  if (isStale) run = "не отвечает (heartbeat > 5 мин)";
  const ts = d.updated_at ? ` · обн. ${String(d.updated_at).slice(0, 19)}` : "";
  let phaseHint = "";
  if (isAwaitingCode) {
    phaseHint = " · ожидание кода Telegram (поле в сайдбаре)";
  } else if (isAwaitingPassword) {
    phaseHint = " · ожидание пароля 2FA (облачный пароль в том же поле сайдбара)";
  } else if (isConnecting) {
    phaseHint = " · подключение к Telegram";
  } else if (isSchedulePaused) {
    phaseHint = " · вне окна расписания, мониторинг на паузе";
  } else if (isIdle && d.running && total > 0) {
    phaseHint = " · пауза до следующего прохода";
  }
  let labelText;
  if (isAwaitingCode || isAwaitingPassword || isConnecting) {
    labelText = `${
      isAwaitingPassword ? "Ожидание пароля 2FA Telegram" : isAwaitingCode ? "Ожидание кода Telegram" : "Подключение"
    }${phaseHint}${ts} · ${run}`;
  } else if (total > 0) {
    labelText = isScanning
      ? `Сканирование: ${idx} / ${total} (${pct}%) · чат: ${cur}${ts} · ${run}`
      : `Чатов в мониторинге: ${total}${phaseHint}${ts} · ${run}`;
  } else {
    labelText = `Нет чатов в проходе (пусто или вне лимитов) · ${run}`;
  }
  const lastAct = d.last_action != null && String(d.last_action).trim() !== "" ? String(d.last_action).trim() : "";
  if (lastAct) {
    const short = lastAct.length > 220 ? `${lastAct.slice(0, 217)}…` : lastAct;
    labelText = `${labelText}\nПоследнее действие: ${short}`;
  }
  label.textContent = labelText;
  if (statusEl) {
    let statusText = `Статус: ${d.running ? "запущен" : "остановлен"}`;
    if (isStale) statusText = "Статус: не отвечает";
    else if (isAwaitingPassword) statusText = "Статус: ждёт пароль 2FA Telegram";
    else if (isAwaitingCode) statusText = "Статус: ждёт код Telegram";
    else if (isConnecting) statusText = "Статус: подключается";
    statusEl.textContent = statusText;
  }
}

async function refreshBotMiniStats() {
  const mini = document.getElementById("botMiniStats");
  if (!mini) return;
  try {
    const st = await api("/api/stats/summary");
    const lim = st.limits || {};
    const dayR = Array.isArray(lim.daily_limit_range) ? lim.daily_limit_range.join("–") : "—";
    mini.textContent =
      `Сегодня ЛС: ${st.daily_sent_count != null ? st.daily_sent_count : "—"} · лимит/день: ${dayR} · ` +
      `за месяц: ${st.monthly_sent_count != null ? st.monthly_sent_count : "—"} / ` +
      `${lim.max_dm_month != null ? lim.max_dm_month : "—"}`;
  } catch {
    mini.textContent = "";
  }
}

function formatScanAuditLine(e) {
  if (!e || typeof e !== "object") return "";
  const tsRaw = e.ts != null ? String(e.ts) : "";
  const ts = tsRaw.length >= 19 ? tsRaw.slice(11, 19) : tsRaw;
  if (e.kind === "activity" && e.action) {
    const bits = [ts, String(e.action)];
    if (e.chat != null) bits.push(`чат ${e.chat}`);
    if (e.user_id != null) bits.push(`user ${e.user_id}`);
    return bits.filter(Boolean).join(" | ");
  }
  const chat = e.chat != null ? String(e.chat) : "—";
  const seen = e.messages_seen != null ? e.messages_seen : "—";
  const hot = e.hot_leads != null ? e.hot_leads : "—";
  const dry = e.dry_run != null ? e.dry_run : "—";
  const q = e.queued != null ? e.queued : "—";
  return `${ts} | ${chat} | просм. ${seen} | горячих ${hot} | тест ${dry} | очередь ${q}`;
}

async function refreshBotScanLog() {
  const el = document.getElementById("botScanLog");
  const tailEl = document.getElementById("botLogTail");
  if (!el) return;
  let d;
  try {
    d = await api("/api/bot/scan-log");
  } catch (e) {
    el.textContent = e.message || String(e);
    if (tailEl) tailEl.textContent = "";
    return;
  }
  const lines = [];
  const log = Array.isArray(d.scan_audit_log) ? d.scan_audit_log : [];
  for (const row of log.slice(-80)) {
    const line = formatScanAuditLine(row);
    if (line) lines.push(line);
  }
  el.textContent = lines.length ? lines.join("\n") : "Пока нет записей (запустите бота).";
  const tail = typeof d.log_tail === "string" ? d.log_tail.trim() : "";
  maybeUiDebugAppendBotLogTail(tail);
  if (tailEl) {
    tailEl.textContent = tail || "(файл лога пуст или ещё не создан)";
  }
}

function renderBotLeadsTable(rows) {
  const table = document.getElementById("botLeadsTable");
  if (!table) return;
  table.innerHTML = "";
  const header = document.createElement("div");
  header.className = "tr wide";
  const cols = ["Метка", "Написано", "Чат", "Этап", "Статус", "Триггер", "Сообщение"];
  for (const c of cols) {
    const cell = document.createElement("div");
    cell.className = "title";
    cell.textContent = c;
    header.appendChild(cell);
  }
  table.appendChild(header);
  const list = Array.isArray(rows) ? rows : [];
  for (const r of list.slice().reverse()) {
    const row = document.createElement("div");
    const rid = String(r._id || "").trim();
    const tagKey = normalizeLeadTagKey(r.lead_tag);
    row.className = `tr wide lead-row-tag-${tagKey}`;

    const tagCell = document.createElement("div");
    tagCell.className = "bot-lead-tag-cell";
    if (rid) {
      const sel = document.createElement("select");
      sel.className = "bot-lead-tag-select";
      sel.title = "Метка (в CSV организации)";
      for (const v of LEAD_TAG_SELECT_ORDER) {
        const o = document.createElement("option");
        o.value = v;
        o.textContent = LEAD_TAG_LABELS[v] || v;
        if (v === tagKey) o.selected = true;
        sel.appendChild(o);
      }
      sel.addEventListener("change", () => {
        const tag = sel.value;
        (async () => {
          try {
            await api("/api/leads/tag", {
              method: "PATCH",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ id: rid, lead_tag: tag }),
            });
            r.lead_tag = tag === "lead" ? "" : tag;
            row.className = `tr wide lead-row-tag-${normalizeLeadTagKey(r.lead_tag)}`;
          } catch (e) {
            alert(e.message || String(e));
          }
        })();
      });
      tagCell.appendChild(sel);
    } else {
      tagCell.classList.add("muted");
      tagCell.textContent = "—";
    }
    row.appendChild(tagCell);

    const kw = String(r.matched_keyword || "").trim();
    const vals = [
      formatLeadWrittenTime(r.timestamp || ""),
      r.source_chat || "",
      shortStageLabel(r.stage || ""),
      shortStatusLabel(r.status || ""),
      kw,
      r.message || "",
    ];
    for (let vi = 0; vi < vals.length; vi++) {
      const cell = document.createElement("div");
      cell.className = "muted";
      const v = vals[vi];
      if (vi === 4 || vi === 5) {
        cell.innerHTML = highlightKeywordFirstHtml(v, kw);
      } else {
        cell.textContent = String(v);
      }
      row.appendChild(cell);
    }
    table.appendChild(row);
  }
}

async function refreshBotLeads() {
  if (!document.getElementById("botLeadsTable")) return;
  let d;
  try {
    d = await api("/api/bot/leads?limit=80");
  } catch {
    return;
  }
  renderBotLeadsTable(d.rows || []);
}

async function refreshBotInfo() {
  if (!document.getElementById("botKv")) return;
  const d = await api("/api/bot/info");
  const dayRange = Array.isArray(d.daily_limit_range) ? d.daily_limit_range.join("–") : "—";
  const fh = d.stage_followup_hours;
  const fhStr =
    fh && typeof fh === "object"
      ? ["stage1", "stage2", "stage3"]
          .map((k) => (Array.isArray(fh[k]) ? `${k}: ${fh[k].join("–")} ч` : null))
          .filter(Boolean)
          .join("; ") || "—"
      : "—";
  const testBadge = document.getElementById("botTestModeBadge");
  if (testBadge) {
    testBadge.classList.toggle("hidden", !d.dry_run);
  }
  setKv(document.getElementById("botKv"), [
    { k: "Организация", v: d.org_id },
    { k: "Роль", v: d.org_role },
    { k: "Подписка", v: `${d.plan_id} (${d.subscription_status})` },
    { k: "Тест без отправки", v: d.dry_run ? "да — в личку не пишем" : "нет — как обычно" },
    { k: "Расписание", v: d.schedule_summary ?? "—" },
    { k: "Follow-up (часы)", v: fhStr },
    { k: "Чатов в мониторинге", v: d.target_chats },
    { k: "Интервал мониторинга (сек)", v: d.monitor_interval_sec != null ? `${d.monitor_interval_sec}` : "—" },
    {
      k: "Макс. проходов мониторинга",
      v:
        d.max_monitor_passes != null && Number(d.max_monitor_passes) > 0
          ? `${d.max_monitor_passes} (потом только ЛС/входящие)`
          : "0 (без лимита)",
    },
    { k: "Лимит ЛС/час/чат", v: d.max_dm_per_hour_per_chat ?? "—" },
    { k: "Лимит ЛС/день (min–max)", v: dayRange },
    { k: "Лимит ЛС/месяц", v: d.max_dm_month ?? "—" },
    { k: "Можно запускать", v: d.can_start ? "да" : `нет: ${(d.reasons || []).join("; ")}` },
  ]);
}

async function startBot() {
  showBotActionMessage("", "ok");
  const d = await api("/api/bot/start", { method: "POST" });
  showBotActionMessage(d.message || "Готово", "ok");
  await refreshBotProgress();
  await refreshBotInfo();
}

async function stopBot() {
  showBotActionMessage("", "ok");
  const d = await api("/api/bot/stop", { method: "POST" });
  showBotActionMessage(d.message || "Остановлен", "ok");
  await refreshBotProgress();
  await refreshBotInfo();
}

function getBotTelegramCodeElements() {
  const inp =
    document.getElementById("globalBotTelegramCode") || document.getElementById("botTelegramCode");
  const msgEl =
    document.getElementById("globalBotTelegramCodeMsg") || document.getElementById("botTelegramCodeMsg");
  return { inp, msgEl };
}

async function submitBotTelegramCode() {
  const { inp, msgEl } = getBotTelegramCodeElements();
  if (!inp || !msgEl) return;
  const code = String(inp.value || "").trim();
  msgEl.textContent = "";
  msgEl.className = "hidden";
  if (!code) {
    msgEl.textContent = "Введите код из Telegram или пароль 2FA";
    msgEl.className = "form-error";
    return;
  }
  try {
    const d = await api("/api/bot/telegram-code", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code }),
    });
    inp.value = "";
    msgEl.textContent = d.message || "Код передан боту";
    msgEl.className = "bot-inline-msg bot-inline-msg--ok";
  } catch (e) {
    msgEl.textContent = e.message || String(e);
    msgEl.className = "form-error";
  }
}

/** @type {any[]} */
let searchChannelsMaster = [];

/** @type {{ items: any[], account_ids: string[] } | null} */
let dialogsCompareCache = null;

/** @type {number} */
let _dialogsCompareFilterT = 0;

function canUseChannelSearch() {
  return window.__leadgenCanChannelSearch !== false;
}

/** Сообщение вместо «молчаливой» кнопки, если флаг поиска выключен (редко, после смены прав). */
function ensureChannelSearchAllowed() {
  if (canUseChannelSearch()) return true;
  const msg =
    "Поиск каналов и генерация фраз сейчас недоступны: нужна роль в организации с доступом к воронке (admin / manager / client / tester). Обновите страницу после смены роли.";
  const errEl = document.getElementById("searchChannelsError");
  if (errEl) setFormError(errEl, msg);
  else alert(msg);
  return false;
}

/** Ключ localStorage: отмеченные TG-аккаунты (поиск / бот / сравнение диалогов). */
function _tgSelectedAccountsStorageKey() {
  const uid = currentUserId != null ? String(currentUserId) : "anon";
  return `leadgen_v1_selected_tg_accounts_${uid}`;
}

/** Выбранный аккаунт для операций с папками на странице поиска. */
function _foldersTgAccountStorageKey() {
  const uid = currentUserId != null ? String(currentUserId) : "anon";
  return `leadgen_v1_folders_tg_account_${uid}`;
}

function loadPersistedTgAccountCheckIds(validIds) {
  try {
    const raw = localStorage.getItem(_tgSelectedAccountsStorageKey());
    if (!raw) return null;
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) return null;
    const out = [];
    for (const x of arr) {
      const id = String(x || "").trim();
      if (id && validIds.has(id)) out.push(id);
    }
    return out.length ? out : null;
  } catch {
    return null;
  }
}

function savePersistedTgAccountCheckIds(ids) {
  try {
    localStorage.setItem(_tgSelectedAccountsStorageKey(), JSON.stringify(ids));
  } catch {
    /* ignore */
  }
}

/**
 * @param {HTMLElement} wrap
 * @param {any} cfg
 * @param {string[]} validIdList
 * @param {string} checkboxSelector например 'input.search-tg-acc'
 */
function applyPersistedTgAccountChecks(wrap, cfg, validIdList, checkboxSelector) {
  const validSet = new Set(validIdList);
  let pick = loadPersistedTgAccountCheckIds(validSet);
  const active = cfg.active_telegram_account != null ? String(cfg.active_telegram_account) : "";
  if (!pick || pick.length === 0) {
    pick = active && validSet.has(active) ? [active] : validIdList.slice(0, 1);
  }
  const pickSet = new Set(pick);
  wrap.querySelectorAll(checkboxSelector).forEach((inp) => {
    const id = String(inp.value || "").trim();
    inp.checked = pickSet.has(id);
  });
  if (!wrap.querySelector(`${checkboxSelector}:checked`)) {
    wrap.querySelectorAll(checkboxSelector).forEach((inp) => {
      if (active && String(inp.value) === active) inp.checked = true;
    });
  }
  if (!wrap.querySelector(`${checkboxSelector}:checked`)) {
    const first = wrap.querySelector(checkboxSelector);
    if (first) first.checked = true;
  }
}

function bindPersistTgAccountChecks(wrap, checkboxSelector) {
  if (wrap.dataset.leadgenPersistTgBound === "1") return;
  wrap.dataset.leadgenPersistTgBound = "1";
  wrap.addEventListener("change", (ev) => {
    const t = ev.target;
    if (!t || t.type !== "checkbox" || !(t.matches && t.matches(checkboxSelector))) return;
    const ids = [];
    wrap.querySelectorAll(`${checkboxSelector}:checked`).forEach((el) => {
      const v = String(el.value || "").trim();
      if (v) ids.push(v);
    });
    savePersistedTgAccountCheckIds(ids);
  });
}

/** Какие Telegram-аккаунты задействовать в поиске (страница «Поиск каналов»). */
function getSearchTelegramAccountPayload() {
  const host = document.getElementById("searchTgAccountChecks") || document.getElementById("botTgAccountChecks");
  if (!host) return {};
  const cbs = host.querySelectorAll("input.search-tg-acc:checked");
  const ids = [];
  cbs.forEach((el) => {
    const v = (el && el.value && String(el.value).trim()) || "";
    if (v) ids.push(v);
  });
  if (ids.length > 1) return { account_ids: ids };
  if (ids.length === 1) return { telegram_account_id: ids[0] };
  return {};
}

async function initBotTgAccountsUi() {
  const host = document.getElementById("botTgAccountsBlock");
  if (!host) return;
  let cfg;
  try {
    cfg = await api("/api/config");
  } catch {
    host.innerHTML = "";
    host.classList.add("hidden");
    return;
  }
  const accs = Array.isArray(cfg.telegram_accounts) ? cfg.telegram_accounts : [];
  if (accs.length <= 1) {
    host.innerHTML = "";
    host.classList.add("hidden");
    return;
  }
  host.classList.remove("hidden");
  host.innerHTML = "";
  const intro = document.createElement("span");
  intro.className = "muted";
  intro.textContent = "Синхронизация/операции Telegram — от аккаунта(ов):";
  host.appendChild(intro);
  const wrap = document.createElement("div");
  wrap.id = "botTgAccountChecks";
  wrap.className = "row";
  wrap.style.flexWrap = "wrap";
  wrap.style.gap = "10px";
  for (const a of accs) {
    if (!a || !a.id) continue;
    const lab = document.createElement("label");
    lab.className = "check-row muted";
    const inp = document.createElement("input");
    inp.type = "checkbox";
    inp.className = "search-tg-acc checkbox";
    inp.value = a.id;
    inp.title = a.label || a.id;
    lab.appendChild(inp);
    const sp = document.createElement("span");
    sp.textContent = String(a.label || a.id);
    lab.appendChild(sp);
    wrap.appendChild(lab);
  }
  const validIds = accs.map((a) => (a && a.id ? String(a.id) : "")).filter(Boolean);
  applyPersistedTgAccountChecks(wrap, cfg, validIds, "input.search-tg-acc");
  bindPersistTgAccountChecks(wrap, "input.search-tg-acc");
  host.appendChild(wrap);
}

/** Аккаунты для /api/chats/dialogs-compare и отписки: чекбоксы поиска или блок в «Чаты мониторинга». */
function getDialogsCompareAccountPayload() {
  const s = document.getElementById("searchTgAccountChecks");
  if (s) {
    const cbs = s.querySelectorAll("input.search-tg-acc:checked");
    const ids = [];
    cbs.forEach((el) => {
      const v = (el && el.value && String(el.value).trim()) || "";
      if (v) ids.push(v);
    });
    if (ids.length > 1) return { account_ids: ids };
    if (ids.length === 1) return { telegram_account_id: ids[0] };
    return {};
  }
  const host = document.getElementById("dialogsCompareAccountChecks");
  if (host) {
    const cbs = host.querySelectorAll("input.dialogs-compare-tg-acc:checked");
    const ids = [];
    cbs.forEach((el) => {
      const v = (el && el.value && String(el.value).trim()) || "";
      if (v) ids.push(v);
    });
    if (ids.length > 1) return { account_ids: ids };
    if (ids.length === 1) return { telegram_account_id: ids[0] };
    return {};
  }
  return {};
}

/** Тело /api/search/channels: лимит, подписчики, давность поста. */
function getSearchRequestPayload(query, limit) {
  const minSub = document.getElementById("searchMinSub");
  const maxIdle = document.getElementById("searchMaxIdle");
  const inc = document.getElementById("searchIncludeStale");
  const enr = document.getElementById("searchEnrichOnly");
  const m = minSub != null && minSub.value !== "" ? Number(minSub.value) : 0;
  const d = maxIdle != null && maxIdle.value !== "" ? Number(maxIdle.value) : 0;
  const min_subscribers = Number.isFinite(m) && m > 0 ? Math.floor(m) : 0;
  const max_inactive_days = Number.isFinite(d) && d > 0 ? Math.min(365, Math.floor(d)) : 0;
  const include_stale = inc ? Boolean(inc.checked) : false;
  const enrichOnly = enr ? Boolean(enr.checked) : false;
  const via = document.getElementById("searchViaComments");
  const bioKwEl = document.getElementById("searchBioKeywords");
  const cMsgEl = document.getElementById("searchCommentsPerChannel");
  const cUsrEl = document.getElementById("searchCommentersPerChannel");
  const reqDisc = document.getElementById("searchRequireDiscussion");
  const payload = {
    query: String(query || "").trim(),
    limit: Number(limit) || 20,
    min_subscribers,
    max_inactive_days,
    include_stale,
    enrich: enrichOnly || max_inactive_days > 0,
    ...getSearchTelegramAccountPayload(),
  };
  if (reqDisc && reqDisc.checked) payload.require_discussion = true;
  if (via && via.checked) {
    payload.via_comments = true;
    const b = (bioKwEl && bioKwEl.value.trim()) || "";
    if (b) payload.bio_keywords = b;
    const cm = cMsgEl && cMsgEl.value !== "" ? Number(cMsgEl.value) : 35;
    const cu = cUsrEl && cUsrEl.value !== "" ? Number(cUsrEl.value) : 12;
    if (Number.isFinite(cm) && cm > 0) payload.comments_messages_per_channel = Math.min(100, Math.max(5, Math.floor(cm)));
    if (Number.isFinite(cu) && cu > 0) payload.commenters_max_per_channel = Math.min(40, Math.max(3, Math.floor(cu)));
  }
  return payload;
}

async function initSearchTgAccountsUi() {
  const host = document.getElementById("searchTgAccountsBlock");
  if (!host) return;
  let cfg;
  try {
    cfg = await api("/api/config");
  } catch {
    host.innerHTML = "";
    host.classList.add("hidden");
    return;
  }
  const accs = Array.isArray(cfg.telegram_accounts) ? cfg.telegram_accounts : [];
  if (accs.length <= 1) {
    host.innerHTML = "";
    host.classList.add("hidden");
    return;
  }
  host.classList.remove("hidden");
  host.innerHTML = "";
  const intro = document.createElement("span");
  intro.className = "muted";
  intro.textContent = "Поиск и синхронизация чатов — от аккаунта(ов):";
  host.appendChild(intro);
  const wrap = document.createElement("div");
  wrap.id = "searchTgAccountChecks";
  wrap.className = "row";
  wrap.style.flexWrap = "wrap";
  wrap.style.gap = "10px";
  for (const a of accs) {
    if (!a || !a.id) continue;
    const lab = document.createElement("label");
    lab.className = "check-row muted";
    const inp = document.createElement("input");
    inp.type = "checkbox";
    inp.className = "search-tg-acc checkbox";
    inp.value = a.id;
    inp.title = a.label || a.id;
    lab.appendChild(inp);
    const sp = document.createElement("span");
    sp.textContent = String(a.label || a.id);
    lab.appendChild(sp);
    wrap.appendChild(lab);
  }
  const validIds = accs.map((a) => (a && a.id ? String(a.id) : "")).filter(Boolean);
  applyPersistedTgAccountChecks(wrap, cfg, validIds, "input.search-tg-acc");
  bindPersistTgAccountChecks(wrap, "input.search-tg-acc");
  host.appendChild(wrap);
}

async function initDialogsCompareAccountsUi() {
  const wrap = document.getElementById("dialogsCompareAccountsWrap");
  const target = document.getElementById("dialogsCompareAccountChecks");
  if (!wrap || !target) return;
  if (document.getElementById("searchTgAccountChecks")) {
    wrap.classList.add("hidden");
    return;
  }
  let cfg;
  try {
    cfg = await api("/api/config");
  } catch {
    wrap.classList.add("hidden");
    return;
  }
  const accs = Array.isArray(cfg.telegram_accounts) ? cfg.telegram_accounts : [];
  if (accs.length <= 1) {
    target.innerHTML = "";
    wrap.classList.add("hidden");
    return;
  }
  wrap.classList.remove("hidden");
  target.innerHTML = "";
  const intro = document.createElement("span");
  intro.className = "muted";
  intro.textContent = "Сравнение и отписка — от аккаунта(ов):";
  target.appendChild(intro);
  const row = document.createElement("div");
  row.className = "row";
  row.style.flexWrap = "wrap";
  row.style.gap = "10px";
  for (const a of accs) {
    if (!a || !a.id) continue;
    const lab = document.createElement("label");
    lab.className = "check-row muted";
    const inp = document.createElement("input");
    inp.type = "checkbox";
    inp.className = "dialogs-compare-tg-acc checkbox";
    inp.value = a.id;
    inp.title = a.label || a.id;
    lab.appendChild(inp);
    const sp = document.createElement("span");
    sp.textContent = String(a.label || a.id);
    lab.appendChild(sp);
    row.appendChild(lab);
  }
  const validIds = accs.map((a) => (a && a.id ? String(a.id) : "")).filter(Boolean);
  applyPersistedTgAccountChecks(row, cfg, validIds, "input.dialogs-compare-tg-acc");
  bindPersistTgAccountChecks(row, "input.dialogs-compare-tg-acc");
  target.appendChild(row);
}

async function initFoldersTgAccountSelect() {
  const sel = document.getElementById("foldersTgAccount");
  if (!sel) return;
  let cfg;
  try {
    cfg = await api("/api/config");
  } catch {
    return;
  }
  const accs = Array.isArray(cfg.telegram_accounts) ? cfg.telegram_accounts : [];
  sel.innerHTML = "";
  const z = document.createElement("option");
  z.value = "";
  z.textContent = "Активный (как в настройках)";
  sel.appendChild(z);
  for (const a of accs) {
    if (!a || !a.id) continue;
    const o = document.createElement("option");
    o.value = a.id;
    o.textContent = a.label ? `${a.label} (${a.id})` : a.id;
    sel.appendChild(o);
  }
  const optVals = new Set([...sel.options].map((o) => o.value));
  let savedFolderAcc = "";
  try {
    savedFolderAcc = localStorage.getItem(_foldersTgAccountStorageKey()) || "";
  } catch {
    savedFolderAcc = "";
  }
  const activeStr = cfg.active_telegram_account != null ? String(cfg.active_telegram_account) : "";
  if (savedFolderAcc && optVals.has(savedFolderAcc)) sel.value = savedFolderAcc;
  else if (activeStr && optVals.has(activeStr)) sel.value = activeStr;

  if (!sel.dataset.leadgenFoldersTgPersist) {
    sel.dataset.leadgenFoldersTgPersist = "1";
    sel.addEventListener("change", () => {
      try {
        localStorage.setItem(_foldersTgAccountStorageKey(), sel.value || "");
      } catch {
        /* ignore */
      }
    });
  }
}

function appendSearchLog(line) {
  const el = document.getElementById("searchLog");
  if (!el) return;
  const ts = new Date().toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  el.textContent += (el.textContent ? "\n" : "") + `[${ts}] ${line}`;
  el.scrollTop = el.scrollHeight;
}

function getSearchTypeFilterValue() {
  const r = document.querySelector('input[name="searchTypeFilter"]:checked');
  return r ? r.value : "all";
}

function searchChannelRowKey(ch) {
  if (ch.id != null) return String(ch.id);
  if (ch.username) return `@${ch.username}`;
  return "";
}

function filterSearchChannelsForDisplay() {
  const mode = getSearchTypeFilterValue();
  const exList = window.__monitorChatsExcludeList || [];
  return searchChannelsMaster.filter((it) => {
    if (searchChannelRowMatchesExclude(it, exList)) return false;
    if (mode === "channels") return Boolean(it.is_broadcast) && !it.is_megagroup;
    if (mode === "groups") return Boolean(it.is_megagroup);
    return true;
  });
}

function shortDialogsCompareAid(aid) {
  if (aid === "active") return "активн.";
  const s = String(aid);
  return s.length > 12 ? `${s.slice(0, 10)}…` : s;
}

function fillDialogsCompareLeaveSelect(account_ids) {
  const sel = document.getElementById("dialogsCompareLeaveAccount");
  if (!sel) return;
  const ids = Array.isArray(account_ids) ? account_ids : [];
  sel.innerHTML = "";
  let hasActive = false;
  const concrete = [];
  for (const aid of ids) {
    if (aid === "active") hasActive = true;
    else concrete.push(aid);
  }
  if (hasActive || concrete.length === 0) {
    const z = document.createElement("option");
    z.value = "";
    z.textContent = "Активный (как в настройках)";
    sel.appendChild(z);
  }
  for (const aid of concrete) {
    const o = document.createElement("option");
    o.value = aid;
    o.textContent = aid;
    sel.appendChild(o);
  }
}

function renderDialogsCompareTable() {
  const wrap = document.getElementById("dialogsCompareTable");
  if (!wrap) return;
  if (!dialogsCompareCache || !Array.isArray(dialogsCompareCache.items) || dialogsCompareCache.items.length === 0) {
    wrap.innerHTML = "";
    wrap.classList.add("hidden");
    return;
  }
  const selected = new Set();
  document.querySelectorAll("input.dialogs-compare-row:checked").forEach((el) => {
    const r = el.dataset.ref;
    if (r) selected.add(r);
  });
  const q = (document.getElementById("dialogsCompareFilter")?.value || "").trim().toLowerCase();
  const account_ids = Array.isArray(dialogsCompareCache.account_ids) ? dialogsCompareCache.account_ids : [];
  const items = dialogsCompareCache.items.filter((it) => {
    if (!q) return true;
    const title = String(it.title || "").toLowerCase();
    const ref = String(it.ref || "").toLowerCase();
    const kind = String(it.kind || "").toLowerCase();
    return title.includes(q) || ref.includes(q) || kind.includes(q);
  });
  if (!items.length) {
    wrap.innerHTML = "";
    const empty = document.createElement("div");
    empty.className = "muted";
    empty.textContent = "Нет строк по фильтру.";
    wrap.appendChild(empty);
    wrap.classList.remove("hidden");
    return;
  }
  wrap.classList.remove("hidden");
  wrap.innerHTML = "";
  const header = document.createElement("div");
  header.className = "tr dialogs-compare-row";
  const hdrCells = ["", "Название", "Ссылка", "Тип", ...account_ids.map((aid) => shortDialogsCompareAid(aid))];
  hdrCells.forEach((t, i) => {
    const c = document.createElement("div");
    c.className = "title";
    c.textContent = t;
    if (i >= 4 && account_ids[i - 4]) c.title = String(account_ids[i - 4]);
    header.appendChild(c);
  });
  wrap.appendChild(header);
  for (const it of items) {
    const ref = String(it.ref || "");
    const row = document.createElement("div");
    row.className = "tr dialogs-compare-row";
    const chkCell = document.createElement("div");
    const chk = document.createElement("input");
    chk.type = "checkbox";
    chk.className = "dialogs-compare-row";
    chk.dataset.ref = ref;
    if (ref && selected.has(ref)) chk.checked = true;
    chkCell.appendChild(chk);
    const titleEl = document.createElement("div");
    titleEl.setAttribute("dir", "auto");
    titleEl.textContent = it.title != null && String(it.title) !== "" ? String(it.title) : "—";
    const linkCell = document.createElement("div");
    if (ref.startsWith("@")) {
      const a = document.createElement("a");
      a.href = `https://t.me/${ref.slice(1)}`;
      a.target = "_blank";
      a.rel = "noopener noreferrer";
      a.className = "link";
      a.textContent = ref;
      linkCell.appendChild(a);
    } else {
      linkCell.className = "muted";
      linkCell.textContent = ref || "—";
    }
    const kindEl = document.createElement("div");
    kindEl.className = "muted";
    kindEl.textContent = String(it.kind || "—");
    row.appendChild(chkCell);
    row.appendChild(titleEl);
    row.appendChild(linkCell);
    row.appendChild(kindEl);
    const am = it.accounts && typeof it.accounts === "object" ? it.accounts : {};
    for (const aid of account_ids) {
      const cell = document.createElement("div");
      cell.className = "muted";
      cell.textContent = am[aid] ? "✓" : "—";
      row.appendChild(cell);
    }
    wrap.appendChild(row);
  }
}

async function loadDialogsCompare() {
  const msg = document.getElementById("dialogsCompareMsg");
  const loadBtn = document.getElementById("dialogsCompareLoadBtn");
  const limEl = document.getElementById("dialogsCompareLimit");
  let limit = limEl && limEl.value !== "" ? Number(limEl.value) : 500;
  if (!Number.isFinite(limit)) limit = 500;
  limit = Math.min(2000, Math.max(10, Math.floor(limit)));
  setButtonBusy(loadBtn, true, { busyLabel: "Загрузка…" });
  if (msg) {
    msg.textContent = "Загрузка из Telegram…";
    msg.classList.remove("hidden");
  }
  try {
    const d = await api("/api/chats/dialogs-compare", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ limit, ...getDialogsCompareAccountPayload() }),
    });
    const items = Array.isArray(d.items) ? d.items : [];
    const account_ids = Array.isArray(d.account_ids) ? d.account_ids : [];
    dialogsCompareCache = { items, account_ids };
    fillDialogsCompareLeaveSelect(account_ids);
    const errArr = Array.isArray(d.errors) ? d.errors : [];
    if (msg) {
      const errPart = errArr.length ? ` Предупреждения: ${errArr.join("; ")}` : "";
      msg.textContent = `Строк: ${items.length}.${errPart}`;
    }
    renderDialogsCompareTable();
  } catch (e) {
    dialogsCompareCache = null;
    const tbl = document.getElementById("dialogsCompareTable");
    if (tbl) {
      tbl.innerHTML = "";
      tbl.classList.add("hidden");
    }
    if (msg) msg.textContent = e.message || String(e);
  } finally {
    setButtonBusy(loadBtn, false);
  }
}

async function leaveDialogsCompareSelected() {
  const msg = document.getElementById("dialogsCompareMsg");
  const refs = [];
  document.querySelectorAll("input.dialogs-compare-row:checked").forEach((el) => {
    const r = el.dataset.ref;
    if (r && String(r).trim()) refs.push(String(r).trim());
  });
  if (!refs.length) {
    if (msg) {
      msg.textContent = "Отметьте хотя бы один чат в таблице.";
      msg.classList.remove("hidden");
    }
    return;
  }
  if (!window.confirm(`Отписаться от ${refs.length} чат(ов) на выбранномTG-аккаунте?`)) return;
  const sel = document.getElementById("dialogsCompareLeaveAccount");
  const telegram_account_id = sel && sel.value ? sel.value : undefined;
  const removeMon = Boolean(document.getElementById("dialogsCompareRemoveMonitoring")?.checked);
  const body = { refs, remove_from_monitoring: removeMon };
  if (telegram_account_id) body.telegram_account_id = telegram_account_id;
  if (msg) {
    msg.textContent = "Отписка…";
    msg.classList.remove("hidden");
  }
  try {
    const d = await api("/api/chats/leave", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    let line = d.message || "Готово";
    if (d.ok_count != null) line = `${line} (успешно: ${d.ok_count})`;
    if (msg) msg.textContent = line;
    await loadFoldersTargetChats().catch(() => {});
    await loadDialogsCompare().catch(() => {});
  } catch (e) {
    if (msg) msg.textContent = e.message || String(e);
  }
}

function getSelectedSearchRefs() {
  const s = new Set();
  document.querySelectorAll("input.search-ch-row").forEach((el) => {
    if (el.checked && el.dataset.ref) s.add(el.dataset.ref);
  });
  return s;
}

function renderSearchChannelsTable() {
  const wrap = document.getElementById("searchChannelsTable");
  const actions = document.getElementById("searchChannelsActions");
  if (!wrap) return;
  const selected = getSelectedSearchRefs();
  const displayItems = filterSearchChannelsForDisplay();
  if (!displayItems.length) {
    wrap.innerHTML = "";
    wrap.classList.add("hidden");
    if (actions) actions.classList.add("hidden");
    return;
  }
  wrap.classList.remove("hidden");
  if (actions) actions.classList.remove("hidden");
  wrap.innerHTML = "";
  const header = document.createElement("div");
  header.className = "tr search-channels-row";
  for (const t of ["", "Название", "Ссылка", "Участники", "Тип", "Посл. пост", "Актив.", "Score", "Запрос / находка", "ЧС"]) {
    const c = document.createElement("div");
    c.className = "title";
    c.textContent = t;
    header.appendChild(c);
  }
  wrap.appendChild(header);
  for (const it of displayItems) {
    const row = document.createElement("div");
    row.className = "tr search-channels-row";
    const uname = it.username ? String(it.username) : "";
    const idStr = it.id != null ? String(it.id) : "";
    const ref = uname ? `@${uname}` : idStr;
    const chkCell = document.createElement("div");
    const chk = document.createElement("input");
    chk.type = "checkbox";
    chk.className = "search-ch-row";
    chk.dataset.ref = ref;
    if (ref && selected.has(ref)) chk.checked = true;
    chkCell.appendChild(chk);
    const title = document.createElement("div");
    title.setAttribute("dir", "auto");
    title.textContent = it.title != null && String(it.title) !== "" ? String(it.title) : "—";
    const linkCell = document.createElement("div");
    if (uname) {
      const a = document.createElement("a");
      a.href = `https://t.me/${uname}`;
      a.target = "_blank";
      a.rel = "noopener noreferrer";
      a.className = "link";
      a.textContent = `@${uname}`;
      linkCell.appendChild(a);
    } else {
      linkCell.className = "muted";
      linkCell.textContent = ref || "—";
    }
    const part = document.createElement("div");
    part.className = "muted";
    part.textContent = it.participants != null ? String(it.participants) : "—";
    const typ = document.createElement("div");
    typ.className = "muted";
    if (it.is_broadcast) typ.textContent = "канал";
    else if (it.is_megagroup) typ.textContent = "супергруппа";
    else typ.textContent = "—";
    const lastP = document.createElement("div");
    lastP.className = "muted search-col-last";
    if (it.last_post_iso) {
      try {
        const dt = new Date(String(it.last_post_iso));
        lastP.textContent = Number.isNaN(dt.getTime()) ? String(it.last_post_iso).slice(0, 10) : dt.toLocaleString("ru-RU", { dateStyle: "short", timeStyle: "short" });
      } catch {
        lastP.textContent = "—";
      }
    } else {
      lastP.textContent = "—";
    }
    const act = document.createElement("div");
    const ar = it.activity;
    if (ar === "stale") {
      act.className = "search-col-act search-col-act--stale";
      act.textContent = "слак";
    } else if (ar === "ok") {
      act.className = "search-col-act search-col-act--ok";
      const idd = it.inactive_days;
      act.textContent = idd != null ? `жив, −${idd}д` : "жив";
    } else {
      act.className = "muted search-col-act";
      act.textContent = "—";
    }
    const scoreCell = document.createElement("div");
    scoreCell.className = "muted search-col-score";
    if (it.quality_score != null && it.quality_score !== "") {
      scoreCell.textContent = String(it.quality_score);
      if (it.quality_metrics && typeof it.quality_metrics === "object") {
        try {
          scoreCell.title = JSON.stringify(it.quality_metrics);
        } catch {
          scoreCell.title = "";
        }
      }
    } else {
      scoreCell.textContent = "—";
    }
    const foundBy = document.createElement("div");
    foundBy.className = "muted search-col-found search-col-found--wrap";
    const accMark =
      it.found_by_account != null && String(it.found_by_account).trim() !== ""
        ? String(it.found_by_account)
        : "";
    const qMark = it.found_by != null ? String(it.found_by) : "";
    let line1 = accMark && qMark ? `${accMark} · ${qMark}` : accMark || qMark || "";
    if (it.found_via === "comment_bio") {
      const seed = it.seed_title || it.seed_channel || "—";
      const who = it.commenter || "—";
      line1 = line1 ? `${line1}\nчерез коммент.: «${seed}» · ${who}` : `через коммент.: «${seed}» · ${who}`;
    }
    foundBy.textContent = line1 || "—";
    const blCell = document.createElement("div");
    const blBtn = document.createElement("button");
    blBtn.type = "button";
    blBtn.className = "secondary search-ch-bl-btn";
    blBtn.textContent = "ЧС";
    blBtn.title = "Чёрный список поиска: не показывать этот чат в выдаче";
    blBtn.addEventListener("click", () => {
      mergeRefsIntoChannelSearchExclude([ref]).catch((e) =>
        setFormError(document.getElementById("searchChannelsError"), e.message || String(e)),
      );
    });
    blCell.appendChild(blBtn);
    row.appendChild(chkCell);
    row.appendChild(title);
    row.appendChild(linkCell);
    row.appendChild(part);
    row.appendChild(typ);
    row.appendChild(lastP);
    row.appendChild(act);
    row.appendChild(scoreCell);
    row.appendChild(foundBy);
    row.appendChild(blCell);
    wrap.appendChild(row);
  }
}

async function generateSearchKeywords() {
  if (!ensureChannelSearchAllowed()) return;
  const errEl = document.getElementById("searchChannelsError");
  const okEl = document.getElementById("searchChannelsOk");
  const kwBtn = document.getElementById("generateKeywordsBtn");
  const niche = (document.getElementById("searchNicheDesc") && document.getElementById("searchNicheDesc").value.trim()) || "";
  const audience = (document.getElementById("searchAudience") && document.getElementById("searchAudience").value.trim()) || "";
  const stop_words = (document.getElementById("searchStopWords") && document.getElementById("searchStopWords").value.trim()) || "";
  const count = Number((document.getElementById("searchKeywordsCount") && document.getElementById("searchKeywordsCount").value) || 15) || 15;
  const ta = document.getElementById("searchKeywordsText");
  setFormError(errEl, "");
  if (niche.length < 3) {
    setFormError(errEl, "Опишите нишу не короче 3 символов.");
    return;
  }
  let llmReady = true;
  try {
    const ovR = await fetch("/api/chats/overview", { credentials: "same-origin" });
    if (ovR.ok) {
      const ov = await ovR.json();
      llmReady = Boolean(ov.llm_ready);
    }
  } catch {
    /* сервер сам вернёт 400 при отсутствии LLM */
  }
  if (!llmReady) {
    const oid = window.__leadgenOrgId;
    setFormError(
      errEl,
      `LLM не настроен для текущей организации${oid != null ? ` (org #${oid})` : ""}. Администратор org: «Настройки и лимиты» → «Подключения». Менеджеру: убедитесь, что в Admin назначена org клиента, где уже сохранён ключ.`,
    );
    return;
  }
  setButtonBusy(kwBtn, true, { busyLabel: "Генерация…" });
  if (okEl) {
    okEl.textContent = "Запрос к LLM…";
    okEl.classList.remove("hidden");
  }
  appendSearchLog("Генерация ключевых слов через LLM…");
  try {
    const r = await fetch("/api/search/generate-keywords", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": await ensureCsrf(),
      },
      body: JSON.stringify({ niche, count, audience, stop_words }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (r.status === 422 && ta) {
        const partial = Array.isArray(d.keywords) ? d.keywords : [];
        if (partial.length) {
          const block = partial.join("\n");
          const cur = ta.value.trim();
          ta.value = cur ? `${cur}\n${block}` : block;
        }
      }
      setFormError(errEl, d.message || `Ошибка ${r.status}`);
      if (okEl) okEl.classList.add("hidden");
      return;
    }
    const kws = Array.isArray(d.keywords) ? d.keywords : [];
    const block = kws.join("\n");
    if (ta) {
      const cur = ta.value.trim();
      ta.value = cur ? `${cur}\n${block}` : block;
    }
    appendSearchLog(`Готово: добавлено ${kws.length} фраз.`);
    if (okEl) {
      okEl.textContent = kws.length ? `Добавлено фраз: ${kws.length}` : "Список пуст";
      okEl.classList.remove("hidden");
    }
  } finally {
    setButtonBusy(kwBtn, false);
  }
}

function _isSessionLockedError(d, status) {
  if (status === 409) return true;
  const msg = (d && (d.message || d.error)) ? String(d.message || d.error) : "";
  return msg.includes("Сессия Telegram занята") || msg.includes("telegram_session_locked");
}

async function tryAutoClearTgSessionLocks() {
  try {
    const r = await api("/api/search/clear-session-locks", { method: "POST" });
    return r && typeof r.removed === "number" ? r.removed : 0;
  } catch {
    return 0;
  }
}

// Глобальный контроллер пакетного поиска: позволяет остановить процесс
// в любой момент через кнопку «Остановить».
let __searchBatchCtl = null;

function _searchBatchCancelled() {
  return Boolean(__searchBatchCtl && __searchBatchCtl.signal.aborted);
}

function _searchBatchSetUiBusy(busy) {
  const batchBtn = document.getElementById("searchBatchBtn");
  const stopBtn = document.getElementById("searchStopBtn");
  if (batchBtn) batchBtn.disabled = !!busy;
  if (stopBtn) stopBtn.classList.toggle("hidden", !busy);
}

async function stopBatchSearchChannels(reason = "Пользователь нажал «Остановить».") {
  if (__searchBatchCtl && !__searchBatchCtl.signal.aborted) {
    try {
      __searchBatchCtl.abort(reason);
    } catch {
      try { __searchBatchCtl.abort(); } catch { /* ignore */ }
    }
    appendSearchLog(`⏹ ${reason}`);
  }
  // Серверный стоп: прибиваем активный Telethon-подпроцесс, иначе он будет
  // удерживать session-lock до завершения (15–60+ секунд) и блокировать
  // следующие запросы.
  try {
    const r = await api("/api/search/stop", { method: "POST" });
    if (r && (r.killed || r.locks_removed)) {
      appendSearchLog(`  серверная остановка: процессов ${r.killed}, блокировок снято ${r.locks_removed}.`);
    }
  } catch (e) {
    appendSearchLog(`  серверная остановка не удалась: ${e.message || e}`);
  }
}

// Небольшая пауза с поддержкой отмены по AbortSignal.
function _abortableSleep(ms, signal) {
  return new Promise((resolve) => {
    if (signal && signal.aborted) { resolve(); return; }
    const t = setTimeout(() => {
      if (signal) signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => { clearTimeout(t); resolve(); };
    if (signal) signal.addEventListener("abort", onAbort, { once: true });
  });
}

async function runBatchSearchChannels() {
  if (!ensureChannelSearchAllowed()) return;
  const errEl = document.getElementById("searchChannelsError");
  const okEl = document.getElementById("searchChannelsOk");
  const ta = document.getElementById("searchKeywordsText");
  const limEl = document.getElementById("searchChannelsLimit");
  setFormError(errEl, "");
  if (okEl) {
    okEl.textContent = "";
    okEl.classList.add("hidden");
  }
  if (!ta) return;
  const keywords = String(ta.value || "")
    .split("\n")
    .map((s) => s.trim())
    .filter((s) => s.length >= 2);
  if (!keywords.length) {
    setFormError(errEl, "Добавьте хотя бы одно ключевое слово в списке (от 2 символов).");
    return;
  }
  if (__searchBatchCtl && !__searchBatchCtl.signal.aborted) {
    setFormError(errEl, "Поиск уже идёт. Дождитесь окончания или нажмите «Остановить».");
    return;
  }
  const limit = limEl ? Number(limEl.value) || 20 : 20;
  const csrf = await ensureCsrf();
  const logEl = document.getElementById("searchLog");
  if (logEl) logEl.textContent = "";
  appendSearchLog(`Старт: ${keywords.length} запрос(ов), до ${limit} результатов на запрос.`);
  __searchBatchCtl = new AbortController();
  const signal = __searchBatchCtl.signal;
  _searchBatchSetUiBusy(true);
  const agg = new Map();
  // Авто-снятие блокировки: до 3 попыток на каждый запрос.
  const MAX_LOCK_RETRIES_PER_KW = 3;
  // Глобальный лимит, чтобы не зациклиться, если очень болит.
  const MAX_LOCK_RETRIES_TOTAL = Math.max(6, keywords.length * 2);
  let lockRetriesTotal = 0;
  let stoppedByUser = false;
  try {
    for (let i = 0; i < keywords.length; i++) {
      if (signal.aborted) { stoppedByUser = true; break; }
      const kw = keywords[i];
      appendSearchLog(`Запрос ${i + 1}/${keywords.length}: «${kw}»…`);
      const doFetch = async () =>
        fetch("/api/search/channels", {
          method: "POST",
          credentials: "same-origin",
          signal,
          headers: {
            "Content-Type": "application/json",
            "X-CSRF-Token": csrf,
          },
          body: JSON.stringify(getSearchRequestPayload(kw, limit)),
        });
      let r;
      try {
        r = await doFetch();
      } catch (e) {
        if (signal.aborted) { stoppedByUser = true; break; }
        appendSearchLog(`  сеть: ${e.message || e}`);
        continue;
      }
      let d = await r.json().catch(() => ({}));
      // Авто-восстановление при «висячем» lock-файле сессии Telegram:
      // снимаем остатки и повторяем тот же запрос. Делаем несколько попыток —
      // первый clear иногда не успевает «догнать» захваченный нашим же
      // воркером файл, ждём 1–3 с и пробуем снова.
      let kwLockRetries = 0;
      while (
        _isSessionLockedError(d, r.status)
        && kwLockRetries < MAX_LOCK_RETRIES_PER_KW
        && lockRetriesTotal < MAX_LOCK_RETRIES_TOTAL
        && !signal.aborted
      ) {
        kwLockRetries += 1;
        lockRetriesTotal += 1;
        appendSearchLog(
          `  занятая сессия Telegram — попытка автоснятия №${kwLockRetries}/${MAX_LOCK_RETRIES_PER_KW}…`,
        );
        const removed = await tryAutoClearTgSessionLocks();
        appendSearchLog(`  снято блокировок: ${removed}.`);
        // Растущая пауза: 800мс → 1.6с → 2.4с — даём предыдущему запросу
        // окончательно закрыть session-файл.
        await _abortableSleep(800 * kwLockRetries, signal);
        if (signal.aborted) break;
        try {
          r = await doFetch();
          d = await r.json().catch(() => ({}));
        } catch (e) {
          if (signal.aborted) { stoppedByUser = true; break; }
          appendSearchLog(`  сеть после автоснятия: ${e.message || e}`);
          d = {};
          r = { status: 0, ok: false };
          break;
        }
      }
      if (signal.aborted) { stoppedByUser = true; break; }
      if (r.status === 499 || (d && d.cancelled)) {
        appendSearchLog("  поиск прерван сервером (Стоп).");
        stoppedByUser = true;
        break;
      }
      if (r.status === 409 && !_isSessionLockedError(d, r.status)) {
        appendSearchLog(`  стоп: ${d.message || "остановите бота"}`);
        setFormError(errEl, d.message || "");
        break;
      }
      if (!r.ok) {
        if (_isSessionLockedError(d, r.status)) {
          appendSearchLog(
            "  сессия Telegram всё ещё занята после автоснятия — пропускаю этот запрос. "
            + "Если повторяется, остановите бота на странице «Бот» и попробуйте снова.",
          );
        } else {
          appendSearchLog(`  ошибка: ${d.message || r.status}`);
        }
        if (d && d.stdout_tail) {
          const t = String(d.stdout_tail || "").trim();
          if (t) appendSearchLog(`  stdout: ${t.slice(-250)}`);
        }
        continue;
      }
      const rows = Array.isArray(d.items) ? d.items : [];
      let newU = 0;
      for (const ch of rows) {
        const key = searchChannelRowKey(ch) || JSON.stringify(ch);
        if (!agg.has(key)) {
          agg.set(key, { ...ch, found_by: kw });
          newU++;
        } else {
          const ex = agg.get(key);
          const fb = ex.found_by != null ? String(ex.found_by) : "";
          if (fb && !fb.includes(kw)) ex.found_by = `${fb}, ${kw}`;
          else if (!fb) ex.found_by = kw;
        }
      }
      appendSearchLog(`  строк: ${rows.length}, новых уникальных +${newU}, всего уникальных ${agg.size}`);
    }
  } finally {
    __searchBatchCtl = null;
    _searchBatchSetUiBusy(false);
  }
  if (stoppedByUser) {
    appendSearchLog("Остановлено пользователем.");
  }
  const list = Array.from(agg.values()).sort(
    (a, b) => (Number(b.participants) || 0) - (Number(a.participants) || 0),
  );
  searchChannelsMaster = list;
  appendSearchLog(`Готово. Уникальных каналов в сводке: ${list.length}.`);
  renderSearchChannelsTable();
  if (okEl) {
    okEl.textContent = list.length
      ? `Сводка: ${list.length} каналов (фильтр типа: ${getSearchTypeFilterValue()}).`
      : "Ничего не найдено";
    okEl.classList.remove("hidden");
  }
}

async function runSearchChannels() {
  if (!ensureChannelSearchAllowed()) return;
  const qEl = document.getElementById("searchChannelsQuery");
  const limEl = document.getElementById("searchChannelsLimit");
  const errEl = document.getElementById("searchChannelsError");
  const okEl = document.getElementById("searchChannelsOk");
  const searchBtn = document.getElementById("searchChannelsBtn");
  if (!qEl) return;
  const query = String(qEl.value || "").trim();
  setFormError(errEl, "");
  if (okEl) {
    okEl.textContent = "";
    okEl.classList.add("hidden");
  }
  if (query.length < 2) {
    setFormError(errEl, "Введите не меньше 2 символов");
    searchChannelsMaster = [];
    renderSearchChannelsTable();
    return;
  }
  const limit = limEl ? Number(limEl.value) || 20 : 20;
  setButtonBusy(searchBtn, true, { busyLabel: "Поиск…" });
  if (okEl) {
    okEl.textContent = "Запрос к Telegram… (несколько аккаунтов обрабатываются по очереди)";
    okEl.classList.remove("hidden");
  }
  appendSearchLog(`Одиночный поиск «${query}»…`);
  try {
    const r = await fetch("/api/search/channels", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": await ensureCsrf(),
      },
      body: JSON.stringify(getSearchRequestPayload(query, limit)),
    });
    const d = await r.json().catch(() => ({}));
    if (r.status === 409) {
      setFormError(errEl, d.message || "Сначала остановите бота на странице «Управление»");
      searchChannelsMaster = [];
      renderSearchChannelsTable();
      if (okEl) okEl.classList.add("hidden");
      return;
    }
    if (!r.ok) {
      setFormError(errEl, d.message || `Ошибка ${r.status}`);
      if (d && d.stdout_tail) {
        const t = String(d.stdout_tail || "").trim();
        if (t) appendSearchLog(`  stdout: ${t.slice(-250)}`);
      }
      searchChannelsMaster = [];
      renderSearchChannelsTable();
      if (okEl) okEl.classList.add("hidden");
      return;
    }
    const items = Array.isArray(d.items) ? d.items : [];
    searchChannelsMaster = items.map((ch) => ({ ...ch, found_by: query }));
    appendSearchLog(`Одиночный поиск «${query}»: ${items.length} строк от API.`);
    renderSearchChannelsTable();
    if (okEl) {
      okEl.textContent = items.length ? `Найдено: ${items.length}` : "Ничего не найдено";
      okEl.classList.remove("hidden");
    }
  } catch (e) {
    setFormError(errEl, e.message || "Сеть недоступна");
    searchChannelsMaster = [];
    renderSearchChannelsTable();
    if (okEl) okEl.classList.add("hidden");
  } finally {
    setButtonBusy(searchBtn, false);
  }
}

function setAllSearchCheckboxes(checked) {
  document.querySelectorAll("input.search-ch-row").forEach((el) => {
    el.checked = checked;
  });
}

/** Ссылки @username / id по текущей выдаче (с учётом фильтра «каналы / супергруппы / все»). */
async function copySearchChannelsResultList() {
  if (!ensureChannelSearchAllowed()) return;
  const errEl = document.getElementById("searchChannelsError");
  const okEl = document.getElementById("searchChannelsOk");
  setFormError(errEl, "");
  const rows = filterSearchChannelsForDisplay();
  const refs = rows
    .map((it) => {
      if (it.username) return `@${it.username}`;
      if (it.id != null) return String(it.id);
      return "";
    })
    .filter(Boolean);
  if (!refs.length) {
    setFormError(errEl, "Нет строк в текущей выдаче (с учётом фильтра типа).");
    return;
  }
  const text = refs.join("\n");
  try {
    await copyTextToClipboard(text);
    if (okEl) {
      okEl.textContent = `Скопировано в буфер: ${refs.length} строк (текущая выдача).`;
      okEl.classList.remove("hidden");
    }
  } catch (e) {
    setFormError(errEl, e.message || "Не удалось скопировать. Разрешите доступ к буферу обмена.");
  }
}

async function addSelectedSearchChannels() {
  const errEl = document.getElementById("searchChannelsError");
  const okEl = document.getElementById("searchChannelsOk");
  setFormError(errEl, "");
  const chats = [];
  document.querySelectorAll("input.search-ch-row:checked").forEach((el) => {
    const ref = el.dataset.ref;
    if (ref) chats.push(ref);
  });
  if (!chats.length) {
    setFormError(errEl, "Отметьте хотя бы один канал");
    return;
  }
  let r;
  const autoJoin = document.getElementById("searchEnrollAutoJoin");
  const disc = document.getElementById("searchEnrollDiscussion");
  const aboutSel = document.getElementById("searchAboutLinks");
  const enrollBody = {
    chats,
    auto_join: autoJoin ? Boolean(autoJoin.checked) : true,
    include_discussion: disc ? Boolean(disc.checked) : true,
    about_links: aboutSel && aboutSel.value ? String(aboutSel.value) : "list",
    ...getSearchTelegramAccountPayload(),
  };
  try {
    r = await fetch("/api/search/add-to-monitoring", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": await ensureCsrf(),
      },
      body: JSON.stringify(enrollBody),
    });
  } catch (e) {
    setFormError(errEl, e.message || "Сеть недоступна");
    return;
  }
  const d = await r.json().catch(() => ({}));
  if (!r.ok) {
    setFormError(errEl, d.message || `Ошибка ${r.status}`);
    return;
  }
  if (okEl) {
    const trimmed = d.trimmed_due_to_plan;
    let line = d.message || "Сохранено";
    if (trimmed != null && Number(trimmed) > 0 && !String(line).includes("лимиту тарифа")) {
      line = `${line} (отсечено по тарифу: ${trimmed})`;
    }
    okEl.textContent = line;
    okEl.classList.remove("hidden");
  }
  await loadFoldersTargetChats().catch(() => {});
  await refreshSidebarAccountExtras().catch(() => {});
}

async function refreshSearchPageHint() {
  const el = document.getElementById("searchLlmHint");
  if (!el || !canUseChannelSearch()) return;
  try {
    const d = await api("/api/chats/overview");
    const needAdmin = window.__leadgenOrgRole !== "admin";
    if (d.llm_ready) {
      el.textContent = "LLM настроен — можно сгенерировать список фраз по нише.";
    } else if (needAdmin) {
      el.textContent =
        "Чтобы сгенерировать фразы, попросите администратора настроить LLM: «Настройки и лимиты» → «Подключения».";
    } else {
      el.textContent = "Включите LLM и укажите API key в «Настройки и лимиты» → «Подключения».";
    }
  } catch {
    el.textContent = "";
  }
}

async function refreshLogs() {
  if (!document.getElementById("logViewer")) return;
  const d = await api("/api/logs");
  const el = document.getElementById("logViewer");
  el.textContent = d.log || "";
  el.scrollTop = el.scrollHeight;
}

async function listFolders() {
  if (!document.getElementById("foldersOutput")) return;
  const sel = document.getElementById("foldersTgAccount");
  const q = sel && sel.value ? `?telegram_account_id=${encodeURIComponent(sel.value)}` : "";
  const d = await api(`/api/folders/list${q}`);
  document.getElementById("foldersOutput").textContent = d.output || "(пусто)";
}

async function uploadMyAvatar() {
  const inp = document.getElementById("avatarFileInput");
  const msg = document.getElementById("avatarUploadMsg");
  if (!inp || !inp.files || !inp.files[0]) {
    if (msg) msg.textContent = "Выберите файл";
    return;
  }
  const fd = new FormData();
  fd.append("file", inp.files[0]);
  const r = await fetch("/api/me/avatar", {
    method: "POST",
    credentials: "same-origin",
    headers: { "X-CSRF-Token": await ensureCsrf() },
    body: fd,
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    if (msg) msg.textContent = data.message || "Ошибка загрузки";
    return;
  }
  if (msg) msg.textContent = data.message || "Готово";
  inp.value = "";
  await refreshAuthState();
  const t = Date.now();
  const sb = document.getElementById("sbAvatar");
  if (sb && data.avatar_url) {
    sb.src = `${data.avatar_url}?t=${t}`;
    sb.classList.remove("hidden");
  }
  const p = document.getElementById("profileAvatarPreview");
  if (p && data.avatar_url) p.src = `${data.avatar_url}?t=${t}`;
}

async function clearMyAvatar() {
  const msg = document.getElementById("avatarUploadMsg");
  const r = await fetch("/api/me/avatar", {
    method: "DELETE",
    credentials: "same-origin",
    headers: { "X-CSRF-Token": await ensureCsrf() },
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) {
    if (msg) msg.textContent = data.message || "Ошибка";
    return;
  }
  if (msg) msg.textContent = data.message || "Сброшено";
  await refreshAuthState();
  const t = Date.now();
  const d = await api("/api/auth/me");
  if (d.authenticated && d.avatar_url) {
    const sb = document.getElementById("sbAvatar");
    if (sb) {
      sb.src = `${d.avatar_url}?t=${t}`;
      sb.classList.remove("hidden");
    }
    const p = document.getElementById("profileAvatarPreview");
    if (p) p.src = `${d.avatar_url}?t=${t}`;
  }
}

async function loadFoldersTargetChats() {
  const ta = document.getElementById("foldersTargetChats");
  if (!ta) return;
  window.__foldersTargetChatsHydrated = false;
  try {
    const cfg = await api("/api/config");
    ta.value = listToLines(cfg.target_chats || []);
    window.__foldersTargetChatsHydrated = true;
  } catch {
    window.__foldersTargetChatsHydrated = false;
  }
}

async function saveFoldersTargetChats(options = {}) {
  const quiet = Boolean(options.quiet);
  const skipLoadAfter = Boolean(options.skipLoadAfter);
  const navLeaveGuard = Boolean(options.navLeaveGuard);
  const ta = document.getElementById("foldersTargetChats");
  if (!ta) return;
  if (navLeaveGuard && window.__foldersTargetChatsHydrated !== true) {
    return;
  }
  const prev = await api("/api/config");
  const payload = { ...prev, target_chats: parseChats(ta.value) };
  const d = await api("/api/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!quiet) alert(d.message || "Сохранено");
  if (!skipLoadAfter) await loadFoldersTargetChats();
  if (!skipLoadAfter && document.getElementById("monitorChatsExcludeWrap")) {
    await refreshMonitorChatsExcludePanel().catch(() => {});
  }
}

async function importFolder() {
  const folder = document.getElementById("folderName").value.trim();
  if (!folder) {
    alert("Введите название или id папки");
    return;
  }
  const sel = document.getElementById("foldersTgAccount");
  const payload = { folder };
  if (sel && sel.value) payload.telegram_account_id = sel.value;
  const d = await api("/api/folders/import", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const out = document.getElementById("foldersOutput");
  if (out) out.textContent = d.output || "(пусто)";
  await loadConfig();
  await loadFoldersTargetChats().catch(() => {});
  await refreshMonitorChatsExcludePanel().catch(() => {});
}

/** @type {string | null} */
let _pendingTotpToken = null;

async function registerUser() {
  setFormError(document.getElementById("regError"), "");
  const email = document.getElementById("regEmail").value.trim();
  const password = document.getElementById("regPassword").value;
  const consent = Boolean(document.getElementById("regConsent")?.checked);
  if (!consent) {
    setFormError(document.getElementById("regError"), "Нужно согласие с политикой и условиями.");
    return;
  }
  try {
    const t = await ensureCsrf();
    const r = await fetch("/api/auth/register", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", "X-CSRF-Token": t },
      body: JSON.stringify({ email, password, consent: true }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.message || "Ошибка регистрации");
    const authState = document.getElementById("authState");
    if (authState) authState.textContent = d.message || "Готово";
  } catch (e) {
    setFormError(document.getElementById("regError"), e.message || "Ошибка регистрации");
  }
}

async function loginUser() {
  setFormError(document.getElementById("loginError"), "");
  const totpB = document.getElementById("totpBlock");
  const email = document.getElementById("loginEmail").value.trim();
  const password = document.getElementById("loginPassword").value;
  try {
    const d = await authPost("/api/auth/login", { email, password });
    if (d.needs_totp && d.totp_token) {
      _pendingTotpToken = d.totp_token;
      if (totpB) totpB.classList.remove("hidden");
      const el = document.getElementById("loginError");
      if (el) {
        el.textContent = d.message || "Введите одноразовый код 2FA.";
        el.classList.remove("hidden");
      }
      return;
    }
    _pendingTotpToken = null;
    if (totpB) totpB.classList.add("hidden");
    // Серверный логин выставляет HttpOnly cookie auth_token и маркер auth_present.
    // Локально хранить токен (d.token) больше не нужно.
    await refreshAuthState();
    if (document.body && document.body.dataset && document.body.dataset.page === "auth") {
      window.location.href = "/";
    }
  } catch (e) {
    setFormError(document.getElementById("loginError"), e.message || "Ошибка входа");
  }
}

async function loginTotpSubmit() {
  setFormError(document.getElementById("loginError"), "");
  const code = (document.getElementById("loginTotp")?.value || "").replace(/\s/g, "");
  if (!_pendingTotpToken || !code) {
    setFormError(document.getElementById("loginError"), "Введите код из приложения-аутентификатора.");
    return;
  }
  try {
    await authPost("/api/auth/login/totp", { totp_token: _pendingTotpToken, code });
    _pendingTotpToken = null;
    document.getElementById("totpBlock")?.classList.add("hidden");
    await refreshAuthState();
    if (document.body && document.body.dataset && document.body.dataset.page === "auth") {
      window.location.href = "/";
    }
  } catch (e) {
    setFormError(document.getElementById("loginError"), e.message || "Ошибка 2FA");
  }
}

async function loadSecurityPage() {
  setFormError(document.getElementById("secPwdError"), "");
  setFormError(document.getElementById("sec2faError"), "");
  setFormError(document.getElementById("secSessError"), "");
  setFormError(document.getElementById("secAuditError"), "");
  const me = await api("/api/auth/me");
  if (!me.authenticated) return;
  const s2 = document.getElementById("sec2faState");
  if (s2) s2.textContent = me.totp_enabled ? "2FA: включена" : "2FA: не включена";
  const d = await api("/api/account/sessions");
  const ul = document.getElementById("secSessList");
  if (ul) {
    ul.innerHTML = "";
    (d.sessions || []).forEach((s) => {
      const li = document.createElement("li");
      li.textContent = `${s.is_current ? "Текущая" : "Сессия"} · …${s.token_suffix} · ${s.created_at || "—"}`;
      if (s.ip) li.textContent += ` · ${s.ip}`;
      ul.appendChild(li);
    });
  }
  const a = await api("/api/account/login-audit?limit=40");
  const aul = document.getElementById("secAuditList");
  if (aul) {
    aul.innerHTML = "";
    (a.items || []).forEach((it) => {
      const li = document.createElement("li");
      li.textContent = `${it.created_at || "—"} · ${it.event || "—"}${it.ip ? ` · ${it.ip}` : ""}`;
      aul.appendChild(li);
    });
  }
  const qrB = document.getElementById("sec2faQrBlock");
  const qr = document.getElementById("sec2faQr");
  if (me.totp_enabled && qrB && qr) {
    qrB.classList.add("hidden");
    qr.removeAttribute("src");
  }
}

async function logoutUser() {
  try {
    await api("/api/auth/logout", { method: "POST" });
  } catch {
    /* ignore */
  }
  leadgenCsrf = null;
  clearLeadgenVolatileSession();
  // HttpOnly cookie очищает сервер; маркер auth_present также удалён сервером.
  await refreshAuthState();
}

function bindActions() {
  bindAdminOrgDebugUiOnce();
  bindThemeAndSidebar();
  bindNavSaveOnLeave();
  bindConfigPageTabs();
  bindLeadgenAssistant();
  bindKwPreviewOnce();
  bindConversationsPageOnce();
  window.addEventListener("beforeunload", () => {
    saveVolatileForCurrentPageSync();
  });

  const saveConfigBtn = document.getElementById("saveConfigBtn");
  if (saveConfigBtn) saveConfigBtn.addEventListener("click", () => saveConfigPage().catch(alert));
  const toggleAdvancedBtn = document.getElementById("toggleAdvancedBtn");
  if (toggleAdvancedBtn) toggleAdvancedBtn.addEventListener("click", () => toggleAdvanced());

  const startBtn = document.getElementById("startBtn");
  if (startBtn) {
    startBtn.addEventListener("click", () =>
      (uiDebug("UI click: start bot"), startBot().catch((e) => showBotActionMessage(e.message || String(e), "error"))),
    );
  }

  const stopBtn = document.getElementById("stopBtn");
  if (stopBtn) {
    stopBtn.addEventListener("click", () =>
      (uiDebug("UI click: stop bot"), stopBot().catch((e) => showBotActionMessage(e.message || String(e), "error"))),
    );
  }

  const botTelegramCodeBtn = document.getElementById("botTelegramCodeBtn");
  if (botTelegramCodeBtn) {
    botTelegramCodeBtn.addEventListener("click", () => submitBotTelegramCode().catch(() => {}));
  }
  const globalBotTelegramCodeBtn = document.getElementById("globalBotTelegramCodeBtn");
  if (globalBotTelegramCodeBtn) {
    globalBotTelegramCodeBtn.addEventListener("click", () => submitBotTelegramCode().catch(() => {}));
  }
  const globalBotTelegramCodeInp = document.getElementById("globalBotTelegramCode");
  if (globalBotTelegramCodeInp) {
    globalBotTelegramCodeInp.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        submitBotTelegramCode().catch(() => {});
      }
    });
  }

  wireBotMonitorSettingsDialog();

  const syncDialogsBtn = document.getElementById("syncDialogsBtn");
  if (syncDialogsBtn) {
    syncDialogsBtn.addEventListener("click", async () => {
      uiDebug("UI click: sync dialogs from account(s)");
      const m = document.getElementById("syncDialogsMsg");
      if (m) {
        m.textContent = "Синхронизация с Telegram…";
        m.classList.remove("hidden");
      }
      setButtonBusy(syncDialogsBtn, true, { busyLabel: "Синхронизация…" });
      try {
        const d = await syncChatsFromAccount({ mode: "merge" });
        if (m) m.textContent = d.message || "Готово";
        await refreshBotInfo();
        await refreshStats();
        await refreshSidebarAccountExtras();
        await loadFoldersTargetChats().catch(() => {});
        if (document.getElementById("monitorChatsExcludeWrap")) {
          await refreshMonitorChatsExcludePanel().catch(() => {});
        }
      } catch (e) {
        if (m) m.textContent = e.message || String(e);
        uiDebug(`UI error: sync dialogs failed: ${e.message || e}`);
      } finally {
        setButtonBusy(syncDialogsBtn, false);
      }
    });
  }
  const searchSyncDialogsBtn = document.getElementById("searchSyncDialogsBtn");
  if (searchSyncDialogsBtn) {
    searchSyncDialogsBtn.addEventListener("click", async () => {
      const m = document.getElementById("searchSyncDialogsMsg");
      const rep = document.getElementById("searchSyncReplace");
      if (m) {
        m.textContent = "Синхронизация с Telegram…";
        m.classList.remove("hidden");
      }
      setButtonBusy(searchSyncDialogsBtn, true, { busyLabel: "Синхронизация…" });
      try {
        const d = await syncChatsFromAccount({
          mode: rep && rep.checked ? "replace" : "merge",
        });
        if (m) m.textContent = d.message || "Готово";
        await refreshStats();
        await refreshSidebarAccountExtras();
        /* Сервер уже записал target_chats; без подтяжки из GET /api/config при уходе со страницы
         * saveFoldersTargetChats отправит старое содержимое textarea и перезапишет конфиг. */
        await loadFoldersTargetChats().catch(() => {});
        if (document.getElementById("monitorChatsExcludeWrap")) {
          await refreshMonitorChatsExcludePanel().catch(() => {});
        }
      } catch (e) {
        if (m) m.textContent = e.message || String(e);
      } finally {
        setButtonBusy(searchSyncDialogsBtn, false);
      }
    });
  }

  const dialogsCompareLoadBtn = document.getElementById("dialogsCompareLoadBtn");
  if (dialogsCompareLoadBtn) {
    dialogsCompareLoadBtn.addEventListener("click", () => loadDialogsCompare().catch(alert));
  }
  const dialogsCompareFilter = document.getElementById("dialogsCompareFilter");
  if (dialogsCompareFilter) {
    dialogsCompareFilter.addEventListener("input", () => {
      window.clearTimeout(_dialogsCompareFilterT);
      _dialogsCompareFilterT = window.setTimeout(() => renderDialogsCompareTable(), 120);
    });
  }
  const dialogsCompareSelectAll = document.getElementById("dialogsCompareSelectAll");
  if (dialogsCompareSelectAll) {
    dialogsCompareSelectAll.addEventListener("click", () => {
      document.querySelectorAll("input.dialogs-compare-row").forEach((el) => {
        el.checked = true;
      });
    });
  }
  const dialogsCompareSelectNone = document.getElementById("dialogsCompareSelectNone");
  if (dialogsCompareSelectNone) {
    dialogsCompareSelectNone.addEventListener("click", () => {
      document.querySelectorAll("input.dialogs-compare-row").forEach((el) => {
        el.checked = false;
      });
    });
  }
  const dialogsCompareLeaveBtn = document.getElementById("dialogsCompareLeaveBtn");
  if (dialogsCompareLeaveBtn) {
    dialogsCompareLeaveBtn.addEventListener("click", () => leaveDialogsCompareSelected().catch(alert));
  }

  if (canUseChannelSearch()) {
    const searchBtn = document.getElementById("searchChannelsBtn");
    if (searchBtn) {
      searchBtn.addEventListener("click", () =>
        runSearchChannels().catch((e) =>
          setFormError(document.getElementById("searchChannelsError"), e.message || String(e)),
        ),
      );
    }
    const searchBatchBtn = document.getElementById("searchBatchBtn");
    if (searchBatchBtn) {
      searchBatchBtn.addEventListener("click", () =>
        runBatchSearchChannels().catch((e) =>
          setFormError(document.getElementById("searchChannelsError"), e.message || String(e)),
        ),
      );
    }
    const searchStopBtn = document.getElementById("searchStopBtn");
    if (searchStopBtn && !searchStopBtn.dataset.bound) {
      searchStopBtn.dataset.bound = "1";
      searchStopBtn.addEventListener("click", () => stopBatchSearchChannels().catch(() => {}));
    }
    const searchClearLocksBtn = document.getElementById("searchClearLocksBtn");
    if (searchClearLocksBtn && !searchClearLocksBtn.dataset.bound) {
      searchClearLocksBtn.dataset.bound = "1";
      searchClearLocksBtn.addEventListener("click", async () => {
        const errEl = document.getElementById("searchChannelsError");
        const okEl = document.getElementById("searchChannelsOk");
        if (errEl) {
          errEl.textContent = "";
          errEl.classList.add("hidden");
        }
        setButtonBusy(searchClearLocksBtn, true, { busyLabel: "Снимаю…" });
        try {
          const d = await api("/api/search/clear-session-locks", { method: "POST" });
          appendSearchLog(d.message || "Готово");
          if (okEl) {
            okEl.textContent = d.message || "Готово";
            okEl.classList.remove("hidden");
          }
        } catch (e) {
          setFormError(errEl, e.message || String(e));
        } finally {
          setButtonBusy(searchClearLocksBtn, false);
        }
      });
    }
    const generateKeywordsBtn = document.getElementById("generateKeywordsBtn");
    if (generateKeywordsBtn) {
      generateKeywordsBtn.addEventListener("click", () =>
        generateSearchKeywords().catch((e) =>
          setFormError(document.getElementById("searchChannelsError"), e.message || String(e)),
        ),
      );
    }
    const searchSelAll = document.getElementById("searchChannelsSelectAll");
    if (searchSelAll) searchSelAll.addEventListener("click", () => setAllSearchCheckboxes(true));
    const searchSelNone = document.getElementById("searchChannelsSelectNone");
    if (searchSelNone) searchSelNone.addEventListener("click", () => setAllSearchCheckboxes(false));
    const searchAdd = document.getElementById("searchChannelsAddBtn");
    if (searchAdd) {
      searchAdd.addEventListener("click", () =>
        addSelectedSearchChannels().catch((e) =>
          setFormError(document.getElementById("searchChannelsError"), e.message || String(e)),
        ),
      );
    }
    const searchCopyAll = document.getElementById("searchChannelsCopyAll");
    if (searchCopyAll) {
      searchCopyAll.addEventListener("click", () =>
        copySearchChannelsResultList().catch((e) =>
          setFormError(document.getElementById("searchChannelsError"), e.message || String(e)),
        ),
      );
    }
    const searchBlacklistBtn = document.getElementById("searchChannelsBlacklistBtn");
    if (searchBlacklistBtn) {
      searchBlacklistBtn.addEventListener("click", () => {
        const refs = [...getSelectedSearchRefs()];
        if (!refs.length) {
          setFormError(document.getElementById("searchChannelsError"), "Отметьте хотя бы один чат.");
          return;
        }
        mergeRefsIntoChannelSearchExclude(refs).catch((e) =>
          setFormError(document.getElementById("searchChannelsError"), e.message || String(e)),
        );
      });
    }
    document.querySelectorAll('input[name="searchTypeFilter"]').forEach((inp) => {
      inp.addEventListener("change", () => renderSearchChannelsTable());
    });
  }

  const listFoldersBtn = document.getElementById("listFoldersBtn");
  if (listFoldersBtn) listFoldersBtn.addEventListener("click", () => listFolders().catch(alert));

  const importFolderBtn = document.getElementById("importFolderBtn");
  if (importFolderBtn) importFolderBtn.addEventListener("click", () => importFolder().catch(alert));

  const foldersSaveChatsBtn = document.getElementById("foldersSaveChatsBtn");
  if (foldersSaveChatsBtn) foldersSaveChatsBtn.addEventListener("click", () => saveFoldersTargetChats().catch(alert));

  const monitorChatsFilter = document.getElementById("monitorChatsFilter");
  if (monitorChatsFilter) {
    let mt = 0;
    monitorChatsFilter.addEventListener("input", () => {
      window.clearTimeout(mt);
      mt = window.setTimeout(() => renderMonitorChatsExcludeTable(), 120);
    });
  }
  const monitorChatsExcludeRefresh = document.getElementById("monitorChatsExcludeRefresh");
  if (monitorChatsExcludeRefresh) {
    monitorChatsExcludeRefresh.addEventListener("click", () => refreshMonitorChatsExcludePanel().catch(alert));
  }
  const monitorChatsExcludeSave = document.getElementById("monitorChatsExcludeSave");
  if (monitorChatsExcludeSave) {
    monitorChatsExcludeSave.addEventListener("click", () => saveMonitorChatsExcludePanel().catch(alert));
  }

  const chatsSaveLlmBtn = document.getElementById("chatsSaveLlmBtn");
  if (chatsSaveLlmBtn) {
    chatsSaveLlmBtn.addEventListener("click", () => saveChatsLlmPage().catch(alert));
  }

  const registerBtn = document.getElementById("registerBtn");
  if (registerBtn) registerBtn.addEventListener("click", () => registerUser().catch(alert));

  const loginBtn = document.getElementById("loginBtn");
  if (loginBtn) loginBtn.addEventListener("click", () => loginUser().catch(alert));
  const loginTotpBtn = document.getElementById("loginTotpBtn");
  if (loginTotpBtn) loginTotpBtn.addEventListener("click", () => loginTotpSubmit().catch(alert));

  const secPwdBtn = document.getElementById("secPwdBtn");
  if (secPwdBtn) {
    secPwdBtn.addEventListener("click", () =>
      (async () => {
        setFormError(document.getElementById("secPwdError"), "");
        const cur = document.getElementById("secCurPwd")?.value || "";
        const neu = document.getElementById("secNewPwd")?.value || "";
        try {
          const d = await api("/api/account/password", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ current_password: cur, new_password: neu }),
          });
          alert(d.message || "Сохранено");
        } catch (e) {
          setFormError(document.getElementById("secPwdError"), e.message || String(e));
        }
      })().catch(alert),
    );
  }
  const sec2faSetupBtn = document.getElementById("sec2faSetupBtn");
  if (sec2faSetupBtn) {
    sec2faSetupBtn.addEventListener("click", () =>
      (async () => {
        setFormError(document.getElementById("sec2faError"), "");
        try {
          const d = await api("/api/auth/2fa/setup", { method: "POST" });
          const qrB = document.getElementById("sec2faQrBlock");
          const im = document.getElementById("sec2faQr");
          const sec = document.getElementById("sec2faSecret");
          if (sec) sec.textContent = d.secret || "—";
          if (im && d.qr_base64) {
            im.src = `data:image/png;base64,${d.qr_base64}`;
            im.classList.remove("hidden");
          }
          if (qrB) qrB.classList.remove("hidden");
        } catch (e) {
          setFormError(document.getElementById("sec2faError"), e.message || String(e));
        }
      })().catch(alert),
    );
  }
  const sec2faConfirmBtn = document.getElementById("sec2faConfirmBtn");
  if (sec2faConfirmBtn) {
    sec2faConfirmBtn.addEventListener("click", () =>
      (async () => {
        setFormError(document.getElementById("sec2faError"), "");
        const code = (document.getElementById("sec2faCode")?.value || "").replace(/\s/g, "");
        try {
          const d = await api("/api/auth/2fa/confirm", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ code }),
          });
          alert(d.message || "Ок");
          loadSecurityPage().catch(alert);
        } catch (e) {
          setFormError(document.getElementById("sec2faError"), e.message || String(e));
        }
      })().catch(alert),
    );
  }
  const sec2faOffBtn = document.getElementById("sec2faOffBtn");
  if (sec2faOffBtn) {
    sec2faOffBtn.addEventListener("click", () =>
      (async () => {
        setFormError(document.getElementById("sec2faError"), "");
        const pwd = document.getElementById("sec2faOffPwd")?.value || "";
        if (!window.confirm("Отключить 2FA?")) return;
        try {
          const d = await api("/api/auth/2fa/disable", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ password: pwd }),
          });
          alert(d.message || "Ок");
          loadSecurityPage().catch(alert);
        } catch (e) {
          setFormError(document.getElementById("sec2faError"), e.message || String(e));
        }
      })().catch(alert),
    );
  }
  const secSessRevoke = document.getElementById("secSessRevoke");
  if (secSessRevoke) {
    secSessRevoke.addEventListener("click", () =>
      (async () => {
        setFormError(document.getElementById("secSessError"), "");
        try {
          const d = await api("/api/account/sessions/revoke-others", { method: "POST" });
          alert(d.message || "Ок");
          loadSecurityPage().catch(alert);
        } catch (e) {
          setFormError(document.getElementById("secSessError"), e.message || String(e));
        }
      })().catch(alert),
    );
  }

  const secDataExportBtn = document.getElementById("secDataExportBtn");
  if (secDataExportBtn) {
    secDataExportBtn.addEventListener("click", () =>
      downloadAuthedGet("/api/me/export", "export.json").catch((e) => alert(e.message || e)),
    );
  }
  const secDataDeleteBtn = document.getElementById("secDataDeleteBtn");
  if (secDataDeleteBtn) {
    secDataDeleteBtn.addEventListener("click", () =>
      (async () => {
        setFormError(document.getElementById("secDataDeleteError"), "");
        const pwd = String(document.getElementById("secDataDeletePwd")?.value || "").trim();
        if (!pwd) {
          setFormError(document.getElementById("secDataDeleteError"), "Введите пароль.");
          return;
        }
        if (!window.confirm("Удалить аккаунт и выйти? Действие необратимо.")) return;
        try {
          await api("/api/me/delete", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ password: pwd }),
          });
          // Сервер уже очистил cookie auth_token + auth_present.
          leadgenCsrf = null;
          window.location.href = "/auth";
        } catch (e) {
          setFormError(document.getElementById("secDataDeleteError"), e.message || String(e));
        }
      })().catch(alert),
    );
  }

  const leadsDownloadCsv = document.getElementById("leadsDownloadCsv");
  if (leadsDownloadCsv) {
    leadsDownloadCsv.addEventListener("click", () =>
      downloadAuthedGet("/api/leads/export", "sent_leads.csv").catch((e) => alert(e.message || e)),
    );
  }
  const statsLeadsDownloadBtn = document.getElementById("statsLeadsDownloadBtn");
  if (statsLeadsDownloadBtn) {
    statsLeadsDownloadBtn.addEventListener("click", () =>
      downloadAuthedGet("/api/leads/export", "sent_leads.csv").catch((e) => alert(e.message || e)),
    );
  }
  const leadsSearch = document.getElementById("leadsSearch");
  if (leadsSearch) {
    let t = 0;
    leadsSearch.addEventListener("input", () => {
      window.clearTimeout(t);
      t = window.setTimeout(() => renderLeadsTable(), 180);
    });
  }
  bindLeadsTableDelegationOnce();
  const leadsTagFilter = document.getElementById("leadsTagFilter");
  if (leadsTagFilter && !leadsTagFilter.dataset.bound) {
    leadsTagFilter.dataset.bound = "1";
    leadsTagFilter.addEventListener("change", () => renderLeadsTable());
  }

  const leadsDeleteSelected = document.getElementById("leadsDeleteSelected");
  if (leadsDeleteSelected && !leadsDeleteSelected.dataset.bound) {
    leadsDeleteSelected.dataset.bound = "1";
    leadsDeleteSelected.addEventListener("click", () => {
      const ids = Array.from(document.querySelectorAll("input.leads-row-check:checked"))
        .map((el) => el.getAttribute("data-lead-id"))
        .filter(Boolean);
      if (!ids.length) {
        alert("Отметьте строки галочками.");
        return;
      }
      if (!window.confirm(`Удалить выбранные записи (${ids.length})?`)) return;
      (async () => {
        const d = await api("/api/leads/delete", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ids }),
        });
        alert(d.message || "Готово");
        await loadLeadsPage();
      })().catch((e) => alert(e.message || String(e)));
    });
  }
  const leadsDeleteAll = document.getElementById("leadsDeleteAll");
  if (leadsDeleteAll && !leadsDeleteAll.dataset.bound) {
    leadsDeleteAll.dataset.bound = "1";
    leadsDeleteAll.addEventListener("click", () => {
      if (!window.confirm("Очистить весь журнал лидов для организации? Это необратимо.")) return;
      (async () => {
        const d = await api("/api/leads/delete", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ all: true }),
        });
        alert(d.message || "Готово");
        await loadLeadsPage();
      })().catch((e) => alert(e.message || String(e)));
    });
  }

  if (!document.body.dataset.kwGenBound) {
    document.body.dataset.kwGenBound = "1";
    document.body.addEventListener("click", (e) => {
      const b = e.target && e.target.closest ? e.target.closest(".kw-gen-btn") : null;
      if (!b) return;
      const kind = b.getAttribute("data-kw-kind");
      if (!kind) return;
      if (!document.getElementById("kwGenNiche")) {
        alert(
          "Откройте «Настройки и лимиты» и вкладку «Воронка и лимиты» — там поле ниши и генерация групп ключей.",
        );
        return;
      }
      e.preventDefault();
      generateKeywordsForKind(kind).catch((err) => alert(err.message || String(err)));
    });
  }

  const offersRefreshBtn = document.getElementById("offersRefreshBtn");
  if (offersRefreshBtn && !offersRefreshBtn.dataset.bound) {
    offersRefreshBtn.dataset.bound = "1";
    offersRefreshBtn.addEventListener("click", () => loadOffersPage().catch((e) => alert(e.message || e)));
  }
  const offersStatusFilter = document.getElementById("offersStatusFilter");
  if (offersStatusFilter && !offersStatusFilter.dataset.bound) {
    offersStatusFilter.dataset.bound = "1";
    offersStatusFilter.addEventListener("change", () => loadOffersPage().catch((e) => alert(e.message || e)));
  }

  const callSaveBtn = document.getElementById("callSaveBtn");
  if (callSaveBtn && !callSaveBtn.dataset.bound) {
    callSaveBtn.dataset.bound = "1";
    callSaveBtn.addEventListener("click", () => {
      (async () => {
        const g = (id) => document.getElementById(id);
        const lead_user_id = String(g("callLeadUserId")?.value || "").trim();
        if (!lead_user_id) {
          alert("Укажите User ID лида.");
          return;
        }
        await api("/api/calls", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            lead_user_id,
            lead_username: String(g("callLeadUsername")?.value || "").trim(),
            scheduled_at: String(g("callScheduledAt")?.value || "").trim(),
            duration_min: Number(g("callDuration")?.value || 30),
            outcome: String(g("callOutcome")?.value || "planned"),
            notes: String(g("callNotes")?.value || "").trim(),
          }),
        });
        if (g("callNotes")) g("callNotes").value = "";
        await loadCallsPage();
      })().catch((e) => alert(e.message || String(e)));
    });
  }
  const callsRefreshBtn = document.getElementById("callsRefreshBtn");
  if (callsRefreshBtn && !callsRefreshBtn.dataset.bound) {
    callsRefreshBtn.dataset.bound = "1";
    callsRefreshBtn.addEventListener("click", () => loadCallsPage().catch((e) => alert(e.message || e)));
  }

  const headerLoginBtn = document.getElementById("headerLoginBtn");
  if (headerLoginBtn) headerLoginBtn.addEventListener("click", () => { window.location.href = "/auth"; });

  const sbLogoutBtn = document.getElementById("sbLogoutBtn");
  if (sbLogoutBtn) sbLogoutBtn.addEventListener("click", () => { logoutUser().catch(alert); });

  const avatarUploadBtn = document.getElementById("avatarUploadBtn");
  if (avatarUploadBtn) avatarUploadBtn.addEventListener("click", () => uploadMyAvatar().catch(alert));
  const avatarClearBtn = document.getElementById("avatarClearBtn");
  if (avatarClearBtn) avatarClearBtn.addEventListener("click", () => clearMyAvatar().catch(alert));

  const refreshUsersBtn = document.getElementById("refreshUsersBtn");
  if (refreshUsersBtn) refreshUsersBtn.addEventListener("click", () => refreshUsers().catch(alert));

  const adminRestartBtn = document.getElementById("adminRestartBtn");
  if (adminRestartBtn && !adminRestartBtn.dataset.bound) {
    adminRestartBtn.dataset.bound = "1";
    adminRestartBtn.addEventListener("click", () => adminRequestRestart().catch((e) => alert(e.message || e)));
  }
  const adminRestartInfoBtn = document.getElementById("adminRestartInfoBtn");
  if (adminRestartInfoBtn && !adminRestartInfoBtn.dataset.bound) {
    adminRestartInfoBtn.dataset.bound = "1";
    adminRestartInfoBtn.addEventListener("click", () => refreshAdminRestartPanel().catch(() => {}));
  }

  const adminUserEmailSearch = document.getElementById("adminUserEmailSearch");
  if (adminUserEmailSearch) adminUserEmailSearch.addEventListener("input", () => renderUsersTable());

  const adminUserRegFrom = document.getElementById("adminUserRegFrom");
  if (adminUserRegFrom) adminUserRegFrom.addEventListener("change", () => renderUsersTable());

  const adminUserRegTo = document.getElementById("adminUserRegTo");
  if (adminUserRegTo) adminUserRegTo.addEventListener("change", () => renderUsersTable());

  const adminUserFilterReset = document.getElementById("adminUserFilterReset");
  if (adminUserFilterReset) {
    adminUserFilterReset.addEventListener("click", () => {
      const s = document.getElementById("adminUserEmailSearch");
      const a = document.getElementById("adminUserRegFrom");
      const b = document.getElementById("adminUserRegTo");
      if (s) s.value = "";
      if (a) a.value = "";
      if (b) b.value = "";
      renderUsersTable();
    });
  }

  const adminPlansTable = document.getElementById("adminPlansTable");
  if (adminPlansTable && !adminPlansTable.dataset.plansClickBound) {
    adminPlansTable.dataset.plansClickBound = "1";
    adminPlansTable.addEventListener("click", (e) => {
      const btn = e.target && e.target.closest ? e.target.closest("[data-admin-plan-save]") : null;
      if (!btn) return;
      const pid = btn.getAttribute("data-admin-plan-save");
      if (!pid) return;
      saveAdminPlan(pid)
        .then((d) => alert((d && d.message) || "Сохранено"))
        .catch((err) => alert(err.message || String(err)));
    });
  }
}

async function saveChatsLlmPage(options = {}) {
  const quiet = Boolean(options.quiet);
  const skipLoadAfter = Boolean(options.skipLoadAfter);
  if (window.__leadgenOrgRole !== "admin") {
    if (!quiet) alert("Настройки LLM может менять только администратор организации.");
    return;
  }
  const g = (id) => document.getElementById(id);
  if (!g("llmEnabled")) return;
  const prev = await api("/api/config");
  const llmKeyRaw = String(g("llmApiKey")?.value || "").trim();
  const next = { ...prev, llm: { ...(prev.llm || {}) } };
  next.llm.enabled = Boolean(g("llmEnabled")?.checked);
  next.llm.base_url = String(g("llmBaseUrl")?.value || "").trim() || "https://api.openai.com/v1";
  next.llm.model = String(g("llmModel")?.value || "").trim() || "gpt-4o-mini";
  if (llmKeyRaw && llmKeyRaw !== "••••••••") {
    next.llm.api_key = llmKeyRaw;
  } else if (prev.llm && prev.llm.api_key) {
    next.llm.api_key = prev.llm.api_key;
  } else {
    next.llm.api_key = "";
  }
  const d = await api("/api/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(next),
  });
  if (!quiet) alert(d.message || "Сохранено");
  chatsLlmFormLoaded = false;
  if (!skipLoadAfter) await loadChatsPage();
}

const LEADGEN_VOLATILE_PREFIX = "leadgen_v1_volatile";

function getCurrentPageId() {
  return (document.body && document.body.dataset && document.body.dataset.page) || "";
}

function _volatileKey(suffix) {
  const uid = currentUserId != null ? String(currentUserId) : "anon";
  return `${LEADGEN_VOLATILE_PREFIX}_${uid}_${suffix}`;
}

function _localSearchPrefsKey() {
  const uid = currentUserId != null ? String(currentUserId) : "anon";
  return `leadgen_v1_prefs_search_${uid}`;
}

function clearLeadgenVolatileSession() {
  try {
    for (let i = sessionStorage.length - 1; i >= 0; i--) {
      const k = sessionStorage.key(i);
      if (k && k.startsWith(LEADGEN_VOLATILE_PREFIX)) {
        sessionStorage.removeItem(k);
      }
    }
  } catch {
    /* ignore */
  }
}

function saveVolatileGlobalSync() {
  try {
    const details = document.querySelectorAll("aside.sidebar details");
    const openIdx = [];
    details.forEach((d, i) => {
      if (d.open) openIdx.push(i);
    });
    sessionStorage.setItem(_volatileKey("global"), JSON.stringify({ v: 1, openDetails: openIdx }));
  } catch {
    /* ignore */
  }
}

function _saveSearchPrefsToLocal() {
  const g = (id) => document.getElementById(id);
  if (!g("searchNicheDesc")) return;
  try {
    const o = {
      v: 1,
      searchNicheDesc: (g("searchNicheDesc") && g("searchNicheDesc").value) || "",
      searchAudience: (g("searchAudience") && g("searchAudience").value) || "",
      searchStopWords: (g("searchStopWords") && g("searchStopWords").value) || "",
      searchMinSub: (g("searchMinSub") && g("searchMinSub").value) || "",
      searchMaxIdle: (g("searchMaxIdle") && g("searchMaxIdle").value) || "",
      searchIncludeStale: g("searchIncludeStale") ? Boolean(g("searchIncludeStale").checked) : false,
      searchEnrichOnly: g("searchEnrichOnly") ? Boolean(g("searchEnrichOnly").checked) : false,
    };
    localStorage.setItem(_localSearchPrefsKey(), JSON.stringify(o));
  } catch {
    /* ignore */
  }
}

function saveVolatileSearchSync() {
  if (!document.getElementById("searchKeywordsText")) return;
  const g = (id) => document.getElementById(id);
  let selected = [];
  try {
    selected = Array.from(getSelectedSearchRefs());
  } catch {
    selected = [];
  }
  const payload = {
    v: 1,
    master: searchChannelsMaster,
    selected,
    searchKeywordsText: (g("searchKeywordsText") && g("searchKeywordsText").value) || "",
    searchNicheDesc: (g("searchNicheDesc") && g("searchNicheDesc").value) || "",
    searchAudience: (g("searchAudience") && g("searchAudience").value) || "",
    searchStopWords: (g("searchStopWords") && g("searchStopWords").value) || "",
    searchKeywordsCount: (g("searchKeywordsCount") && g("searchKeywordsCount").value) || "15",
    searchMinSub: (g("searchMinSub") && g("searchMinSub").value) || "",
    searchMaxIdle: (g("searchMaxIdle") && g("searchMaxIdle").value) || "",
    searchIncludeStale: g("searchIncludeStale") ? g("searchIncludeStale").checked : false,
    searchEnrichOnly: g("searchEnrichOnly") ? g("searchEnrichOnly").checked : false,
    searchChannelsLimit: (g("searchChannelsLimit") && g("searchChannelsLimit").value) || "20",
    searchChannelsQuery: (g("searchChannelsQuery") && g("searchChannelsQuery").value) || "",
    searchLog: (g("searchLog") && g("searchLog").textContent) || "",
    searchTypeFilter: getSearchTypeFilterValue(),
    folderName: (g("folderName") && g("folderName").value) || "",
    foldersOutput: (g("foldersOutput") && g("foldersOutput").textContent) || "",
    searchChannelsOkText: (g("searchChannelsOk") && g("searchChannelsOk").textContent) || "",
    searchChannelsOkHidden: g("searchChannelsOk") ? g("searchChannelsOk").classList.contains("hidden") : true,
  };
  try {
    sessionStorage.setItem(_volatileKey("search"), JSON.stringify(payload));
  } catch {
    try {
      const light = { ...payload, master: [], selected: [] };
      sessionStorage.setItem(_volatileKey("search"), JSON.stringify(light));
    } catch {
      /* give up */
    }
  }
  _saveSearchPrefsToLocal();
}

/**
 * @param {boolean} hadSessionSnapshot — если true, из localStorage подмешиваем только пустые текстовые поля (ниша/аудитория/стоп).
 */
function mergeLocalSearchPrefsIntoForm(hadSessionSnapshot) {
  const g = (id) => document.getElementById(id);
  if (!g("searchNicheDesc")) return;
  let raw;
  try {
    raw = localStorage.getItem(_localSearchPrefsKey());
  } catch {
    return;
  }
  if (!raw) return;
  let o;
  try {
    o = JSON.parse(raw);
  } catch {
    return;
  }
  if (!o || o.v !== 1) return;
  if (g("searchNicheDesc") && !String(g("searchNicheDesc").value).trim() && o.searchNicheDesc) {
    g("searchNicheDesc").value = o.searchNicheDesc;
  }
  if (g("searchAudience") && !String(g("searchAudience").value).trim() && o.searchAudience) {
    g("searchAudience").value = o.searchAudience;
  }
  if (g("searchStopWords") && !String(g("searchStopWords").value).trim() && o.searchStopWords) {
    g("searchStopWords").value = o.searchStopWords;
  }
  if (hadSessionSnapshot) return;
  if (g("searchMinSub") && o.searchMinSub !== undefined && o.searchMinSub !== "") g("searchMinSub").value = o.searchMinSub;
  if (g("searchMaxIdle") && o.searchMaxIdle !== undefined && o.searchMaxIdle !== "") g("searchMaxIdle").value = o.searchMaxIdle;
  if (g("searchIncludeStale") && o.searchIncludeStale != null) g("searchIncludeStale").checked = o.searchIncludeStale;
  if (g("searchEnrichOnly") && o.searchEnrichOnly != null) g("searchEnrichOnly").checked = o.searchEnrichOnly;
}

function saveVolatileChatsSync() {
  const sn = document.getElementById("chatsLeadSnippet");
  const out = document.getElementById("chatsLlmOutput");
  if (!sn && !out) return;
  try {
    sessionStorage.setItem(
      _volatileKey("chats"),
      JSON.stringify({
        v: 1,
        leadSnippet: sn ? sn.value : "",
        llmOutput: out ? out.textContent : "",
      }),
    );
  } catch {
    /* ignore */
  }
}

function saveVolatileConfigSync() {
  const editor = document.getElementById("configEditor");
  const box = document.getElementById("advancedBox");
  if (!editor && !box) return;
  try {
    sessionStorage.setItem(
      _volatileKey("config"),
      JSON.stringify({
        v: 1,
        advancedOpen: box ? !box.classList.contains("hidden") : false,
        editor: editor ? editor.value : "",
      }),
    );
  } catch {
    /* ignore */
  }
}

function saveVolatileAdminSync() {
  if (!document.getElementById("adminUserEmailSearch")) return;
  try {
    const g = (id) => document.getElementById(id);
    sessionStorage.setItem(
      _volatileKey("admin"),
      JSON.stringify({
        v: 1,
        email: (g("adminUserEmailSearch") && g("adminUserEmailSearch").value) || "",
        from: (g("adminUserRegFrom") && g("adminUserRegFrom").value) || "",
        to: (g("adminUserRegTo") && g("adminUserRegTo").value) || "",
      }),
    );
  } catch {
    /* ignore */
  }
}

function saveVolatileForCurrentPageSync() {
  if (!hasAuthToken()) return;
  const page = getCurrentPageId();
  if (!page || page === "auth") return;
  if (page === "search") saveVolatileSearchSync();
  else if (page === "chats") saveVolatileChatsSync();
  else if (page === "config") saveVolatileConfigSync();
  else if (page === "admin") saveVolatileAdminSync();
  saveVolatileGlobalSync();
}

function restoreVolatileGlobal() {
  let raw;
  try {
    raw = sessionStorage.getItem(_volatileKey("global"));
  } catch {
    return;
  }
  if (!raw) return;
  let o;
  try {
    o = JSON.parse(raw);
  } catch {
    return;
  }
  const openIdx = Array.isArray(o.openDetails) ? o.openDetails : [];
  const details = document.querySelectorAll("aside.sidebar details");
  details.forEach((d, i) => {
    d.open = openIdx.includes(i);
  });
}

function restoreVolatileSearch() {
  const g = (id) => document.getElementById(id);
  if (!g("searchKeywordsText")) return;
  let raw;
  try {
    raw = sessionStorage.getItem(_volatileKey("search"));
  } catch {
    raw = null;
  }
  let hadSession = false;
  if (raw) {
    let o;
    try {
      o = JSON.parse(raw);
    } catch {
      o = null;
    }
    if (o && o.v === 1) {
      hadSession = true;
      if (Array.isArray(o.master)) searchChannelsMaster = o.master;
      if (g("searchKeywordsText") && o.searchKeywordsText != null) g("searchKeywordsText").value = o.searchKeywordsText;
      if (g("searchNicheDesc") && o.searchNicheDesc != null) g("searchNicheDesc").value = o.searchNicheDesc;
      if (g("searchAudience") && o.searchAudience != null) g("searchAudience").value = o.searchAudience;
      if (g("searchStopWords") && o.searchStopWords != null) g("searchStopWords").value = o.searchStopWords;
      if (g("searchKeywordsCount") && o.searchKeywordsCount != null) g("searchKeywordsCount").value = o.searchKeywordsCount;
      if (g("searchMinSub") && o.searchMinSub != null) g("searchMinSub").value = o.searchMinSub;
      if (g("searchMaxIdle") && o.searchMaxIdle != null) g("searchMaxIdle").value = o.searchMaxIdle;
      if (g("searchIncludeStale") && o.searchIncludeStale != null) g("searchIncludeStale").checked = o.searchIncludeStale;
      if (g("searchEnrichOnly") && o.searchEnrichOnly != null) g("searchEnrichOnly").checked = o.searchEnrichOnly;
      if (g("searchChannelsLimit") && o.searchChannelsLimit != null) g("searchChannelsLimit").value = o.searchChannelsLimit;
      if (g("searchChannelsQuery") && o.searchChannelsQuery != null) g("searchChannelsQuery").value = o.searchChannelsQuery;
      if (g("searchLog") && o.searchLog != null) g("searchLog").textContent = o.searchLog;
      const fv = o.searchTypeFilter || "all";
      const radio = document.querySelector(`input[name="searchTypeFilter"][value="${fv}"]`);
      if (radio) radio.checked = true;
      if (g("folderName") && o.folderName != null) g("folderName").value = o.folderName;
      if (g("foldersOutput") && o.foldersOutput != null) g("foldersOutput").textContent = o.foldersOutput;
      const errEl = g("searchChannelsError");
      if (errEl) {
        setFormError(errEl, "");
      }
      const okEl = g("searchChannelsOk");
      if (okEl && o.searchChannelsOkText != null) {
        okEl.textContent = o.searchChannelsOkText;
        if (o.searchChannelsOkHidden) okEl.classList.add("hidden");
        else okEl.classList.remove("hidden");
      }
      renderSearchChannelsTable();
      const sel = Array.isArray(o.selected) ? o.selected : [];
      if (sel.length) {
        document.querySelectorAll("input.search-ch-row").forEach((inp) => {
          const ref = inp.dataset.ref;
          if (ref && sel.includes(ref)) inp.checked = true;
        });
      }
    }
  }
  mergeLocalSearchPrefsIntoForm(hadSession);
}

function restoreVolatileChats() {
  const raw = (() => {
    try {
      return sessionStorage.getItem(_volatileKey("chats"));
    } catch {
      return null;
    }
  })();
  if (!raw) return;
  let o;
  try {
    o = JSON.parse(raw);
  } catch {
    return;
  }
  if (o.v !== 1) return;
  const sn = document.getElementById("chatsLeadSnippet");
  const out = document.getElementById("chatsLlmOutput");
  if (sn && o.leadSnippet != null) sn.value = o.leadSnippet;
  if (out && o.llmOutput != null) out.textContent = o.llmOutput;
}

function restoreVolatileConfig() {
  const raw = (() => {
    try {
      return sessionStorage.getItem(_volatileKey("config"));
    } catch {
      return null;
    }
  })();
  if (!raw) return;
  let o;
  try {
    o = JSON.parse(raw);
  } catch {
    return;
  }
  if (o.v !== 1) return;
  const editor = document.getElementById("configEditor");
  const box = document.getElementById("advancedBox");
  if (!editor || !box) return;
  if (o.advancedOpen) {
    box.classList.remove("hidden");
    if (o.editor) editor.value = o.editor;
  }
}

function restoreVolatileAdmin() {
  const raw = (() => {
    try {
      return sessionStorage.getItem(_volatileKey("admin"));
    } catch {
      return null;
    }
  })();
  if (!raw) return;
  let o;
  try {
    o = JSON.parse(raw);
  } catch {
    return;
  }
  if (o.v !== 1) return;
  if (!document.getElementById("adminUserEmailSearch")) return;
  const g = (id) => document.getElementById(id);
  if (o.email != null && g("adminUserEmailSearch")) g("adminUserEmailSearch").value = o.email;
  if (o.from != null && g("adminUserRegFrom")) g("adminUserRegFrom").value = o.from;
  if (o.to != null && g("adminUserRegTo")) g("adminUserRegTo").value = o.to;
  renderUsersTable();
}

function restoreVolatileForCurrentPage() {
  if (!hasAuthToken()) return;
  const page = getCurrentPageId();
  if (!page || page === "auth") return;
  if (page === "search") restoreVolatileSearch();
  else if (page === "chats") restoreVolatileChats();
  else if (page === "config") restoreVolatileConfig();
  else if (page === "admin") restoreVolatileAdmin();
  restoreVolatileGlobal();
}

/** Перед переходом в другой раздел: тихо сохранить правки текущей страницы (настройки / LLM / список чатов). */
async function saveCurrentPageIfNeeded() {
  saveVolatileForCurrentPageSync();
  if (!hasAuthToken()) return;
  const page = document.body && document.body.dataset && document.body.dataset.page;
  if (!page || page === "auth") return;
  if (page === "config" && document.getElementById("saveConfigBtn")) {
    if (window.__configPageHydrated !== true) return;
    await saveConfigPage({ quiet: true, skipLoadAfter: true });
    return;
  }
  if (
    (page === "folders" || page === "search") &&
    document.getElementById("foldersTargetChats")
  ) {
    await saveFoldersTargetChats({ quiet: true, skipLoadAfter: true, navLeaveGuard: true });
    return;
  }
}

function bindNavSaveOnLeave() {
  document.querySelectorAll("aside a.nav-link[href^='/'], aside a.nav-sublink[href^='/']").forEach((a) => {
    a.addEventListener("click", (e) => {
      if (e.defaultPrevented) return;
      if (e.button !== 0) return;
      if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
      const href = a.getAttribute("href");
      if (!href || href === "#") return;
      if (a.target === "_blank") return;
      e.preventDefault();
      closeMobileSidebar();
      saveCurrentPageIfNeeded()
        .then(() => {
          window.location.href = href;
        })
        .catch((err) => {
          alert(err.message || String(err));
        });
    });
  });
}

const KW_GROUP_FIELD = {
  hot_lead: "kwHot",
  required_intent_hot_lead: "kwIntent",
  exclude_hot_lead: "kwExclude",
  negative: "kwNegative",
  qualification: "kwQual",
  interested: "kwInterested",
  bio_block: "kwBioBlock",
};

/** @type {{ taId: string, keywords: string[], kind: string }} */
let kwPreviewState = { taId: "", keywords: [], kind: "" };

function kwLinesDedupeAgainstExisting(existingText, newLines, dedup) {
  if (!dedup) return newLines.slice();
  const cur = String(existingText || "")
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean);
  const lower = new Set(cur.map((s) => s.toLowerCase()));
  return newLines.filter((k) => k && !lower.has(String(k).trim().toLowerCase()));
}

function renderKwPreviewList() {
  const list = document.getElementById("kwPreviewList");
  if (!list) return;
  list.innerHTML = "";
  kwPreviewState.keywords.forEach((phrase, idx) => {
    const row = document.createElement("div");
    row.className = "kw-preview-row";
    const id = `kwPv_${idx}`;
    row.innerHTML = `<input type="checkbox" id="${id}" class="checkbox" data-kw-idx="${idx}" checked><label for="${id}"></label>`;
    const lab = row.querySelector("label");
    if (lab) lab.textContent = phrase;
    list.appendChild(row);
  });
}

function openKwPreviewPanel(kind, taId, keywords) {
  const panel = document.getElementById("kwPreviewPanel");
  const title = document.getElementById("kwPreviewTarget");
  if (!panel || !title) return;
  kwPreviewState = { taId, keywords: keywords.map((k) => String(k || "").trim()).filter(Boolean), kind };
  title.textContent = `Группа: ${kind} → поле #${taId}`;
  renderKwPreviewList();
  panel.classList.remove("hidden");
}

function closeKwPreviewPanel() {
  const panel = document.getElementById("kwPreviewPanel");
  if (panel) panel.classList.add("hidden");
  kwPreviewState = { taId: "", keywords: [], kind: "" };
}

function bindKwPreviewOnce() {
  if (document.body.dataset.kwPreviewBound) return;
  document.body.dataset.kwPreviewBound = "1";
  const apply = document.getElementById("kwPreviewApply");
  const cancel = document.getElementById("kwPreviewCancel");
  const selAll = document.getElementById("kwPreviewSelectAll");
  if (apply) {
    apply.addEventListener("click", () => {
      const ta = document.getElementById(kwPreviewState.taId);
      if (!ta || !kwPreviewState.keywords.length) {
        closeKwPreviewPanel();
        return;
      }
      const mode =
        (document.querySelector('input[name="kwInsertMode"]:checked') || {}).value === "replace"
          ? "replace"
          : "append";
      const dedup = !!(document.getElementById("kwDedup") && document.getElementById("kwDedup").checked);
      const picked = [];
      document.querySelectorAll("#kwPreviewList input[type=checkbox][data-kw-idx]").forEach((inp) => {
        if (inp.checked) {
          const i = Number(inp.getAttribute("data-kw-idx"));
          if (kwPreviewState.keywords[i] != null) picked.push(kwPreviewState.keywords[i]);
        }
      });
      if (!picked.length) {
        alert("Отметьте хотя бы одну фразу.");
        return;
      }
      const cur = String(ta.value || "");
      const mergedPick = kwLinesDedupeAgainstExisting(mode === "append" ? cur : "", picked, dedup);
      if (mode === "replace") {
        ta.value = mergedPick.join("\n");
      } else {
        const base = cur.trim();
        const next = mergedPick.join("\n");
        ta.value = base ? `${base}\n${next}` : next;
      }
      closeKwPreviewPanel();
    });
  }
  if (cancel) cancel.addEventListener("click", () => closeKwPreviewPanel());
  if (selAll) {
    selAll.addEventListener("click", () => {
      const boxes = document.querySelectorAll("#kwPreviewList input[type=checkbox]");
      if (!boxes.length) return;
      const anyOff = Array.from(boxes).some((b) => !b.checked);
      boxes.forEach((b) => {
        b.checked = anyOff;
      });
    });
  }
}

async function generateKeywordsForKind(kind) {
  const nicheEl = document.getElementById("kwGenNiche");
  const audEl = document.getElementById("kwGenAudience");
  const taId = KW_GROUP_FIELD[kind];
  if (!nicheEl) {
    alert("Страница настроек не загружена полностью. Обновите «Настройки и лимиты».");
    return;
  }
  if (!taId) {
    alert(`Неизвестная группа ключей: ${kind}. Обновите страницу (кэш app.js).`);
    return;
  }
  const niche = String(nicheEl.value || "").trim();
  if (niche.length < 3) {
    alert("Укажите нишу / продукт (не короче 3 символов).");
    return;
  }
  const ta = document.getElementById(taId);
  if (!ta) {
    alert(`Не найдено текстовое поле для группы «${kind}». Обновите страницу.`);
    return;
  }
  try {
    const d = await api("/api/config/generate-keywords-group", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        niche,
        audience: String(audEl?.value || "").trim(),
        kind,
        count: 20,
      }),
    });
    const lines = Array.isArray(d.keywords) ? d.keywords : [];
    bindKwPreviewOnce();
    openKwPreviewPanel(kind, taId, lines);
  } catch (e) {
    alert(e.message || String(e));
  }
}

function bindOffersTableActionsOnce() {
  const tbl = document.getElementById("offersTable");
  if (!tbl || tbl.dataset.leadgenOffersActions) return;
  tbl.dataset.leadgenOffersActions = "1";
  tbl.addEventListener("click", (e) => {
    const t = e.target;
    if (!t || !t.closest) return;
    const saveBtn = t.closest("[data-outreach-save]");
    const apprBtn = t.closest("[data-outreach-approve]");
    const rejBtn = t.closest("[data-outreach-reject]");
    const regenBtn = t.closest("[data-outreach-regenerate]");
    const callBtn = t.closest("[data-outreach-call]");
    const run = async () => {
      if (saveBtn) {
        const id = saveBtn.getAttribute("data-outreach-save");
        const tr = saveBtn.closest("tr");
        const ta = tr && tr.querySelector("textarea.outreach-draft");
        const draft = ta ? String(ta.value || "").trim() : "";
        if (draft.length < 2) {
          alert("Текст слишком короткий.");
          return;
        }
        await api(`/api/outreach/${id}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ draft_text: draft }),
        });
        await loadOffersPage();
        return;
      }
      if (regenBtn) {
        const id = regenBtn.getAttribute("data-outreach-regenerate");
        const tr = regenBtn.closest("tr");
        const ta = tr && tr.querySelector("textarea.outreach-draft");
        const d = await api(`/api/outreach/${id}/regenerate`, { method: "POST" });
        if (ta && d.draft_text) ta.value = String(d.draft_text);
        else await loadOffersPage();
        return;
      }
      if (callBtn) {
        const id = callBtn.getAttribute("data-outreach-call");
        const uid = callBtn.getAttribute("data-lead-user-id") || "";
        const un = callBtn.getAttribute("data-lead-username") || "";
        const conv = callBtn.getAttribute("data-conversation-id") || "";
        await api("/api/calls", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            lead_user_id: uid,
            lead_username: un,
            outcome: "planned",
            notes: `Из оффера #${id}`,
            conversation_id: conv ? Number(conv) : undefined,
          }),
        });
        alert("Созвон добавлен. Откройте раздел «Созвоны».");
        return;
      }
      if (apprBtn) {
        const id = apprBtn.getAttribute("data-outreach-approve");
        const tr = apprBtn.closest("tr");
        const ta = tr && tr.querySelector("textarea.outreach-draft");
        const draft = ta ? String(ta.value || "").trim() : "";
        await api(`/api/outreach/${id}/approve`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(draft ? { draft_text: draft } : {}),
        });
        await loadOffersPage();
        return;
      }
      if (rejBtn) {
        const id = rejBtn.getAttribute("data-outreach-reject");
        if (!window.confirm("Отклонить эту отправку?")) return;
        await api(`/api/outreach/${id}/reject`, { method: "POST" });
        await loadOffersPage();
      }
    };
    if (saveBtn || apprBtn || rejBtn || regenBtn || callBtn) {
      e.preventDefault();
      run().catch((err) => alert(err.message || String(err)));
    }
  });
}

async function loadOffersPage() {
  const tbody = document.getElementById("offersTbody");
  if (!tbody) return;
  const errEl = document.getElementById("offersError");
  const stSel = document.getElementById("offersStatusFilter");
  const st = stSel ? String(stSel.value || "").trim() : "";
  if (errEl) {
    errEl.textContent = "";
    errEl.classList.add("hidden");
  }
  try {
    const q = st ? `?status=${encodeURIComponent(st)}` : "";
    const d = await api(`/api/outreach${q}`);
    const items = Array.isArray(d.items) ? d.items : [];
    tbody.innerHTML = "";
    for (const it of items) {
      const tr = document.createElement("tr");
      const id = it.id;
      const status = String(it.status || "");
      const userLbl = [it.username, it.user_id].filter(Boolean).map(String).join(" · ");
      const td0 = document.createElement("td");
      td0.textContent = String(id ?? "");
      const td1 = document.createElement("td");
      td1.textContent = status;
      const td2 = document.createElement("td");
      td2.textContent = String(it.stage ?? "");
      const td3 = document.createElement("td");
      td3.textContent = userLbl;
      const td4 = document.createElement("td");
      td4.textContent = String(it.source_chat || "");
      const td5 = document.createElement("td");
      const canEdit = status === "pending";
      if (canEdit) {
        const ta = document.createElement("textarea");
        ta.className = "textarea-sm outreach-draft";
        ta.rows = 3;
        ta.value = String(it.draft_text || "");
        td5.appendChild(ta);
      } else {
        const pre = document.createElement("div");
        pre.className = "muted";
        pre.style.whiteSpace = "pre-wrap";
        pre.style.maxWidth = "420px";
        pre.textContent = String(it.draft_text || "");
        td5.appendChild(pre);
      }
      const td6 = document.createElement("td");
      td6.className = "offers-actions";
      if (canEdit) {
        const bSave = document.createElement("button");
        bSave.type = "button";
        bSave.className = "secondary";
        bSave.setAttribute("data-outreach-save", String(id));
        bSave.textContent = "Сохранить";
        const bReg = document.createElement("button");
        bReg.type = "button";
        bReg.className = "secondary";
        bReg.setAttribute("data-outreach-regenerate", String(id));
        bReg.textContent = "Регенерировать";
        const bCall = document.createElement("button");
        bCall.type = "button";
        bCall.className = "secondary";
        bCall.setAttribute("data-outreach-call", String(id));
        bCall.setAttribute("data-lead-user-id", String(it.user_id ?? ""));
        bCall.setAttribute("data-lead-username", String(it.username ?? ""));
        if (it.conversation_id != null && it.conversation_id !== "") {
          bCall.setAttribute("data-conversation-id", String(it.conversation_id));
        }
        bCall.textContent = "Созвон";
        const bOk = document.createElement("button");
        bOk.type = "button";
        bOk.setAttribute("data-outreach-approve", String(id));
        bOk.textContent = "Одобрить";
        const bNo = document.createElement("button");
        bNo.type = "button";
        bNo.className = "secondary";
        bNo.setAttribute("data-outreach-reject", String(id));
        bNo.textContent = "Отклонить";
        td6.appendChild(bSave);
        td6.appendChild(document.createTextNode(" "));
        td6.appendChild(bReg);
        td6.appendChild(document.createTextNode(" "));
        td6.appendChild(bCall);
        td6.appendChild(document.createTextNode(" "));
        td6.appendChild(bOk);
        td6.appendChild(document.createTextNode(" "));
        td6.appendChild(bNo);
      } else {
        const sp = document.createElement("span");
        sp.className = "muted";
        sp.textContent = "—";
        td6.appendChild(sp);
      }
      tr.appendChild(td0);
      tr.appendChild(td1);
      tr.appendChild(td2);
      tr.appendChild(td3);
      tr.appendChild(td4);
      tr.appendChild(td5);
      tr.appendChild(td6);
      tbody.appendChild(tr);
    }
  } catch (e) {
    if (errEl) {
      errEl.textContent = e.message || String(e);
      errEl.classList.remove("hidden");
    }
    tbody.innerHTML = "";
  }
}

async function loadConversationsPage() {
  const tbody = document.getElementById("convTbody");
  if (!tbody) return;
  const errEl = document.getElementById("convError");
  if (errEl) {
    errEl.textContent = "";
    errEl.classList.add("hidden");
  }
  try {
    const d = await api("/api/conversations");
    const items = Array.isArray(d.items) ? d.items : [];
    tbody.innerHTML = "";
    for (const it of items) {
      const tr = document.createElement("tr");
      const hid = Array.isArray(it.history) ? it.history : [];
      const last = hid
        .slice(-4)
        .map((h) =>
          h && h.text ? `${h.role || "?"}: ${String(h.text).slice(0, 72)}${String(h.text).length > 72 ? "…" : ""}` : "",
        )
        .filter(Boolean)
        .join(" · ");
      const id = it.id;
      tr.innerHTML = `<td>${escapeHtmlCell(String(id ?? ""))}</td><td>${escapeHtmlCell(String(it.status || ""))}</td><td>${escapeHtmlCell(String(it.current_stage ?? ""))}</td><td>${escapeHtmlCell([it.lead_username, it.lead_user_id].filter(Boolean).join(" · "))}</td><td>${escapeHtmlCell(String(it.source_chat || ""))}</td><td class="muted">${escapeHtmlCell(last || "—")}</td><td class="conv-actions-cell"></td>`;
      const tdAct = tr.querySelector(".conv-actions-cell");
      if (tdAct) {
        const run = (fn) => {
          fn().catch((e) => alert(e.message || String(e)));
        };
        const mk = (label, variant) => {
          const b = document.createElement("button");
          b.type = "button";
          b.className = variant === "primary" ? "" : "secondary";
          b.textContent = label;
          return b;
        };
        const bGen = mk("LLM дальше", "sec");
        bGen.addEventListener("click", () => {
          run(async () => {
            await api(`/api/conversations/${id}/generate-next`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ advance_stage: false }),
            });
            await loadConversationsPage();
          });
        });
        const bAdv = mk("+этап", "sec");
        bAdv.title = "Сгенерировать и сдвинуть этап 1→2→3";
        bAdv.addEventListener("click", () => {
          run(async () => {
            await api(`/api/conversations/${id}/generate-next`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ advance_stage: true }),
            });
            await loadConversationsPage();
          });
        });
        const bSkip = mk("Пропуск", "sec");
        bSkip.addEventListener("click", () => {
          run(async () => {
            if (!window.confirm("Пометить переписку как пропущенную (ignored)?")) return;
            await api(`/api/conversations/${id}/skip`, { method: "POST" });
            await loadConversationsPage();
          });
        });
        const bDead = mk("Dead", "sec");
        bDead.addEventListener("click", () => {
          run(async () => {
            if (!window.confirm("Закрыть переписку как dead?")) return;
            await api(`/api/conversations/${id}/dead`, { method: "POST" });
            await loadConversationsPage();
          });
        });
        tdAct.appendChild(bGen);
        tdAct.appendChild(document.createTextNode(" "));
        tdAct.appendChild(bAdv);
        tdAct.appendChild(document.createTextNode(" "));
        tdAct.appendChild(bSkip);
        tdAct.appendChild(document.createTextNode(" "));
        tdAct.appendChild(bDead);
      }
      tbody.appendChild(tr);
    }
  } catch (e) {
    if (errEl) {
      errEl.textContent = e.message || String(e);
      errEl.classList.remove("hidden");
    }
    tbody.innerHTML = "";
  }
}

function bindConversationsPageOnce() {
  const b = document.getElementById("convRefreshBtn");
  if (b && !b.dataset.bound) {
    b.dataset.bound = "1";
    b.addEventListener("click", () => loadConversationsPage().catch((e) => alert(e.message || String(e))));
  }
}

async function loadCallsPage() {
  const tbody = document.getElementById("callsTbody");
  if (!tbody) return;
  const errEl = document.getElementById("callsError");
  if (errEl) {
    errEl.textContent = "";
    errEl.classList.add("hidden");
  }
  try {
    const d = await api("/api/calls");
    const items = Array.isArray(d.items) ? d.items : [];
    tbody.innerHTML = "";
    for (const it of items) {
      const tr = document.createElement("tr");
      const id = it.id;
      const convCell =
        it.conversation_id != null && it.conversation_id !== ""
          ? escapeHtmlCell(String(it.conversation_id))
          : "—";
      tr.innerHTML = `<td>${escapeHtmlCell(String(id ?? ""))}</td><td>${escapeHtmlCell(String(it.scheduled_at || it.created_at || ""))}</td><td>${escapeHtmlCell([it.lead_username, it.lead_user_id].filter(Boolean).join(" · "))}</td><td>${convCell}</td><td>${escapeHtmlCell(String(it.outcome || ""))}</td><td>${escapeHtmlCell(String(it.duration_min ?? ""))}</td><td>${escapeHtmlCell(String(it.notes || ""))}</td>`;
      const tdDel = document.createElement("td");
      const del = document.createElement("button");
      del.type = "button";
      del.className = "secondary";
      del.textContent = "Удалить";
      del.addEventListener("click", () => {
        (async () => {
          if (!window.confirm("Удалить запись о созвоне?")) return;
          await api(`/api/calls/${id}`, { method: "DELETE" });
          await loadCallsPage();
        })().catch((err) => alert(err.message || String(err)));
      });
      tdDel.appendChild(del);
      tr.appendChild(tdDel);
      tbody.appendChild(tr);
    }
  } catch (e) {
    if (errEl) {
      errEl.textContent = e.message || String(e);
      errEl.classList.remove("hidden");
    }
    tbody.innerHTML = "";
  }
}

function _renderWebLeadContacts(item) {
  const td = document.createElement("td");
  const lists = [
    { label: "✉", values: item.emails || [] },
    { label: "☎", values: item.phones || [] },
    { label: "TG", values: (item.telegrams || []).map((t) => `@${t}`) },
    { label: "WA", values: item.whatsapps || [] },
    { label: "VK", values: (item.vks || []).map((v) => `vk.com/${v}`) },
  ];
  let any = false;
  for (const grp of lists) {
    if (!grp.values.length) continue;
    any = true;
    const row = document.createElement("div");
    row.className = "muted";
    const tag = document.createElement("strong");
    tag.textContent = `${grp.label}: `;
    row.appendChild(tag);
    row.appendChild(document.createTextNode(grp.values.slice(0, 5).join(", ")));
    td.appendChild(row);
  }
  if (!any) {
    td.textContent = "—";
    td.className = "muted";
  }
  return td;
}

async function loadWebLeadsPage() {
  const tbody = document.getElementById("webLeadsTbody");
  if (!tbody) return;
  const errEl = document.getElementById("webLeadsError");
  const meta = document.getElementById("webLeadsListMeta");
  const keyHint = document.getElementById("webLeadsKeyHint");
  if (errEl) {
    errEl.textContent = "";
    errEl.classList.add("hidden");
  }
  try {
    const d = await api("/api/web-leads?limit=200");
    if (keyHint) {
      if (d.serpapi_key_configured) keyHint.classList.add("hidden");
      else keyHint.classList.remove("hidden");
    }
    const items = Array.isArray(d.items) ? d.items : [];
    tbody.innerHTML = "";
    if (meta) {
      const pending = Number(d.pending_jobs || 0);
      meta.textContent = `Записей: ${items.length}${pending ? ` · в очереди: ${pending}` : ""}`;
    }
    for (const it of items) {
      const tr = document.createElement("tr");

      const tdDomain = document.createElement("td");
      const dStrong = document.createElement("strong");
      dStrong.textContent = it.domain || "";
      tdDomain.appendChild(dStrong);
      if (it.url) {
        const hint = document.createElement("div");
        hint.className = "muted";
        const a = document.createElement("a");
        a.href = it.url;
        a.target = "_blank";
        a.rel = "nofollow noopener";
        a.textContent = "открыть сайт";
        hint.appendChild(a);
        tdDomain.appendChild(hint);
      }
      if (it.title) {
        const tt = document.createElement("div");
        tt.className = "muted";
        tt.textContent = it.title;
        tdDomain.appendChild(tt);
      }
      tr.appendChild(tdDomain);

      tr.appendChild(_renderWebLeadContacts(it));

      const tdSrc = document.createElement("td");
      tdSrc.textContent = it.source || "";
      if (it.query) {
        const q = document.createElement("div");
        q.className = "muted";
        q.textContent = `«${it.query}»`;
        tdSrc.appendChild(q);
      }
      tr.appendChild(tdSrc);

      const tdStatus = document.createElement("td");
      tdStatus.textContent = it.status || "";
      if (it.last_error) {
        const er = document.createElement("div");
        er.className = "muted";
        er.textContent = it.last_error;
        tdStatus.appendChild(er);
      }
      tr.appendChild(tdStatus);

      const tdUpd = document.createElement("td");
      tdUpd.textContent = it.updated_at || it.created_at || "";
      tr.appendChild(tdUpd);

      const tdAct = document.createElement("td");
      const refresh = document.createElement("button");
      refresh.type = "button";
      refresh.className = "secondary";
      refresh.textContent = "Обновить";
      refresh.addEventListener("click", () => {
        (async () => {
          await api(`/api/web-leads/${it.id}/refresh`, { method: "POST" });
          await loadWebLeadsPage();
        })().catch((err) => alert(err.message || String(err)));
      });
      tdAct.appendChild(refresh);

      const promote = document.createElement("button");
      promote.type = "button";
      const hasContact = (it.emails || []).length || (it.phones || []).length || (it.telegrams || []).length || (it.whatsapps || []).length;
      if (it.conversation_id) {
        promote.textContent = "В CRM";
        promote.disabled = true;
      } else {
        promote.textContent = "В CRM";
        promote.disabled = !hasContact;
        promote.addEventListener("click", () => {
          (async () => {
            await api(`/api/web-leads/${it.id}/promote`, { method: "POST" });
            await loadWebLeadsPage();
          })().catch((err) => alert(err.message || String(err)));
        });
      }
      tdAct.appendChild(promote);

      const del = document.createElement("button");
      del.type = "button";
      del.className = "secondary";
      del.textContent = "Удалить";
      del.addEventListener("click", () => {
        (async () => {
          if (!window.confirm(`Удалить запись «${it.domain}»?`)) return;
          await api(`/api/web-leads/${it.id}`, { method: "DELETE" });
          await loadWebLeadsPage();
        })().catch((err) => alert(err.message || String(err)));
      });
      tdAct.appendChild(del);

      tr.appendChild(tdAct);
      tbody.appendChild(tr);
    }
  } catch (e) {
    if (errEl) {
      errEl.textContent = e.message || String(e);
      errEl.classList.remove("hidden");
    }
    tbody.innerHTML = "";
  }
}

async function submitWebLeadsSearch() {
  const q = String(document.getElementById("webLeadsQuery")?.value || "").trim();
  const count = Number(document.getElementById("webLeadsCount")?.value || 20) || 20;
  const gl = String(document.getElementById("webLeadsGl")?.value || "ru").trim() || "ru";
  const msg = document.getElementById("webLeadsSearchMsg");
  if (msg) msg.textContent = "";
  if (q.length < 2) {
    if (msg) msg.textContent = "Введите запрос (минимум 2 символа).";
    return;
  }
  try {
    const d = await api("/api/web-leads/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: q, count, gl, hl: gl }),
    });
    if (msg) msg.textContent = d.message || "Запрос поставлен в очередь";
    setTimeout(() => loadWebLeadsPage().catch(() => {}), 800);
  } catch (e) {
    if (msg) msg.textContent = e.message || String(e);
  }
}

async function submitWebLeadsImport() {
  const ta = document.getElementById("webLeadsImportArea");
  const msg = document.getElementById("webLeadsImportMsg");
  if (msg) msg.textContent = "";
  const text = String(ta?.value || "").trim();
  if (!text) {
    if (msg) msg.textContent = "Вставьте список доменов.";
    return;
  }
  try {
    const d = await api("/api/web-leads/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ domains: text }),
    });
    if (msg) msg.textContent = `${d.message || "ОК"} (поставлено: ${d.queued ?? 0})`;
    if (ta) ta.value = "";
    setTimeout(() => loadWebLeadsPage().catch(() => {}), 800);
  } catch (e) {
    if (msg) msg.textContent = e.message || String(e);
  }
}

let _webLeadsActionsBound = false;
function bindWebLeadsActionsOnce() {
  if (_webLeadsActionsBound) return;
  _webLeadsActionsBound = true;
  const sBtn = document.getElementById("webLeadsSearchBtn");
  if (sBtn) sBtn.addEventListener("click", () => submitWebLeadsSearch().catch((e) => alert(e.message || String(e))));
  const iBtn = document.getElementById("webLeadsImportBtn");
  if (iBtn) iBtn.addEventListener("click", () => submitWebLeadsImport().catch((e) => alert(e.message || String(e))));
  const rBtn = document.getElementById("webLeadsRefreshBtn");
  if (rBtn) rBtn.addEventListener("click", () => loadWebLeadsPage().catch((e) => alert(e.message || String(e))));
  const kBtn = document.getElementById("webLeadsSaveKeyBtn");
  if (kBtn) kBtn.addEventListener("click", () => saveWebLeadsSerpKey().catch((e) => alert(e.message || String(e))));
}

async function saveWebLeadsSerpKey() {
  const inp = document.getElementById("webLeadsSerpKey");
  const msg = document.getElementById("webLeadsKeyMsg");
  if (msg) msg.textContent = "";
  const key = String(inp?.value || "").trim();
  try {
    await api("/api/web-leads/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ serpapi_key: key }),
    });
    if (msg) msg.textContent = key ? "Ключ сохранён" : "Ключ удалён";
    if (inp) inp.value = "";
    await loadWebLeadsPage();
  } catch (e) {
    if (msg) msg.textContent = e.message || String(e);
  }
}

function _leadChatKeyForLlm(item) {
  const uid = String(item.user_id || "").trim();
  const un = String(item.username || "").trim();
  const sc = String(item.source_chat || "").trim();
  const uPart = un ? `@${un.replace(/^@/, "")} (id ${uid})` : `id ${uid}`;
  return sc ? `${uPart} · чат ${sc}` : uPart;
}

async function loadChatsPage() {
  const table = document.getElementById("chatsTable");
  if (!table) return;
  const hint = document.getElementById("chatsLlmHint");
  const snippet = document.getElementById("chatsLeadSnippet");
  let overview;
  let conv;
  try {
    [overview, conv] = await Promise.all([api("/api/chats/overview"), api("/api/leads/conversations")]);
  } catch (e) {
    table.innerHTML = "";
    const row = document.createElement("div");
    row.className = "tr wide";
    const eT = document.createElement("div");
    eT.className = "title";
    eT.textContent = "Ошибка";
    const eV = document.createElement("div");
    eV.textContent = String(e?.message || e || "");
    row.appendChild(eT);
    row.appendChild(eV);
    table.appendChild(row);
    if (hint) hint.textContent = "";
    applyChatsLlmPanelRole(window.__leadgenOrgRole);
    return;
  }
  const d = overview;
  if (hint) {
    const needAdmin = window.__leadgenOrgRole !== "admin";
    if (d.llm_ready) {
      hint.textContent =
        "Список лидов из журнала; черновик первого ЛС — по кнопке. При включённом LLM бот может генерировать тексты этапов 2–3 перед отправкой.";
    } else if (needAdmin) {
      hint.textContent =
        "LLM не настроен. Включите на странице «Настройки и лимиты» → «Подключения».";
    } else {
      hint.textContent =
        "Попросите администратора включить LLM в «Настройки и лимиты» → «Подключения».";
    }
  }
  const items = Array.isArray(conv.items) ? conv.items : [];
  table.innerHTML = "";
  if (!items.length) {
    const row = document.createElement("div");
    row.className = "tr wide";
    const t1 = document.createElement("div");
    t1.className = "title";
    t1.textContent = "Пока нет лидов в журнале";
    const t2 = document.createElement("div");
    t2.className = "muted";
    t2.textContent = "Запустите бота и дождитесь записей в sent_leads.csv.";
    row.appendChild(t1);
    row.appendChild(t2);
    table.appendChild(row);
    applyChatsLlmPanelRole(window.__leadgenOrgRole);
    return;
  }
  for (const c of items) {
    const row = document.createElement("div");
    row.className = "tr wide";
    const left = document.createElement("div");
    left.className = "title";
    const un = String(c.username || "").trim();
    const uid = String(c.user_id || "").trim();
    left.textContent = un ? `@${un.replace(/^@/, "")}` : uid ? `user ${uid}` : "—";
    const right = document.createElement("div");
    const meta = document.createElement("div");
    meta.className = "muted";
    const prev = String(c.last_message_preview || "").trim();
    const lt = String(c.last_lead_tag || "").trim();
    const tagLab = lt ? LEAD_TAG_LABELS[lt] || lt : LEAD_TAG_LABELS.lead;
    meta.textContent = `${formatLeadWrittenTime(c.last_timestamp || "")} · ${shortStageLabel(c.last_stage || "")} · ${shortStatusLabel(c.last_status || "")} · ${tagLab} · ${c.source_chat || ""}${prev ? ` — «${prev.slice(0, 120)}${prev.length > 120 ? "…" : ""}»` : ""}`;
    const btnRow = document.createElement("div");
    btnRow.className = "row";
    const tgOpen = document.createElement("button");
    tgOpen.type = "button";
    tgOpen.className = "secondary";
    tgOpen.textContent = "Открыть в Telegram";
    let tgHref = "";
    if (un) {
      tgHref = `https://t.me/${un.replace(/^@/, "")}`;
    } else if (/^\d+$/.test(uid)) {
      tgHref = `tg://user?id=${uid}`;
    }
    tgOpen.disabled = !tgHref;
    tgOpen.title = tgHref || "Нет username или числового user id";
    tgOpen.addEventListener("click", () => {
      if (tgHref) window.open(tgHref, "_blank", "noopener,noreferrer");
    });
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = "Черновик первого ЛС (LLM)";
    btn.className = "secondary";
    btn.disabled = !d.llm_ready;
    const chatKey = _leadChatKeyForLlm(c);
    btn.addEventListener("click", async () => {
      const manual = snippet ? snippet.value.trim() : "";
      const snip = manual || prev || "";
      const out = document.getElementById("chatsLlmOutput");
      if (out) out.textContent = "…";
      try {
        const r = await api("/api/chats/suggest-offer", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ chat_key: chatKey, lead_snippet: snip }),
        });
        if (out) out.textContent = r.text || "(пусто)";
      } catch (e) {
        if (out) out.textContent = e.message || String(e);
      }
    });
    btnRow.appendChild(tgOpen);
    btnRow.appendChild(document.createTextNode(" "));
    btnRow.appendChild(btn);
    right.appendChild(meta);
    right.appendChild(btnRow);
    row.appendChild(left);
    row.appendChild(right);
    table.appendChild(row);
  }
  applyChatsLlmPanelRole(window.__leadgenOrgRole);
}

/** @type {any} */
let _statsChartLeads = null;
/** @type {any} */
let _statsChartUsage = null;

/** Перерисовать графики аналитики после смены светлой/тёмной темы (цвета осей). */
function afterThemeApplied() {
  if (!document.getElementById("statsChartLeadsDaily")) return;
  if (typeof Chart === "undefined") return;
  refreshStats().catch(() => {});
}

async function refreshStatsCharts(summary) {
  if (typeof Chart === "undefined") return;
  const c1 = document.getElementById("statsChartLeadsDaily");
  const c2 = document.getElementById("statsChartUsage");
  if (!c1 && !c2) return;
  const isDark = document.documentElement.getAttribute("data-theme") === "dark";
  const tick = isDark ? "#e5e5e5" : "#333";
  const grid = isDark ? "rgba(255,255,255,0.08)" : "rgba(0,0,0,0.06)";

  let tl = { series: [] };
  try {
    tl = await api("/api/stats/leads-timeline?days=30");
  } catch {
    /* ignore */
  }
  if (c1) {
    const ser = Array.isArray(tl.series) ? tl.series : [];
    const labels = ser.map((s) => (s.date && s.date.length >= 10 ? s.date.slice(5, 10) : s.date));
    const data = ser.map((s) => s.count);
    if (!_statsChartLeads) {
      _statsChartLeads = new Chart(c1, {
        type: "line",
        data: {
          labels,
          datasets: [
            {
              label: "Строк в CSV за день",
              data,
              borderColor: "rgb(59, 130, 246)",
              backgroundColor: "rgba(59, 130, 246, 0.12)",
              fill: true,
              tension: 0.25,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: false,
          plugins: {
            legend: { labels: { color: tick } },
          },
          scales: {
            x: { ticks: { color: tick, maxRotation: 45 }, grid: { color: grid } },
            y: { beginAtZero: true, ticks: { color: tick, precision: 0 }, grid: { color: grid } },
          },
        },
      });
    } else {
      _statsChartLeads.data.labels = labels;
      _statsChartLeads.data.datasets[0].data = data;
      _statsChartLeads.options.plugins.legend.labels.color = tick;
      _statsChartLeads.options.scales.x.ticks.color = tick;
      _statsChartLeads.options.scales.x.grid.color = grid;
      _statsChartLeads.options.scales.y.ticks.color = tick;
      _statsChartLeads.options.scales.y.grid.color = grid;
      _statsChartLeads.update("none");
    }
  }
  if (c2) {
    const p = summary.plan || {};
    const maxC = Number(p.max_chats) || 0;
    const maxD = Number(p.max_dm_day) || 0;
    const maxM = Number(p.max_dm_month) || 0;
    const u1 = Number(summary.target_chats) || 0;
    const u2 = Number(summary.daily_sent_count) || 0;
    const u3 = Number(summary.monthly_sent_count) || 0;
    const barNow = [u1, u2, u3];
    const barMax = [maxC, maxD, maxM];
    if (!_statsChartUsage) {
      _statsChartUsage = new Chart(c2, {
        type: "bar",
        data: {
          labels: ["Чаты", "ЛС/день", "ЛС/мес"],
          datasets: [
            { label: "Сейчас", data: barNow, backgroundColor: "rgba(59, 130, 246, 0.7)" },
            {
              label: "Потолок (тариф)",
              data: barMax,
              backgroundColor: "rgba(148, 163, 184, 0.55)",
            },
          ],
        },
        options: {
          indexAxis: "y",
          responsive: true,
          maintainAspectRatio: false,
          animation: false,
          plugins: { legend: { labels: { color: tick } } },
          scales: {
            x: { beginAtZero: true, ticks: { color: tick }, grid: { color: grid } },
            y: { ticks: { color: tick }, grid: { color: grid } },
          },
        },
      });
    } else {
      _statsChartUsage.data.datasets[0].data = barNow;
      _statsChartUsage.data.datasets[1].data = barMax;
      _statsChartUsage.options.plugins.legend.labels.color = tick;
      _statsChartUsage.options.scales.x.ticks.color = tick;
      _statsChartUsage.options.scales.x.grid.color = grid;
      _statsChartUsage.options.scales.y.ticks.color = tick;
      _statsChartUsage.options.scales.y.grid.color = grid;
      _statsChartUsage.update("none");
    }
  }
}

async function refreshStats() {
  if (!document.getElementById("statsKv")) return;
  const d = await api("/api/stats/summary");
  const cs = d.calls_summary || {};
  const cards = [
    { k: "Чатов в мониторинге", v: d.target_chats },
    { k: "Лидов (CSV строк)", v: d.leads_rows_total },
    { k: "Отправлено сегодня", v: `${d.daily_sent_count} / ${d.daily_limit || "—"}` },
    { k: "Отправлено за месяц", v: d.monthly_sent_count },
    { k: "Созвоны: запланировано", v: cs.scheduled != null ? cs.scheduled : "—" },
    { k: "Созвоны: состоялись", v: cs.done != null ? cs.done : "—" },
    { k: "Созвоны: успех", v: cs.won != null ? cs.won : "—" },
  ];
  setCards(document.getElementById("statsCards"), cards);

  const lim = d.limits || {};
  const dayRange = Array.isArray(lim.daily_limit_range) ? lim.daily_limit_range.join("–") : "—";
  const stRows = [
    { k: "Организация (org_id)", v: d.org_id },
    { k: "Контактов в базе", v: d.contacted_users },
    { k: "В чёрном списке", v: d.blacklist_users },
    { k: "Лимит ЛС/день (по тарифу)", v: dayRange },
    { k: "Лимит ЛС/месяц (по тарифу)", v: lim.max_dm_month ?? "—" },
    { k: "Интервал мониторинга (сек)", v: lim.monitor_interval_sec ?? "—" },
  ];
  if (d.plan && d.plan.max_telegram_accounts != null) {
    stRows.push({ k: "Telegram-аккаунтов (лимит тарифа)", v: d.plan.max_telegram_accounts });
  }
  setKv(document.getElementById("statsKv"), stRows);

  const help = document.getElementById("statsHelp");
  if (help) {
    setKv(help, [
      { k: "Лиды в таблице", v: "Сколько записей о обработанных сообщениях и отправках в личку накопилось у организации." },
      { k: "Отправлено сегодня", v: "Сколько личных сообщений бот отправил с начала текущих суток (счётчик обнуляется каждый день)." },
      { k: "Лимит сообщений в день", v: "По тарифу: максимум личных сообщений за сутки. В настройках может быть своё число, но тариф не даст превысить план." },
      { k: "Лимит за месяц", v: "Потолок личных сообщений за календарный месяц; при достижении бот перестаёт отправлять до следующего месяца." },
      { k: "Интервал мониторинга", v: "Как часто бот проверяет чаты. Меньше секунд — чаще проверка, но вышериск ограничений со стороны Telegram." },
    ]);
  }
  await refreshStatsCharts(d);
}

async function refreshBillingPaymentsOnly() {
  const payBody = document.getElementById("billingPaymentsTbody");
  const payEmpty = document.getElementById("billingPaymentsEmpty");
  if (!payBody || !hasAuthToken()) return;
  let items = [];
  try {
    const pr = await api("/api/billing/payments");
    items = Array.isArray(pr.items) ? pr.items : [];
  } catch {
    return;
  }
  if (payEmpty) payEmpty.style.display = items.length ? "none" : "block";
  payBody.innerHTML = "";
  for (const it of items) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${escapeHtmlCell(String(it.id ?? ""))}</td><td>${escapeHtmlCell(String(it.plan_id || ""))}</td><td>${escapeHtmlCell(String(it.amount_rub_gross ?? ""))}</td><td>${escapeHtmlCell(String(it.status || ""))}</td><td>${escapeHtmlCell(String(it.created_at || ""))}</td><td>${escapeHtmlCell(String(it.paid_at || "—"))}</td>`;
    payBody.appendChild(tr);
  }
}

async function loadBilling() {
  if (!document.getElementById("planCards")) return;
  const me = await api("/api/auth/me");
  if (!me.authenticated) {
    window.location.href = "/auth";
    return;
  }
  // billing: show current plan + plan catalog from DB
  const stats = await api("/api/stats/summary");
  const plansResp = await api("/api/plans");
  const plans = Array.isArray(plansResp.plans) ? plansResp.plans : [];
  const cards = [
    { k: "Текущий тариф", v: me.plan_id },
    { k: "Статус подписки", v: me.subscription_status },
    { k: "Лимит ЛС/день", v: (stats.limits && stats.limits.daily_limit_range) ? String(stats.limits.daily_limit_range) : "—" },
    { k: "Лимит ЛС/месяц", v: (stats.limits && stats.limits.max_dm_month) ? stats.limits.max_dm_month : "—" },
  ];
  if (stats.plan && stats.plan.max_telegram_accounts != null) {
    cards.push({ k: "Telegram-аккаунтов (по тарифу)", v: stats.plan.max_telegram_accounts });
  }
  setCards(document.getElementById("planCards"), cards);
  const dayRange = (stats.limits && Array.isArray(stats.limits.daily_limit_range)) ? stats.limits.daily_limit_range.join("–") : "—";
  setKv(document.getElementById("billingKv"), [
    { k: "Что ограничивает тариф", v: "чаты, ЛС/день, ЛС/месяц, интервал мониторинга, число Telegram-аккаунтов" },
    { k: "Ваш ЛС/день", v: dayRange },
    { k: "Ваш ЛС/месяц", v: (stats.limits && stats.limits.max_dm_month) ? stats.limits.max_dm_month : "—" },
    { k: "Интервал мониторинга (сек)", v: (stats.limits && stats.limits.monitor_interval_sec) ? `${stats.limits.monitor_interval_sec}` : "—" },
  ]);

  const pt = document.getElementById("plansTable");
  if (pt) {
    pt.innerHTML = "";
    for (const p of plans) {
      const row = document.createElement("div");
      row.className = "tr wide";
      const price = Number(p.price_rub_month || 0);
      const pid = String(p.id || "");
      const mtg = p.max_telegram_accounts != null ? ` • TG до ${p.max_telegram_accounts}` : "";
      const left = document.createElement("div");
      left.innerHTML = `<div class="title">${escapeHtmlCell(String(p.title))} • ${price} ₽/мес</div><div class="muted">Чаты: до ${p.max_chats} • ЛС/день: ${p.max_dm_day} • ЛС/мес: ${p.max_dm_month} • мониторинг ≥ ${p.monitor_interval_min_sec} с${mtg}</div>`;
      const actions = document.createElement("div");
      actions.className = "row";
      actions.style.marginTop = "8px";
      if (price > 0 && pid) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.textContent = "Оплатить (ЮKassa)";
        btn.addEventListener("click", () => {
          (async () => {
            try {
              const d = await api("/api/billing/checkout", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ plan_id: pid }),
              });
              if (d.checkout_url) {
                window.location.href = d.checkout_url;
              } else {
                alert(d.message || "Нет ссылки оплаты — проверьте настройки сервера.");
              }
            } catch (e) {
              alert(e.message || String(e));
            }
          })();
        });
        actions.appendChild(btn);
      } else {
        const sp = document.createElement("span");
        sp.className = "muted";
        sp.textContent = price <= 0 ? "Без оплаты" : "";
        actions.appendChild(sp);
      }
      row.appendChild(left);
      row.appendChild(actions);
      pt.appendChild(row);
    }
  }

  await refreshBillingPaymentsOnly();

  const qs = new URLSearchParams(window.location.search || "");
  const paidReturn = qs.get("paid") === "1";
  if (paidReturn) {
    try {
      window.history.replaceState({}, "", window.location.pathname);
    } catch {
      /* ignore */
    }
  }
  const hint = document.getElementById("billingHint");
  if (hint) {
    hint.textContent = paidReturn
      ? "Вы вернулись с оплаты. Если статус «paid» не появился через минуту, обновите страницу (ожидается уведомление ЮKassa на /api/billing/webhook/yookassa)."
      : "Оплата через ЮKassa: для клиента без отдельной комиссии (комиссия провайдера — с мерчанта). На сервере: YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY, PUBLIC_APP_URL.";
  }
}

// ─── Локальное сохранение UI-состояния ─────────────────────────────────────
// Любой input/select/textarea/details с атрибутом data-persist="ключ"
// автоматически:
//   • при загрузке страницы — восстанавливает значение из localStorage;
//   • при изменении — сохраняет его обратно.
// Это переживает перезапуск сайта и логаут. НЕ используется для серверных
// настроек config.json — они грузятся/сохраняются через API.
const __PERSIST_PREFIX = "leadgen_ui_v1__";

function _persistKey(rawKey) {
  return __PERSIST_PREFIX + String(rawKey || "").trim();
}

function _persistReadRaw(rawKey) {
  try {
    const v = localStorage.getItem(_persistKey(rawKey));
    return v == null ? null : v;
  } catch {
    return null;
  }
}

function _persistWriteRaw(rawKey, value) {
  try {
    if (value == null) localStorage.removeItem(_persistKey(rawKey));
    else localStorage.setItem(_persistKey(rawKey), String(value));
  } catch {
    /* localStorage недоступен / переполнен — молча игнорируем */
  }
}

function _persistApplyValue(el) {
  const key = el.getAttribute("data-persist");
  if (!key) return;
  const stored = _persistReadRaw(key);
  if (stored == null) return;
  const tag = (el.tagName || "").toLowerCase();
  const type = (el.getAttribute("type") || "").toLowerCase();
  if (tag === "details") {
    el.open = stored === "1" || stored === "true";
    return;
  }
  if (type === "checkbox") {
    el.checked = stored === "1" || stored === "true";
    return;
  }
  if (type === "radio") {
    // для radio храним ВЫБРАННОЕ значение по data-persist, общему для группы
    el.checked = String(el.value) === stored;
    return;
  }
  el.value = stored;
}

function _persistCaptureValue(el) {
  const key = el.getAttribute("data-persist");
  if (!key) return;
  const tag = (el.tagName || "").toLowerCase();
  const type = (el.getAttribute("type") || "").toLowerCase();
  if (tag === "details") {
    _persistWriteRaw(key, el.open ? "1" : "0");
    return;
  }
  if (type === "checkbox") {
    _persistWriteRaw(key, el.checked ? "1" : "0");
    return;
  }
  if (type === "radio") {
    if (el.checked) _persistWriteRaw(key, el.value);
    return;
  }
  _persistWriteRaw(key, el.value == null ? "" : String(el.value));
}

function initPersistedInputs(root) {
  const scope = root || document;
  const nodes = scope.querySelectorAll("[data-persist]");
  nodes.forEach((el) => {
    if (el.dataset.persistBound === "1") return;
    el.dataset.persistBound = "1";
    _persistApplyValue(el);
    const tag = (el.tagName || "").toLowerCase();
    if (tag === "details") {
      el.addEventListener("toggle", () => _persistCaptureValue(el));
    } else {
      el.addEventListener("change", () => _persistCaptureValue(el));
      if (tag === "input" || tag === "textarea") {
        el.addEventListener("input", () => _persistCaptureValue(el));
      }
    }
  });
}

async function bootstrap() {
  bindGlobalUiDebugHooks();
  await ensureCsrf().catch(() => null);
  bindPasswordToggles();
  initPersistedInputs();
  bindActions();
  await refreshHealth();
  await refreshAuthState();
  await migrateLegacyBotJunkToServerOnce().catch(() => {});
  await refreshHomeDashboard();

  // Page-specific initial loads
  await loadConfigPage();
  await refreshBotProgress();
  await refreshBotInfo();
  await refreshStats();
  await loadBilling();
  await loadChatsPage();
  await loadLeadsPage();
  if (document.getElementById("offersTbody")) {
    bindOffersTableActionsOnce();
    await loadOffersPage().catch(() => {});
  }
  if (document.getElementById("callsTbody")) {
    await loadCallsPage().catch(() => {});
  }
  if (document.getElementById("convTbody")) {
    await loadConversationsPage().catch(() => {});
  }
  if (document.getElementById("webLeadsTbody")) {
    bindWebLeadsActionsOnce();
    await loadWebLeadsPage().catch(() => {});
  }

  // Все таймеры — через makePoller: пропускаем тики при скрытой вкладке и не наслаиваем запросы.
  setInterval(
    makePoller(async () => {
      if (hasAuthToken()) await refreshSidebarAccountExtras();
    }),
    60000,
  );
  setInterval(makePoller(refreshPendingOutreachBadge), 30000);

  // Only keep polling if widgets exist on the current page
  if (document.getElementById("botStatus")) {
    refreshBotProgress().catch(() => {});
    refreshBotScanLog().catch(() => {});
    refreshBotLeads().catch(() => {});
    refreshBotMiniStats().catch(() => {});
    // Базовая инфо: лимиты/окно — меняются редко.
    setInterval(makePoller(refreshBotInfo), 30000);
    // Прогресс — главная индикация. 2.5 с достаточно глазу, но без перегрузки SQLite.
    setInterval(makePoller(refreshBotProgress), 2500);
    // Лог уже пишет фронтом ту же scan_progress; обновляем редко, дальше доверяем прогрессу.
    setInterval(makePoller(refreshBotScanLog), 4000);
    setInterval(makePoller(refreshBotLeads), 8000);
    // Дневные/месячные счётчики — отдельным редким таймером, не на каждом тике прогресса.
    setInterval(makePoller(refreshBotMiniStats), 15000);
  }
  if (document.getElementById("statsKv")) {
    await refreshStats().catch(() => {});
    setInterval(makePoller(refreshStats), 30000);
  }
  if (document.getElementById("chatsTable")) {
    setInterval(makePoller(loadChatsPage), 30000);
  }
  if (document.getElementById("leadsTbody")) {
    setInterval(makePoller(loadLeadsPage), 20000);
  }
  if (document.getElementById("offersTbody")) {
    setInterval(makePoller(loadOffersPage), 20000);
  }
  if (document.getElementById("callsTbody")) {
    setInterval(makePoller(loadCallsPage), 30000);
  }
  if (document.getElementById("convTbody")) {
    setInterval(makePoller(loadConversationsPage), 30000);
  }
  if (document.getElementById("webLeadsTbody")) {
    setInterval(makePoller(loadWebLeadsPage), 8000);
  }
  if (document.querySelector("[data-home-card]")) {
    setInterval(makePoller(refreshHomeDashboard), 90000);
  }
  if (document.getElementById("billingPaymentsTbody")) {
    setInterval(makePoller(refreshBillingPaymentsOnly), 45000);
  }
  bindUiDebugPanel();
  uiDebugLogPageEntry();
  restoreVolatileForCurrentPage();
  if (document.getElementById("foldersTargetChats")) {
    await loadFoldersTargetChats().catch(() => {});
    await initFoldersTgAccountSelect().catch(() => {});
  }
  if (document.getElementById("monitorChatsExcludeWrap")) {
    await refreshMonitorChatsExcludePanel().catch(() => {});
  }
  if (document.getElementById("searchKeywordsText") && canUseChannelSearch()) {
    await refreshSearchPageHint().catch(() => {});
    await initSearchTgAccountsUi().catch(() => {});
  }
  if (document.getElementById("botTgAccountsBlock")) {
    await initBotTgAccountsUi().catch(() => {});
  }
  if (document.getElementById("dialogsCompareTable")) {
    await initDialogsCompareAccountsUi().catch(() => {});
  }
  if (document.body && document.body.dataset && document.body.dataset.page === "security") {
    await loadSecurityPage().catch(alert);
  }
}

bootstrap().catch((e) => alert(e.message || e));
