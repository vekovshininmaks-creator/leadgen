# HTTP API (краткий справочник)

Защищённые операции требуют сессии (cookie после входа) и при необходимости заголовка **`X-CSRF-Token`** для мутаций. Альтернатива для автоматизации:

`X-Auth-Token: <token>`

(токен возвращается при логине).

---

## Auth и профиль

- `POST /api/auth/register` — регистрация
- `POST /api/auth/login` — вход
- `GET /api/auth/me` — текущий пользователь
- `GET /api/auth/csrf` — CSRF-токен
- `GET /api/me/export` — экспорт данных профиля (JSON)
- `POST /api/me/delete` — удаление аккаунта (пароль)

## Конфиг организации

- `GET /api/config`, `POST /api/config`

## LLM

- `GET /api/llm/presets` — публичные пресеты Base URL / моделей

## Бот и мониторинг

- `POST /api/bot/start`, `POST /api/bot/stop`, `GET /api/bot/status`

## Поиск каналов и папки Telegram

- `POST /api/search/generate-keywords` — фразы для поиска через LLM
- `POST /api/search/channels`, `POST /api/search/add-to-monitoring` (организационные роли с доступом к поиску: admin / manager / client / **tester**; поиск обычно при остановленном боте)
- `POST /api/search/create-folder` — папка в Telegram из выбранных чатов
- `GET /api/folders/list`, `POST /api/folders/import`

## Чаты и синхронизация

- `POST /api/chats/sync-dialogs` — диалоги → `target_chats`. Тело: `mode` merge|replace, `limit`, `telegram_account_id` или несколько `account_ids`

## Лиды и аналитика

- `GET /api/leads/export` — CSV лидов организации
- `GET /api/stats/leads-timeline?days=30` — ряд по `sent_leads.csv`

## Админка

- `GET /api/admin/orgs`
- `POST /api/admin/manager/assign-org` — назначить менеджеру клиента организацию (только global admin; тело `{"user_id": N, "org_id": M}`)
- `POST /api/admin/membership/set` — задать **единственную** связку пользователь ↔ организация ↔ **роль в org** (только global admin; CSRF; тело `{"user_id": N, "org_id": M, "role": "admin"|"manager"|"client"|"tester"}`). Перед вставкой удаляются все строки `memberships` для этого `user_id`.
- `GET /api/admin/users` — помимо `org_id` для пользователя возвращает **`org_membership_role`** — роль в выбранной организации (тот же подзапрос ORDER BY, что и для `org_id`).
- `PATCH /api/admin/plans/<plan_id>` — тарифы в БД (global admin; CSRF)

## Прочее

- `POST /api/assistant/ask` — помощник по интерфейсу (LLM org). Тело: `question`, опционально `page`
- `GET /api/logs`
- `GET /healthz` — проверка живости и версии сервера

Подробности ролей и ограничений см. в коде `web_app.py` и в [`README.md`](../README.md).
