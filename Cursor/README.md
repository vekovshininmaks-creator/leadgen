# Leadgen (server-first)

Веб-консоль, backend API и база пользователей; мониторинг Telegram — отдельным процессом/подпроцессом.

## Документация

| Документ | Содержание |
|----------|------------|
| **[docs/запуск.md](docs/запуск.md)** | Локальный запуск, production, проверки, FAQ |
| **[docs/деплой-сервер.md](docs/деплой-сервер.md)** | VPS: systemd, nginx, TLS, переменные окружения |
| **[docs/runbook-deploy-by-ip.md](docs/runbook-deploy-by-ip.md)** | Быстрый деплой по HTTP и IP |
| **[docs/LLM-настройка.md](docs/LLM-настройка.md)** | LLM в организации |
| **[docs/api.md](docs/api.md)** | Список HTTP API |
| **[docs/гайд-для-тестеров.md](docs/гайд-для-тестеров.md)** | Гайд для QA: экраны, сценарии, багрепорты |

## LLM

Настраивает админ организации: **«Контакты» → «Настройки и лимиты» → «Подключения (Telegram + LLM)»**. Подробнее — `docs/LLM-настройка.md`.

## Организация и роли в консоли

- Платформенная роль **`manager`** (таблица `users`) задаётся в Admin отдельно от роли **в организации** (`memberships`).
- **Admin → Пользователи**: блок **«Организация и роль»** — выбор org и роли в org (**client** / **manager** / **tester** / **admin** организации), сохранение через **`POST /api/admin/membership/set`** (одна связка пользователь ↔ org).
- Для пользователя с ролью **`manager`** в `users` по-прежнему есть узкий сценарий **`POST /api/admin/manager/assign-org`** (ставит membership **manager** в выбранной org).
- Роль **tester** в org — доступ к воронке/поиску/outreach как у широких ролей, но без секретов в конфиге (см. **`docs/гайд-для-тестеров.md`**).

## Несколько Telegram-аккаунтов на организацию

- В `tenants/org_N/config.json`: `telegram_accounts` и `active_telegram_account`.
- Лимит тарифа: `plans.max_telegram_accounts` (верхняя граница на org — 10).
- Сессии: каталог `sessions/` (`leadgen_org_<N>_session`, `leadgen_org_<N>_acc_<id>`).
- В UI: **Настройки → Подключения**; на поиске каналов при 2+ аккаунтах — выбор аккаунтов. См. также `docs/api.md` (search, folders, sync-dialogs).

## Структура репозитория

- `web_app.py` — Flask backend
- `wsgi.py` — точка входа gunicorn
- `telegram_leadgen_bot.py` — движок бота
- `config.json` — шаблон; рабочий конфиг организации в `tenants/`
- `templates/`, `static/` — UI

## Запуск за одну команду

Локально (после `pip install -r requirements.txt`):

```bash
python web_app.py
```

Или: `npm.cmd run dev` (Windows), `npm run dev` — см. `package.json`.

Порт и URL по умолчанию — в **`docs/запуск.md`**.
