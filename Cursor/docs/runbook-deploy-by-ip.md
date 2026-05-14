# Runbook: Leadgen на VPS по IP (максимально подробно)

Этот документ написан так, чтобы по нему мог пройти человек, который **почти не работал за компьютером**. Ничего не нужно «понимать заранее»: читайте сверху вниз, делайте по шагам.

**Что получится в конце:** у вас есть «сервер в интернете» с адресом вида `203.0.113.10`, и в браузере открывается панель Leadgen по адресу `http://203.0.113.10/auth` (без домена и без HTTPS на первом этапе).

Общая документация по запуску: [`запуск.md`](запуск.md). Полный деплой с доменом и TLS: [`деплой-сервер.md`](деплой-сервер.md). Кратко про выкладку кода с ПК на VPS и бесплатное имя под сертификат: [`синхронизация-и-домен.md`](синхронизация-и-домен.md).

---

## Часть A. Словарь — что вообще происходит

Прочитайте один раз; потом можно возвращаться к отдельным словам.

| Слово | Простыми словами |
|--------|-------------------|
| **Компьютер (ПК)** | То, на чём вы сейчас сидите: экран, клавиатура, мышь. |
| **Браузер** | Программа для сайтов: Chrome, Edge, Firefox, Safari. Адресная строка — сверху, куда вводят `http://...`. |
| **Интернет** | Связь вашего ПК с другими машинами по сети. |
| **VPS** | «Компьютер в облаке»: у него есть процессор, память, диск, но физически он стоит в датацентре провайдера. Вы им управляете удалённо. |
| **IP-адрес** | Номер сервера в интернете, например `203.0.113.10`. Как «номер дома», только для машины. |
| **SSH** | Безопасный способ **удалённо управлять** сервером: вы вводите команды текстом, сервер их выполняет. |
| **Терминал / консоль / командная строка** | Окно с чёрным или белым фоном, куда вводят **команды** и нажимают Enter. На Windows это часто **PowerShell** или **Терминал Windows**. |
| **Команда** | Строка текста (на английском), которую вы вставили в терминал и нажали **Enter**. |
| **Копировать / вставить** | Выделить текст мышью → **Ctrl+C** (копировать). В терминал вставить: **Ctrl+Shift+V** или правый клик «Вставить» (в PowerShell часто работает и **Ctrl+V**). |
| **root** | Суперпользователь на Linux: может всё. На новом VPS часто дают вход именно как `root`. |
| **Ubuntu** | Популярная операционная система **на сервере** (не Windows). Команды ниже рассчитаны на **Ubuntu 22.04 или 24.04**. |
| **sudo** | «Выполни эту команду от имени администратора». Часто спросит пароль — вводите **слепо** (звёздочки не показывают) и снова **Enter**. |
| **nginx** | Программа на сервере: принимает запросы из интернета на порт **80** и передаёт их во внутреннее приложение. |
| **gunicorn** | Программа, которая **запускает** ваше Python-приложение (веб-панель) и держит его работающим. |
| **systemd** | Система Linux, которая **автоматически поднимает** сервис после перезагрузки сервера и перезапускает при сбое. |
| **Репозиторий / git clone** | «Папка с кодом» в системе контроля версий Git. `git clone` — **скачать** эту папку на сервер. |
| **Секретный ключ** | Длинная случайная строка. Её **никому не показывают** и не кидают в чаты — только в конфиг на сервере. |
| **HTTP / HTTPS** | HTTP — сайт без замка в браузере (по IP обычно так). HTTPS — с шифрованием и доменом. На первом этапе у вас **только HTTP** — это нормально для старта по IP. |

---

## Часть B. Что должно быть у вас до начала

1. **Письмо от хостинга** (или панель провайдера), где написано:
   - **IP-адрес** сервера (например `203.0.113.10`).
   - **Логин** (часто `root`).
   - **Пароль** или инструкция, как скачать **SSH-ключ** (файл `.pem`).
2. **Порт 22** (SSH) и **порт 80** (сайт) **открыты**:
   - в панели провайдера (фаервол / Security groups),
   - и внутри Ubuntu (настроим `ufw` ниже).
3. **Ссылка на репозиторий** с кодом Leadgen, например:
   - `https://github.com/ВАШ_АККАУНТ/leadgen.git`  
   Если репозиторий **приватный**, вам нужен способ клонирования (логин/токен) — это отдельная тема; попросите того, кто ведёт проект, выдать **Personal Access Token** или добавить ваш SSH-ключ в репозиторий.

Если чего-то нет — **не продолжайте**: напишите в поддержку хостинга «нужен доступ по SSH и открыты порты 22 и 80».

---

## Часть C. Как «зайти» на сервер с вашего ПК

Нужно открыть **терминал** и подключиться по **SSH**.

### C1. Windows 10 / 11

1. Нажмите клавиши **Win + S** (поиск).
2. Введите слово: **PowerShell** или **Терминал** (Windows Terminal).
3. Откройте программу **Windows PowerShell** или **Терминал**.
4. Вставьте команду (подставьте **свой IP** вместо примера):

```text
ssh root@203.0.113.10
```

5. Нажмите **Enter**.
6. Первый раз спросит про «fingerprint» — введите **`yes`** и **Enter**.
7. Введите **пароль** от сервера (символы не отображаются — это нормально) и **Enter**.

Если пишет «connection refused» или «timed out» — порт 22 закрыт или IP неверный. Исправляйте в панели хостинга.

**Если у вас ключ `.pem`**, а не пароль (часто у AWS):

```powershell
ssh -i "C:\Users\ВАШ_ПОЛЬЗОВАТЕЛЬ\Downloads\my-key.pem" root@203.0.113.10
```

На Windows иногда нужно ограничить права на файл ключа (иначе SSH откажется). В PowerShell **от имени пользователя**:

```powershell
icacls "C:\Users\ВАШ_ПОЛЬЗОВАТЕЛЬ\Downloads\my-key.pem" /inheritance:r
icacls "C:\Users\ВАШ_ПОЛЬЗОВАТЕЛЬ\Downloads\my-key.pem" /grant:r "$env:USERNAME:(R)"
```

### C2. macOS

1. Откройте **Программы → Утилиты → Терминал**.
2. Команда та же: `ssh root@ВАШ_IP`.

### C3. Альтернатива: PuTTY (Windows, если ssh «не завёлся»)

1. Скачайте PuTTY с официального сайта проекта PuTTY.
2. В поле **Host Name** введите IP.
3. Порт **22**, тип **SSH**.
4. **Open** → логин `root` → пароль.

Дальше в PuTTY вы будете вставлять те же команды, что и в обычном SSH.

---

## Часть D. Вы на сервере: как понять, что всё ок

После успешного входа вы увидите приглашение вроде:

```text
root@ubuntu:~#
```

или

```text
root@something:~$
```

Это значит: **сервер слушает вас**. Все команды из следующих частей вводите **по одной блоку** (можно целиком копировать), в конце — **Enter**.

**Важно:** если в команде есть `sudo`, система может спросить пароль — снова вводите пароль root (или пароль пользователя с правами sudo) и Enter.

---

## Часть E. Шаг 1 — обновить список программ и поставить нужное

Скопируйте **целиком** и выполните:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git nginx ufw
```

**Что это делает:**

- `apt update` — обновляет каталог программ в интернете.
- `apt install` — ставит:
  - **python3** — язык, на котором написана панель;
  - **venv** — изолированное окружение для библиотек;
  - **pip** — установщик библиотек Python;
  - **git** — чтобы скачать код;
  - **nginx** — «витрина» сайта наружу;
  - **ufw** — простой фаервол.

Проверка версии Python (должно быть **3.11 или новее**):

```bash
python3 --version
```

Если покажет 3.10 или ниже — для этого проекта лучше поставить Python 3.11+ (см. [`деплой-сервер.md`](деплой-сервер.md) про `deadsnakes`). На Ubuntu 22.04/24.04 обычно уже подходит.

---

## Часть F. Шаг 2 — пользователь `leadgen` и папки

Приложение не должно работать от root «на постоянке». Создаём системного пользователя и каталоги.

Используем **идемпотентный** вариант: команда `id -u leadgen` проверяет, есть ли уже такой пользователь, и создаёт его только если нет.

```bash
id -u leadgen >/dev/null 2>&1 || sudo useradd -r -s /usr/sbin/nologin leadgen
sudo mkdir -p /opt/leadgen /var/lib/leadgen /var/backups/leadgen
sudo chown -R leadgen:leadgen /opt/leadgen /var/lib/leadgen /var/backups/leadgen
```

> **«Я нажал Enter и ничего не произошло»** — в Linux это нормально: команды `mkdir`, `chown`, `useradd` при успехе **ничего не печатают**. Появилась пустая строка с приглашением `root@...#` — значит всё ок. Проверить себя:
>
> ```bash
> ls -la /opt/leadgen /var/lib/leadgen /var/backups/leadgen
> id leadgen
> ```
>
> Должны увидеть три папки с владельцем `leadgen leadgen` и строку про пользователя `leadgen`. Только после этого идите дальше.
>
> Если **приглашение `PS C:\...>`** — вы всё ещё в PowerShell на своём ПК, а не на сервере. Сначала зайдите по SSH (Часть C), и только потом вводите эти команды.

> Если вы пробовали деплой раньше и видите **`useradd: user 'leadgen' already exists`** — это не ошибка, а информация: пользователь уже создан. Просто переходите к команде `mkdir`/`chown` ниже. Проверить, что юзер действительно существует, можно так:
>
> ```bash
> id leadgen
> ```
>
> Должны увидеть строку вида `uid=998(leadgen) gid=998(leadgen) groups=998(leadgen)` — значит всё нормально.

**Что это значит:**

- `/opt/leadgen` — здесь будет код и виртуальное окружение.
- `/var/lib/leadgen` — здесь лежит **база данных** `data.db` (важные данные).
- `/var/backups/leadgen` — сюда положим бэкапы.

---

## Часть G. Шаг 3 — скачать код (git clone)

**Подставьте свой URL репозитория** вместо `<URL_РЕПОЗИТОРИЯ>`.

```bash
sudo -u leadgen git clone <URL_РЕПОЗИТОРИЯ> /opt/leadgen/app
```

Пример с GitHub (публичный репо):

```bash
sudo -u leadgen git clone https://github.com/ВАШ_АККАУНТ/ВАШ_РЕПО.git /opt/leadgen/app
```

**Важно:** адрес `https://github.com/...` — это **не команда**. Его нужно вставить **внутрь** строки выше, сразу после слов `git clone`, в одну строку. Если в терминале вы ввели только URL, Bash выдаст что-то вроде:

```text
-bash: line N: https://github.com/...: No such file or directory
```

Правильно (одна строка целиком, скопируйте и подставьте свой URL при необходимости):

```bash
sudo -u leadgen git clone https://github.com/vekovshininmaks-creator/leadgen.git /opt/leadgen/app
```

Если папка `/opt/leadgen/app` **уже существует** и не пустая, `git clone` напишет:

```text
fatal: destination path '/opt/leadgen/app' already exists and is not an empty directory.
```

**Перед удалением** обязательно посмотрите, что внутри:

```bash
ls -la /opt/leadgen/app
```

- Если видите только пустые подпапки (`logs`, `tenants`, `sessions`, `data`) — это **остатки от Части I** runbook, которые кто-то создал слишком рано. Безопасно удалить и склонировать заново:

  ```bash
  sudo rm -rf /opt/leadgen/app
  sudo -u leadgen git clone https://github.com/vekovshininmaks-creator/leadgen.git /opt/leadgen/app
  sudo mkdir -p /opt/leadgen/app/logs /opt/leadgen/app/tenants /opt/leadgen/app/sessions /opt/leadgen/app/data
  sudo chown -R leadgen:leadgen /opt/leadgen/app/logs /opt/leadgen/app/tenants /opt/leadgen/app/sessions /opt/leadgen/app/data
  ```

- Если видите файлы вроде `web_app.py`, `requirements.txt`, `templates/`, `static/` — **код уже клонирован** в одной из прошлых попыток. Клонировать заново не нужно; обновитесь:

  ```bash
  sudo -u leadgen bash -lc 'cd /opt/leadgen/app && git pull'
  ```

- Если внутри лежат пользовательские данные (`tenants/org_*` с реальными org, `data.db`, файлы сессий `.session`) — **не удаляйте слепо**. Склонируйте во временный путь и перенесите код руками или согласуйте с разработчиком.

Если Git просит логин/пароль на приватный репозиторий — используйте токен, который выдал разработчик (или настройте SSH-ключ на сервере; это можно сделать позже с помощью сисадмина).

Проверка, что папка появилась:

```bash
ls -la /opt/leadgen/app
```

Должны увидеть файлы вроде `web_app.py`, `requirements.txt`.

---

## Часть H. Шаг 4 — виртуальное окружение Python и библиотеки

```bash
sudo -u leadgen python3 -m venv /opt/leadgen/venv
sudo -u leadgen /opt/leadgen/venv/bin/pip install -U pip
sudo -u leadgen /opt/leadgen/venv/bin/pip install -r /opt/leadgen/app/requirements.txt
```

**Сколько ждать:** от одной до нескольких минут, зависит от сервера и интернета.

Если в конце есть слово **ERROR** и установка оборвалась — скопируйте последние 30 строк вывода и покажите тому, кто помогает с деплоем.

---

## Часть I. Шаг 5 — папки для логов, арендаторов, сессий и data

Без этого шага сервис иногда **не стартует** (ошибки namespace / 502 в nginx).

```bash
sudo mkdir -p /opt/leadgen/app/logs /opt/leadgen/app/tenants /opt/leadgen/app/sessions /opt/leadgen/app/data
sudo chown -R leadgen:leadgen /opt/leadgen/app/logs /opt/leadgen/app/tenants /opt/leadgen/app/sessions /opt/leadgen/app/data
```

---

## Часть J. Шаг 6 — секрет для сессий (FLASK_SECRET_KEY)

Выполните:

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

**На экран выведется одна длинная строка** из букв и цифр.

1. Выделите мышью эту строку.
2. Скопируйте (**Ctrl+Shift+C** в некоторых терминалах или выделение + правый клик «Копировать»).
3. Вставьте во **временный** блокнот у себя на ПК (или оставьте в буфере) — она понадобится в следующем шаге.

**Не публикуйте** этот ключ в мессенджерах и скриншотах.

---

## Часть K. Шаг 7 — systemd: чтобы панель всегда была запущена

### K1. Открыть редактор файла сервиса

```bash
sudo nano /etc/systemd/system/leadgen-web.service
```

**Как пользоваться nano (минимум):**

- Вставили текст — он появляется в окне.
- Сохранить: **Ctrl+O**, затем **Enter**.
- Выйти: **Ctrl+X**.

### K2. Вставьте содержимое (ОБЯЗАТЕЛЬНО замените два места)

1. Вместо `ВСТАВЬТЕ_ИЗ_ШАГА_J` — вставьте **строку-секрет** из части J **одной строкой**, без пробелов в начале/конце.
2. Вместо `ВАШ_IP` — вставьте **ваш публичный IP** (тот же, что в SSH), например `203.0.113.10`.

```ini
[Unit]
Description=Leadgen web console
After=network.target

[Service]
Type=simple
User=leadgen
Group=leadgen
WorkingDirectory=/opt/leadgen/app
Environment=FLASK_SECRET_KEY=ВСТАВЬТЕ_ИЗ_ШАГА_J
Environment=DB_PATH=/var/lib/leadgen/data.db
Environment=PUBLIC_APP_URL=http://ВАШ_IP
Environment=LEADGEN_TRUSTED_PROXY_COUNT=1
ExecStart=/opt/leadgen/venv/bin/gunicorn -w 2 -b 127.0.0.1:8000 web_app:app
Restart=always
RestartSec=3
NoNewPrivileges=yes
ProtectSystem=strict
ReadWritePaths=/var/lib/leadgen /opt/leadgen/app/tenants /opt/leadgen/app/sessions /opt/leadgen/app/data /opt/leadgen/app/logs
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
```

**Почему так:**

- `PUBLIC_APP_URL=http://ВАШ_IP` — панель открывается по **http** и **IP** (без домена).
- `LEADGEN_TRUSTED_PROXY_COUNT=1` — потому что перед приложением стоит **nginx** и подставляет заголовки `X-Forwarded-*`.

### K2.1. Альтернатива без редактора (если nano не открывается)

Если `nano` выдаёт `ncurses: cannot initialize terminal type ...` (типично для встроенной WinSCP-консоли), запишите файл целым блоком. Сначала **отредактируйте** на своём ПК две строки внутри блока — подставьте свой секрет вместо `ВСТАВЬТЕ_ИЗ_ШАГА_J` и свой IP вместо `ВАШ_IP`. Затем вставьте блок целиком в SSH:

```bash
sudo tee /etc/systemd/system/leadgen-web.service >/dev/null <<'EOF'
[Unit]
Description=Leadgen web console
After=network.target

[Service]
Type=simple
User=leadgen
Group=leadgen
WorkingDirectory=/opt/leadgen/app
Environment=FLASK_SECRET_KEY=ВСТАВЬТЕ_ИЗ_ШАГА_J
Environment=DB_PATH=/var/lib/leadgen/data.db
Environment=PUBLIC_APP_URL=http://ВАШ_IP
Environment=LEADGEN_TRUSTED_PROXY_COUNT=1
ExecStart=/opt/leadgen/venv/bin/gunicorn -w 2 -b 127.0.0.1:8000 web_app:app
Restart=always
RestartSec=3
NoNewPrivileges=yes
ProtectSystem=strict
ReadWritePaths=/var/lib/leadgen /opt/leadgen/app/tenants /opt/leadgen/app/sessions /opt/leadgen/app/data /opt/leadgen/app/logs
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
EOF
```

Проверка:

```bash
sudo cat /etc/systemd/system/leadgen-web.service
```

Если в файле остались **буквальные** `ВСТАВЬТЕ_ИЗ_ШАГА_J` или `ВАШ_IP` — сервис **не запустится корректно**: вернитесь и подмените их.

### K3. Критически важно для HTTP по IP

**Не добавляйте** в этот файл строки:

```ini
Environment=ENV=prod
Environment=HTTPS=1
```

Пока у вас нет нормального **HTTPS**, эти переменные заставят cookie быть «только для HTTPS», и **вход в панель сломается** в браузере.

### K4. Применить и запустить

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now leadgen-web
sudo systemctl status leadgen-web --no-pager
```

**Успех:** внизу зелёная строка или текст **active (running)**.

**Не успех:** статус **failed** или **activating**. Тогда:

```bash
sudo journalctl -u leadgen-web -n 80 --no-pager
```

Частая причина — забыли часть I (папки). Создайте папки, затем:

```bash
sudo systemctl restart leadgen-web
```

**Запасной путь**, если совсем не поднимается (временно упростить безопасность юнита): откройте тот же файл `nano`, **удалите** строки `ProtectSystem=strict`, `ReadWritePaths=...`, `PrivateTmp=yes`, сохраните, затем снова:

```bash
sudo systemctl daemon-reload
sudo systemctl restart leadgen-web
```

Проверка «изнутри сервера», отвечает ли приложение:

```bash
curl -sS http://127.0.0.1:8000/healthz
```

Если видите что-то вроде `ok` или JSON — хорошо.

---

## Часть L. Шаг 8 — nginx: чтобы из интернета открывалось на порту 80

### L1. Создать конфиг

```bash
sudo nano /etc/nginx/sites-available/leadgen-http.conf
```

Вставьте:

```nginx
server {
    listen 80 default_server;
    server_name _;
    client_max_body_size 5m;

    location /static/ {
        alias /opt/leadgen/app/static/;
        expires 7d;
    }
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Сохраните (**Ctrl+O**, Enter) и выйдите (**Ctrl+X**).

### L2. Включить сайт и перезагрузить nginx

```bash
sudo ln -s /etc/nginx/sites-available/leadgen-http.conf /etc/nginx/sites-enabled/leadgen-http.conf
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx
```

**`nginx -t`** — проверка «синтаксис ок?». Если ошибка — читайте, что написано; чаще всего опечатка в файле.

---

## Часть M. Шаг 9 — фаервол (разрешить сайт и SSH)

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw enable
```

На вопрос про продолжение наберите **`y`** и Enter.

**Не включайте `ufw`**, пока не разрешили **OpenSSH** — иначе можно отрезать себе доступ. Если отрезали — через «консоль» в панели хостинга (VNC / serial) восстановите правила или переустановите VPS.

### M2. У вас открывается, а тестер с интернета — «не работает»

`ufw` на сервере — **не единственный** фаервол. У большинства VPS есть **вторая стенка в панели провайдера** (Security groups, «Сеть», «Фаервол», «Правила доступа»). Туда нужно явно разрешить **входящий TCP 80** с **0.0.0.0/0** (или «из интернета»), иначе с вашего SSH всё видно, а у коллеги — таймаут.

Проверка **с самого сервера** (локально nginx жив):

```bash
curl -sS -I http://127.0.0.1/ | head -n 3
ss -tlnp | grep ':80 '
```

Во второй команде должно быть что-то вроде `0.0.0.0:80` или `*:80` у процесса `nginx`.

Проверка **«доступен ли порт снаружи»** — с **другого** компьютера (не с сервера), например домашний ПК тестера или [онлайн-портчекеры](https://www.yougetsignal.com/tools/open-ports/) (вставьте IP и порт **80**). Если порт «closed» / «filtered» — чините **панель хостинга**, не nginx.

Что слать тестеру в сообщении (шаблон):

```text
Открой в браузере именно по HTTP (буква S в https не нужна):
http://ВАШ_IP/auth

Если браузер сам подставил https — убери s, должно быть http://
Если не грузится — попробуй с мобильного интернета без VPN и напиши, что именно видишь (таймаут / ошибка сертификата / 502).
```

---

## Часть N. Шаг 10 — открыть панель в браузере

1. Откройте браузер.
2. В адресной строке введите (подставьте IP):

```text
http://ВАШ_IP/auth
```

3. Нажмите Enter.

Должна открыться страница регистрации/входа.

**Первый зарегистрированный пользователь** обычно становится администратором организации №1 — это ожидаемое поведение для пустой базы.

### Если «не открывается»

По порядку:

1. Проверьте, что в URL именно **`http://`**, не `https://`.
2. Проверьте IP — тот же, что для SSH.
3. На сервере:

```bash
curl -sS -I http://127.0.0.1/ | head
curl -sS http://127.0.0.1:8000/healthz
sudo systemctl status leadgen-web nginx --no-pager
```

4. В панели хостинга убедитесь, что **внешний** фаервол пропускает **TCP 80**.

---

## Часть O. Шаг 11 — что настроить внутри панели

1. **Настройки и лимиты → Подключения**
   - **LLM API key** — если нужны генерации (ключи, офферы и т.д.).
   - **api_id / api_hash** для Telegram — с [my.telegram.org](https://my.telegram.org).
2. **Лиды по сайтам → Подключения** — ключ SerpAPI, если используете этот сценарий.

Без Telegram-ключей бот не сможет логиниться в аккаунт; без LLM — не будут работать LLM-кнопки (это нормально, если вы их не используете).

---

## Часть P. Шаг 12 — запуск Telegram-бота

В интерфейсе: **Контакты (бот) → Бот → Старт**.

На странице должен появиться «живой» прогресс (heartbeat), а не вечная загрузка.

Для production часто выносят бота в **отдельный systemd** — см. [`деплой-сервер.md`](деплой-сервер.md). Не запускайте **два** бота на одной сессии Telegram случайно (веб-кнопка + отдельный процесс без понимания — можно словить конфликты).

---

## Часть Q. Шаг 13 — бэкапы (очень рекомендуется)

Создайте скрипт:

```bash
sudo nano /opt/leadgen/backup.sh
```

Вставьте:

```bash
#!/bin/bash
TS=$(date +%F-%H%M)
DEST=/var/backups/leadgen
mkdir -p "$DEST"
tar czf "$DEST/leadgen-$TS.tar.gz" \
    -C / var/lib/leadgen \
    -C /opt/leadgen/app tenants sessions data 2>/dev/null || true
find "$DEST" -name 'leadgen-*.tar.gz' -mtime +14 -delete
```

Сохраните, затем:

```bash
sudo chmod +x /opt/leadgen/backup.sh
( sudo crontab -l 2>/dev/null; echo "0 3 * * * /opt/leadgen/backup.sh" ) | sudo crontab -
```

**Что это делает:** каждый день в **03:00** сервер кладёт архив в `/var/backups/leadgen/` и удаляет архивы старше **14 дней**.

---

## Часть R. Частые ошибки «простыми словами»

### R1. В браузере «502 Bad Gateway»

**Что это:** nginx жив, а «движок» за ним (gunicorn) не отвечает или упал.

**Что сделать на сервере:**

```bash
curl -sS http://127.0.0.1:8000/healthz
sudo systemctl status leadgen-web --no-pager
sudo journalctl -u leadgen-web -n 80 --no-pager
```

- Если `curl` на `8000` **не** работает — чините `leadgen-web` (часто забыли папки из части I).
- Если `curl` на `8000` **работает**, а снаружи 502 — смотрите nginx (`nginx -t`, логи nginx).

### R2. «Зашёл, но сразу выкидывает / не логинится»

Частая причина на HTTP по IP: в systemd случайно включили **`ENV=prod`** или **`HTTPS=1`**. Уберите их (часть K3), перезапустите сервис.

### R1.5. «ncurses: cannot initialize terminal type ($TERM="unknown"); exiting»

Это пытается открыться `nano` (или другой полноэкранный редактор), но в текущей консоли не задан тип терминала. Чаще всего бывает во **встроенной «Console» WinSCP** — она не передаёт `TERM`.

Три варианта починки (любой подойдёт):

1. **Откройте PuTTY** через WinSCP: **Commands → Open in PuTTY** (Ctrl+P). В PuTTY `nano` работает нормально.
2. **Задайте TERM прямо в этой сессии**:

   ```bash
   export TERM=xterm-256color
   sudo nano /etc/systemd/system/leadgen-web.service
   ```

3. **Запишите файл без редактора** через `cat <<EOF` / `tee` — см. **Часть K3.1** ниже (этот способ работает даже в самой урезанной консоли).

### R2.1. «useradd: user 'leadgen' already exists»

Это **сообщение, а не ошибка**. Пользователь уже создан предыдущим запуском. Просто пропустите `useradd`, и идите дальше с `mkdir` / `chown`. Идемпотентный вариант одной строкой:

```bash
id -u leadgen >/dev/null 2>&1 || sudo useradd -r -s /usr/sbin/nologin leadgen
```

### R3. «Я поменял код на ПК — как обновить сервер?»

Обычно на сервере (под пользователем `leadgen`):

```bash
sudo -u leadgen bash -lc 'cd /opt/leadgen/app && git pull'
sudo systemctl restart leadgen-web
```

Если проект без git — другой процесс (rsync/scp); это уже задача разработчика.

### R4. Где «настоящая» база на проде

При `DB_PATH=/var/lib/leadgen/data.db` **это и есть прод**.

Файл `data.db`, лежащий у вас на домашнем ПК в копии проекта, **сам по себе** сервер не меняет. Не заливайте поверх прода свою локальную базу без бэкапа и без понимания последствий.

---

## Часть S. HTTPS, мобильный доступ и «не удалось соединиться»

### S0. Почему с телефона «попытка соединения не удалась», хотя на сервере `ufw` разрешает 80

На сервере **ufw** — это только **один** барьер. Снаружи (мобильный интернет) трафик идёт через **сеть провайдера VPS** (Рег.облако и т.п.). Если в **панели облака** не открыт входящий **TCP 80** (а для HTTPS ниже — ещё и **TCP 443**), браузер покажет **таймаут / не удалось установить соединение** — и по `http://`, и по `https://`.

**Что сделать:** в панели Рег.облако у сервера добавьте правила **входящего** трафика: **80/tcp** и **443/tcp** с источника «везде» (`0.0.0.0/0`). Потом проверьте с телефона снова.

Проверка с **другой** сети (не SSH на сервер):

```bash
# с домашнего ПК Linux/macOS
curl -sS -I --max-time 15 http://ВАШ_IP/auth | head -n 1
```

Должна прийти строка `HTTP/1.1 200` или `302`. Если **таймаут** — почти наверняка **внешний** фаервол, не nginx.

---

### S1. Нормальный HTTPS **без покупки домена** (бесплатный сертификат Let's Encrypt)

Браузеры **не выдают** доверенный сертификат на «голый» IP (`https://203.0.113.10`). Обходной путь — бесплатное имя, которое **указывает на ваш IP**: сервис [sslip.io](https://sslip.io).

**Формат имени:** каждую **точку** в IP замените на **дефис** и добавьте `.sslip.io`.

Пример: IP `80.78.253.210` → имя **`80-78-253-210.sslip.io`**.

Проверка с сервера (должен показать ваш IP):

```bash
getent hosts 80-78-253-210.sslip.io
```

#### S1.1. Откройте порты и поставьте certbot

```bash
sudo ufw allow 443/tcp
sudo ufw reload
sudo apt update
sudo apt install -y certbot python3-certbot-nginx
```

В **панели Рег.облако** разрешите входящий **TCP 443** (и **80** — certbot для проверки Let's Encrypt по HTTP-01 слушает порт 80).

#### S1.2. Nginx: отдельный `server` под sslip (до certbot)

Создайте конфиг (подставьте **своё** имя `ВАШ-IP-через-дефис.sslip.io`):

```bash
sudo tee /etc/nginx/sites-available/leadgen-sslip.conf >/dev/null <<'EOF'
server {
    listen 80;
    server_name 80-78-253-210.sslip.io;
    client_max_body_size 5m;

    location /static/ {
        alias /opt/leadgen/app/static/;
        expires 7d;
    }
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
EOF
sudo ln -sf /etc/nginx/sites-available/leadgen-sslip.conf /etc/nginx/sites-enabled/leadgen-sslip.conf
sudo nginx -t && sudo systemctl reload nginx
```

**Важно:** в `server_name` должны быть **реальные** цифры вашего IP в формате sslip, не копируйте слепо пример с `80-78-253-210`, если ваш IP другой.

Запустите certbot (подставьте своё имя):

```bash
sudo certbot --nginx -d 80-78-253-210.sslip.io --non-interactive --agree-tos -m you@example.com --redirect
```

(Замените `you@example.com` на вашу почту — она уходит в Let's Encrypt для уведомлений об истечении сертификата.)

Certbot сам допишет SSL в nginx и часто включит редирект с HTTP на HTTPS.

#### S1.3. Обновите systemd и перезапустите веб

Откройте юнит и **исправьте три строки** (через `nano` или блок `tee` из части K2.1):

- `Environment=PUBLIC_APP_URL=https://80-78-253-210.sslip.io` (ваш sslip-хост, **https**)
- `Environment=HTTPS=1`
- `Environment=ENV=prod` (по желанию; вместе с `HTTPS=1` включает Secure-cookie — см. `web_app.py`)

Затем:

```bash
sudo systemctl daemon-reload
sudo systemctl restart leadgen-web
sudo systemctl status leadgen-web --no-pager
```

Проверка в браузере: **`https://80-78-253-210.sslip.io/auth`** (подставьте своё имя). Должен быть **замочек** без ручного «обхода опасности».

**Ссылку тестеру** давайте уже с **https** и этим хостом, не с голым IP — так меньше сюрпризов в Chrome.

---

### S2. HTTPS только на IP (самоподписанный сертификат)

Можно включить `listen 443 ssl` с **self-signed** сертификатом на IP. Браузер будет ругаться («небезопасно») — для внешних тестеров **хуже**, чем sslip.io + Let's Encrypt. Используйте только если certbot недоступен.

---

### S3. Свой домен и классический TLS

Когда купите домен: A-запись на IP, `certbot --nginx -d app.example.com`, в юните `PUBLIC_APP_URL=https://app.example.com`, `HTTPS=1`, `ENV=prod`. Подробнее: [`деплой-сервер.md`](деплой-сервер.md).

---

## Часть T. Мини-чеклист «всё ли я сделал»

- [ ] SSH на сервер работает.
- [ ] `python3 --version` ≥ 3.11 (или вы осознанно на другой схеме).
- [ ] Код в `/opt/leadgen/app`, venv в `/opt/leadgen/venv`, `pip install -r requirements.txt` без ошибок.
- [ ] Папки из части I созданы, права `leadgen`.
- [ ] `leadgen-web.service` создан, секрет вставлен, `PUBLIC_APP_URL=http://IP` (на старте по IP), **без** `ENV=prod`/`HTTPS=1` до появления нормального HTTPS.
- [ ] `systemctl status leadgen-web` — **active (running)**.
- [ ] `curl http://127.0.0.1:8000/healthz` — ответ есть.
- [ ] nginx слушает 80, `curl -I http://127.0.0.1/` с сервера даёт не 502.
- [ ] `ufw` пропускает 22 и 80 (и **443**, если включили HTTPS по части S).
- [ ] В **панели Рег.облако** открыты входящие **TCP 80** (и **443** для HTTPS) — иначе с телефона «не удалось соединиться».
- [ ] Браузер: `http://IP/auth` открывается **с другой сети** (LTE / дом), не только с сервера.
- [ ] (Опционально HTTPS без домена) Часть **S1**: `80-78-253-210.sslip.io` + certbot + `PUBLIC_APP_URL=https://…sslip.io` + `HTTPS=1` в юните.
- [ ] (Желательно) бэкап по cron из части Q.

Если на каком-то пункте застряли — сохраните **точный текст ошибки** и последние строки `journalctl` — так вас быстрее выведут из тупика любой человек с опытом Linux.

---

## Часть U. Где взять помощь

- Внутренняя документация: [`запуск.md`](запуск.md), [`деплой-сервер.md`](деплой-сервер.md).
- Синхронизация данных между машинами (осторожно с `.session`): [`SYNC.md`](../SYNC.md) в корне репозитория (если файл есть в вашей версии проекта).

Удачного деплоя.
